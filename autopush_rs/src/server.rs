use std::cell::RefCell;
use std::ffi::CStr;
use std::io;
use std::panic;
use std::sync::Arc;
use std::thread;

use futures::executor::{spawn, Spawn};
use futures::future::ok;
use futures::sync::mpsc;
use futures::sync::oneshot;
use futures::{Stream, Future, Sink, Async};
use libc::c_char;
use tokio_core::net::TcpListener;
use tokio_core::reactor::Core;
use tokio_tungstenite::accept_async;
use tungstenite::Error;

use MyNotify;
use rt::{self, AutopushError, UnwindGuard};
use client::{AutopushClient, ClientInner};

#[repr(C)]
pub struct AutopushServer {
    inner: UnwindGuard<ServerInner>,
}

pub struct ServerInner {
    rx: RefCell<Spawn<mpsc::Receiver<ClientInner>>>,
    notify: Arc<MyNotify>,

    // Used when shutting down a server
    tx: Option<oneshot::Sender<()>>,
    thread: Option<thread::JoinHandle<()>>,
}

#[no_mangle]
pub extern "C" fn autopush_server_new(addr: *const c_char,
                                      cb: extern fn(usize),
                                      err: &mut AutopushError)
    -> *mut AutopushServer
{
    rt::catch(err, || unsafe {
        let addr = CStr::from_ptr(addr).to_str().unwrap();

        // TODO: returning results to python?
        let inner = ServerInner::new(addr, cb).expect("failed to start");

        Box::new(AutopushServer {
            inner: UnwindGuard::new(inner),
        })
    })
}

#[no_mangle]
pub extern "C" fn autopush_server_free(srv: *mut AutopushServer) {
    rt::abort_on_panic(|| unsafe {
        println!("free server");
        Box::from_raw(srv);
    })
}

#[no_mangle]
pub extern "C" fn autopush_server_accept_client(srv: *mut AutopushServer,
                                                next_id: usize,
                                                err: &mut AutopushError)
    -> *mut AutopushClient
{
    unsafe {
        (*srv).inner.catch(err, |srv| {
            srv.accept_client().map(|client| {
                Box::new(AutopushClient::new(client, srv, next_id))
            })
        })
    }
}

impl ServerInner {
    fn new(addr: &str, cb: extern fn(usize)) -> io::Result<ServerInner> {
        let (donetx, donerx) = oneshot::channel();
        let addr = addr.to_string();
        let (inittx, initrx) = oneshot::channel();
        let (tx, rx) = mpsc::channel(0);
        let thread = thread::spawn(move || {
            let mut core = match init(&addr, tx) {
                Ok(core) => {
                    inittx.send(None).unwrap();
                    core
                }
                Err(e) => return inittx.send(Some(e)).unwrap(),
            };
            drop(core.run(donerx));
        });

        match initrx.wait() {
            Ok(Some(e)) => Err(e),
            Ok(None) => {
                Ok(ServerInner {
                    tx: Some(donetx),
                    thread: Some(thread),
                    rx: RefCell::new(spawn(rx)),
                    notify: Arc::new(MyNotify(cb)),
                })
            }
            Err(_) => {
                panic::resume_unwind(thread.join().unwrap_err());
            }
        }
    }

    fn accept_client(&self) -> Option<ClientInner> {
        let mut rx = self.rx.borrow_mut();
        match rx.poll_stream_notify(&self.notify, 0).expect("streams cannot error") {
            Async::Ready(Some(client)) => Some(client),
            Async::Ready(None) => panic!("I/O thread is gone"),
            Async::NotReady => None,
        }
    }

    pub fn notify(&self) -> &Arc<MyNotify> {
        &self.notify
    }
}

fn init(addr: &str, tx: mpsc::Sender<ClientInner>) -> io::Result<Core> {
    let core = Core::new()?;
    let handle = core.handle();
    let ws_listener = TcpListener::bind(&addr.parse().unwrap(), &handle)?;

    let handle = core.handle();
    let ws_srv = ws_listener.incoming()
        .map_err(Error::Io)

        // First up perform the websocket handshake on each connection
        //
        // TODO: time this out
        .map(|(socket, addr)| {
            accept_async(socket).then(move |s| Ok((s, addr)))
        })

        // Let up to 100 handshakes proceed in parallel
        .buffer_unordered(100)

        // After each handshake, send the client to the main thread. Each client
        // send to the main thread has two queues, one for incoming messages and
        // one for outgoing messages.
        .fold(tx, move |tx, (res, addr)| {
            let socket = match res {
                Ok(socket) => socket,
                Err(e) => {
                    println!("failed to accept client {}: {}", addr, e);
                    return Box::new(ok(tx)) as Box<Future<Item = _, Error = _>>
                }
            };
            let handle = handle.clone();
            let (atx, arx) = mpsc::channel(0);
            let (btx, brx) = mpsc::channel(0);
            let tx = tx.send(ClientInner {
                tx: RefCell::new(spawn(atx)),
                rx: RefCell::new(spawn(brx)),
            }).map(move |tx| {

                // After we've send our client to the main thread we start
                // reading messages off the socket. The `socket` here is a
                // `Sink` and a `Stream` fused together, so we start off by
                // splitting  it. After the split we connect everything by
                // forwarding all the various pipes to one another. Once that's
                // all taken care of we spawn this work to execute concurrently
                // with all other clients and then we'll try to send the next
                // client to the main thread.
                let (sink, stream) = socket.split();

                // arx can't have an error, but that's not statically expressed
                let arx = arx.map_err(|()| panic!());

                let btx = btx.sink_map_err(|e| {
                    let msg = format!("btx sink error (python gone?): {}", e);
                    Error::Io(io::Error::new(io::ErrorKind::Other, msg))
                });
                let write_messages = arx.forward(sink).map(|_| ());
                let read_messages = stream.forward(btx).map(|_| ());

                // TODO: join or select?
                handle.spawn(read_messages.join(write_messages).then(move |res| {
                    // TODO: log this? ignore this? unsure.
                    println!("{}: {:?}", addr, res);
                    Ok(())
                }));

                tx
            }).map_err(|e| {
                let msg = format!("tx sink error (python gone?): {}", e);
                Error::Io(io::Error::new(io::ErrorKind::Other, msg))
            });

            Box::new(tx)
        });

    core.handle().spawn(ws_srv.then(|res| {
        println!("srv res: {:?}", res.map(drop));
        Ok(())
    }));

    Ok(core)
}

impl Drop for ServerInner {
    fn drop(&mut self) {
        drop(self.tx.take());
        self.thread.take().unwrap().join().unwrap();
    }
}
