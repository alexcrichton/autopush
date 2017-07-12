use std::any::Any;
use std::cell::{Cell, RefCell};
use std::ffi::CStr;
use std::io;
use std::panic;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use futures::executor::{spawn, Spawn};
use futures::future::{ok, Either};
use futures::sync::mpsc;
use futures::sync::oneshot;
use futures::{Stream, Future, Sink, Async, IntoFuture};
use libc::c_char;
use tokio_core::net::TcpListener;
use tokio_core::reactor::{Core, Timeout, Handle};
use tokio_tungstenite::accept_async;
use tungstenite::Error;

use MyNotify;
use rt::{self, AutopushError, UnwindGuard};
use client::{AutopushClient, ClientInner};

#[repr(C)]
pub struct AutopushServer {
    inner: UnwindGuard<ServerInner>,
}

#[repr(C)]
pub struct AutopushServerOptions {
    pub debug: i32,
    pub port: u16,
    pub url: *const c_char,
    pub ssl_key: *const c_char,
    pub ssl_cert: *const c_char,
    pub ssl_dh_param: *const c_char,
    pub open_handshake_timeout: u32,
    pub auto_ping_interval: u32,
    pub auto_ping_timeout: u32,
    pub max_connections: u32,
    pub close_handshake_timeout: u32,
}

pub struct ServerInner {
    rx: RefCell<Spawn<mpsc::Receiver<ClientInner>>>,
    notify: RefCell<Arc<MyNotify>>,

    // Used when shutting down a server
    tx: Cell<Option<oneshot::Sender<()>>>,
    thread: Cell<Option<thread::JoinHandle<()>>>,
}

struct ServerOptions {
    debug: bool,
    port: u16,
    url: String,
    ssl_key: Option<PathBuf>,
    ssl_cert: Option<PathBuf>,
    ssl_dh_param: Option<PathBuf>,
    open_handshake_timeout: Duration,
    auto_ping_interval: Duration,
    auto_ping_timeout: Duration,
    max_connections: u32,
    close_handshake_timeout: Duration,
}

#[no_mangle]
pub extern "C" fn autopush_server_new(opts: *const AutopushServerOptions,
                                      err: &mut AutopushError)
    -> *mut AutopushServer
{
    unsafe fn to_s<'a>(ptr: *const c_char) -> Option<&'a str> {
        if ptr.is_null() {
            None
        } else {
            let s = CStr::from_ptr(ptr).to_str().expect("invalid utf-8");
            if s.is_empty() {
                None
            } else {
                Some(s)
            }
        }
    }

    unsafe fn to_dur(seconds: u32) -> Duration {
        Duration::new(seconds.into(), 0)
    }

    rt::catch(err, || unsafe {
        let opts = &*opts;

        // TODO: returning results to python?
        let inner = ServerInner::new(ServerOptions {
            debug: opts.debug != 0,
            port: opts.port,
            url: to_s(opts.url).expect("url must be specified").to_string(),
            ssl_key: to_s(opts.ssl_key).map(PathBuf::from),
            ssl_cert: to_s(opts.ssl_cert).map(PathBuf::from),
            ssl_dh_param: to_s(opts.ssl_dh_param).map(PathBuf::from),
            auto_ping_interval: to_dur(opts.auto_ping_interval),
            auto_ping_timeout: to_dur(opts.auto_ping_timeout),
            close_handshake_timeout: to_dur(opts.close_handshake_timeout),
            max_connections: opts.max_connections,
            open_handshake_timeout: to_dur(opts.open_handshake_timeout),
        }).expect("failed to start");

        Box::new(AutopushServer {
            inner: UnwindGuard::new(inner),
        })
    })
}

#[no_mangle]
pub extern "C" fn autopush_server_start(srv: *mut AutopushServer,
                                        cb: extern fn(usize),
                                        err: &mut AutopushError) -> i32 {
    unsafe {
        (*srv).inner.catch(err, |srv| {
            *srv.notify.borrow_mut() = Arc::new(MyNotify(cb));
        })
    }
}

#[no_mangle]
pub extern "C" fn autopush_server_stop(srv: *mut AutopushServer,
                                       err: &mut AutopushError) -> i32 {
    unsafe {
        (*srv).inner.catch(err, |srv| {
            srv.stop().expect("tokio thread panicked");
        })
    }
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
    fn new(opts: ServerOptions) -> io::Result<ServerInner> {
        let (donetx, donerx) = oneshot::channel();
        let (inittx, initrx) = oneshot::channel();
        let (tx, rx) = mpsc::channel(0);

        assert!(opts.ssl_key.is_none(), "ssl not supported");
        assert!(opts.ssl_cert.is_none(), "ssl not supported");
        assert!(opts.ssl_dh_param.is_none(), "ssl not supported");

        let thread = thread::spawn(move || {
            let mut core = match init(opts, tx) {
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
                extern fn dummy(_: usize) {}

                Ok(ServerInner {
                    tx: Cell::new(Some(donetx)),
                    thread: Cell::new(Some(thread)),
                    rx: RefCell::new(spawn(rx)),
                    notify: RefCell::new(Arc::new(MyNotify(dummy))),
                })
            }
            Err(_) => {
                panic::resume_unwind(thread.join().unwrap_err());
            }
        }
    }

    fn accept_client(&self) -> Option<ClientInner> {
        let mut rx = self.rx.borrow_mut();
        let notify = self.notify.borrow();
        match rx.poll_stream_notify(&*notify, 0).expect("streams cannot error") {
            Async::Ready(Some(client)) => Some(client),
            Async::Ready(None) => panic!("I/O thread is gone"),
            Async::NotReady => None,
        }
    }

    pub fn notify(&self) -> Arc<MyNotify> {
        self.notify.borrow().clone()
    }

    fn stop(&self) -> Result<(), Box<Any+Send>> {
        drop(self.tx.take());
        if let Some(thread) = self.thread.take() {
            thread.join()?;
        }
        Ok(())
    }
}

fn init(opts: ServerOptions, tx: mpsc::Sender<ClientInner>) -> io::Result<Core> {
    let core = Core::new()?;
    let handle = core.handle();
    let addr = format!("127.0.0.1:{}", opts.port);
    let ws_listener = TcpListener::bind(&addr.parse().unwrap(), &handle)?;

    assert!(opts.ssl_key.is_none(), "ssl not supported yet");
    assert!(opts.ssl_cert.is_none(), "ssl not supported yet");
    assert!(opts.ssl_dh_param.is_none(), "ssl not supported yet");

    let handle = core.handle();
    let handle2 = core.handle();
    let ws_srv = ws_listener.incoming()
        .map_err(Error::Io)

        // First up perform the websocket handshake on each connection
        .map(move |(socket, addr)| {
            timeout(accept_async(socket), Duration::new(2, 0), &handle2)
                .then(move |s| Ok((s, addr)))
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
        drop(self.stop());
    }
}

fn timeout<F>(f: F, dur: Duration, handle: &Handle)
    -> Box<Future<Item = F::Item, Error = Error>>
    where F: Future<Error = Error> + 'static,
{
    let timeout = Timeout::new(dur, handle).into_future().flatten();
    Box::new(f.select2(timeout).then(|res| {
        match res {
            Ok(Either::A((item, _timeout))) => Ok(item),
            Err(Either::A((e, _timeout))) => Err(e),
            Ok(Either::B(((), _item))) => {
                let msg = "timed out";
                Err(Error::Io(io::Error::new(io::ErrorKind::Other, msg)))
            }
            Err(Either::B((e, _item))) => Err(Error::Io(e)),
        }
    }))
}
