use std::any::Any;
use std::cell::{Cell, RefCell};
use std::ffi::CStr;
use std::io;
use std::panic;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use futures::executor::{spawn, Spawn};
use futures::future::{self, ok, Either};
use futures::stream::Fuse;
use futures::sync::mpsc;
use futures::sync::oneshot;
use futures::{Stream, Future, Sink, Async, IntoFuture, Poll};
use futures::{StartSend, AsyncSink};
use libc::c_char;
use tokio_core::net::{TcpListener, TcpStream};
use tokio_core::reactor::{Core, Timeout, Handle, Interval};
use tokio_tungstenite::{accept_async, WebSocketStream};
use tungstenite::{Error, Message};

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
    pub auto_ping_interval: f64,
    pub auto_ping_timeout: f64,
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
    open_handshake_timeout: Option<Duration>,
    auto_ping_interval: Duration,
    auto_ping_timeout: Duration,
    max_connections: Option<u32>,
    close_handshake_timeout: Option<Duration>,
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

    unsafe fn ito_dur(seconds: u32) -> Option<Duration> {
        if seconds == 0 {
            None
        } else {
            Some(Duration::new(seconds.into(), 0))
        }
    }

    unsafe fn fto_dur(seconds: f64) -> Option<Duration> {
        if seconds == 0.0 {
            None
        } else {
            Some(Duration::new(seconds as u64,
                               (seconds.fract() * 1_000_000_000.0) as u32))
        }
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
            auto_ping_interval: fto_dur(opts.auto_ping_interval)
                .expect("ping interval cannot be 0"),
            auto_ping_timeout: fto_dur(opts.auto_ping_timeout)
                .expect("ping timeout cannot be 0"),
            close_handshake_timeout: ito_dur(opts.close_handshake_timeout),
            max_connections: if opts.max_connections == 0 {
                None
            } else {
                Some(opts.max_connections)
            },
            open_handshake_timeout: ito_dur(opts.open_handshake_timeout),
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
    let open_timeout = opts.open_handshake_timeout;
    let max_connections = opts.max_connections.unwrap_or(u32::max_value());
    let open_connections = Rc::new(Cell::new(0));

    let ws_srv = ws_listener.incoming()
        .map_err(Error::Io)

        // First up perform the websocket handshake on each connection. If we've
        // accepted too many connections though just drop it on the ground.
        .map(move |(socket, addr)| {
            if open_connections.get() >= max_connections {
                let msg = format!("accepted socket exceeds max number of open \
                                   connections");
                let err = Error::Io(io::Error::new(io::ErrorKind::Other, msg));
                Box::new(ok((Err(err), addr))) as Box<Future<Item = _, Error = _>>
            } else {
                let token = OpenToken::new(open_connections.clone());
                Box::new(timeout(accept_async(socket), open_timeout, &handle2)
                            .then(move |s| Ok((s.map(|s| (s, token)), addr))))
            }
        })

        // Let up to 100 handshakes proceed in parallel
        .buffer_unordered(100)

        // After each handshake, send the client to the main thread. Each client
        // send to the main thread has two queues, one for incoming messages and
        // one for outgoing messages.
        .fold(tx, move |tx, (res, addr)| {
            let (socket, token) = match res {
                Ok(pair) => pair,
                Err(e) => {
                    println!("failed to accept client {}: {}", addr, e);
                    return Box::new(ok(tx)) as Box<Future<Item = _, Error = _>>
                }
            };
            let (atx, arx) = mpsc::channel(0);
            let (btx, brx) = mpsc::channel(0);
            let client = match ClientDriver::new(&opts,
                                                 &handle,
                                                 socket,
                                                 btx,
                                                 arx) {
                Ok(s) => s,
                Err(e) => {
                    println!("failed to make ping handler {}: {}", addr, e);
                    return Box::new(ok(tx))
                }
            };

            handle.spawn(client.then(move |res| {
                // TODO: log this? ignore this? unsure.
                println!("{}: {:?}", addr, res);
                drop(token);
                Ok(())
            }));


            let tx = tx.send(ClientInner {
                tx: RefCell::new(spawn(atx)),
                rx: RefCell::new(spawn(brx)),
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

fn timeout<F>(f: F, dur: Option<Duration>, handle: &Handle)
    -> Box<Future<Item = F::Item, Error = Error>>
    where F: Future<Error = Error> + 'static,
{
    let dur = match dur {
        Some(dur) => dur,
        None => return Box::new(f),
    };
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

// Helper struct to maintain the count of open connected connections.
struct OpenToken {
    cnt: Rc<Cell<u32>>,
}

impl OpenToken {
    fn new(cnt: Rc<Cell<u32>>) -> OpenToken {
        cnt.set(cnt.get() + 1);
        OpenToken { cnt: cnt }
    }
}

impl Drop for OpenToken {
    fn drop(&mut self) {
        self.cnt.set(self.cnt.get() - 1);
    }
}

struct ClientDriver {
    handle: Handle,
    socket: Rc<RefCell<WebSocketStream<TcpStream>>>,
    ping_interval: Interval,
    ping_timeout: Option<Timeout>,
    ping_timeout_dur: Duration,
    msg_exchange: Box<Future<Item = (), Error = Error>>,
}

impl ClientDriver {
    fn new(opts: &ServerOptions,
           handle: &Handle,
           socket: WebSocketStream<TcpStream>,
           tx: mpsc::Sender<Message>,
           rx: mpsc::Receiver<Message>)
        -> io::Result<ClientDriver>
    {
        // The `socket` is itself a sink and a stream, and we've also got a sink
        // (`tx`) and a stream (`rx`) to send messages. Half of our job will be
        // doing all this proxying: reading messages from `socket` and sending
        // them to `tx` while also reading messages from `rx` and sending them
        // on `socket`.
        //
        // Our other job will be to manage the websocket protocol pings going
        // out and coming back. The `opts` provided indicate how often we send
        // pings and how long we'll wait for the ping to come back before we
        // time it out.
        //
        // To make these tasks easier we start out by throwing the `socket` into
        // an `Rc` object. This'll allow us to share it between the ping/pong
        // management and message shuffling.
        let socket = Rc::new(RefCell::new(socket));

        let stream = RcObject(socket.clone());
        let sink = RcObject(socket.clone());
        let rx = rx.map_err(|()| panic!());
        let tx = tx.sink_map_err(|e| {
            let msg = format!("btx sink error (python gone?): {}", e);
            Error::Io(io::Error::new(io::ErrorKind::Other, msg))
        });
        let read_messages = MySendAll::new(stream, tx).map(|_| ());
        let handle2 = handle.clone();
        let close_timeout = opts.close_handshake_timeout;
        let write_messages = MySendAll::new(rx, sink).map(move |(_, mut sink)| {
            timeout(future::poll_fn(move || sink.close()), close_timeout, &handle2)
        });

        Ok(ClientDriver {
            handle: handle.clone(),
            ping_interval: Interval::new(opts.auto_ping_interval, handle)?,
            ping_timeout_dur: opts.auto_ping_timeout,
            ping_timeout: None,
            socket: socket,
            msg_exchange: Box::new(read_messages.join(write_messages).map(|_| ())),
        })
    }
}

impl Future for ClientDriver {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<(), Error> {
        // If it's time for us to send a ping, then queue up a ping to get sent
        // and start the clock for that ping to time out.
        while let Async::Ready(_) = self.ping_interval.poll()? {
            if self.ping_timeout.is_some() {
                continue
            }
            self.socket.borrow_mut().send_ping(Vec::new())?;
            self.ping_timeout = Some(Timeout::new(self.ping_timeout_dur,
                                                  &self.handle)?);
            // fall through below to start polling `ping_timeout`
            break
        }

        if let Async::Ready(_) = self.socket.borrow_mut().poll_pong() {
            self.ping_timeout = None;
        }

        // If the client takes too long to respond to our websocket ping then we
        // terminate the whole connection.
        if let Async::Ready(Some(())) = self.ping_timeout.poll()? {
            let msg = format!("ping took too long to respond, terminating");
            return Err(Error::Io(io::Error::new(io::ErrorKind::Other, msg)))
        }

        self.msg_exchange.poll()
    }
}

// Helper object to turn `Rc<RefCell<T>>` into a `Stream` and `Sink`
struct RcObject<T>(Rc<RefCell<T>>);

impl<T: Stream> Stream for RcObject<T> {
    type Item = T::Item;
    type Error = T::Error;

    fn poll(&mut self) -> Poll<Option<T::Item>, T::Error> {
        self.0.borrow_mut().poll()
    }
}

impl<T: Sink> Sink for RcObject<T> {
    type SinkItem = T::SinkItem;
    type SinkError = T::SinkError;

    fn start_send(&mut self, msg: T::SinkItem)
        -> StartSend<T::SinkItem, T::SinkError>
    {
        self.0.borrow_mut().start_send(msg)
    }

    fn poll_complete(&mut self) -> Poll<(), T::SinkError> {
        self.0.borrow_mut().poll_complete()
    }

    fn close(&mut self) -> Poll<(), T::SinkError> {
        self.0.borrow_mut().close()
    }
}

// This is a copy of `Future::forward`, except that it doesn't close the sink
// when it's finished.
struct MySendAll<T: Stream, U> {
    sink: Option<U>,
    stream: Option<Fuse<T>>,
    buffered: Option<T::Item>,
}

impl<T, U> MySendAll<T, U>
    where U: Sink<SinkItem=T::Item>,
          T: Stream,
          T::Error: From<U::SinkError>,
{
    fn new(t: T, u: U) -> MySendAll<T, U> {
        MySendAll {
            sink: Some(u),
            stream: Some(t.fuse()),
            buffered: None,
        }
    }

    fn sink_mut(&mut self) -> &mut U {
        self.sink.as_mut().take()
            .expect("Attempted to poll MySendAll after completion")
    }

    fn stream_mut(&mut self) -> &mut Fuse<T> {
        self.stream.as_mut().take()
            .expect("Attempted to poll MySendAll after completion")
    }

    fn take_result(&mut self) -> (T, U) {
        let sink = self.sink.take()
            .expect("Attempted to poll MySendAll after completion");
        let fuse = self.stream.take()
            .expect("Attempted to poll MySendAll after completion");
        (fuse.into_inner(), sink)
    }

    fn try_start_send(&mut self, item: T::Item) -> Poll<(), U::SinkError> {
        debug_assert!(self.buffered.is_none());
        if let AsyncSink::NotReady(item) = try!(self.sink_mut().start_send(item)) {
            self.buffered = Some(item);
            return Ok(Async::NotReady)
        }
        Ok(Async::Ready(()))
    }
}

impl<T, U> Future for MySendAll<T, U>
    where U: Sink<SinkItem=T::Item>,
          T: Stream,
          T::Error: From<U::SinkError>,
{
    type Item = (T, U);
    type Error = T::Error;

    fn poll(&mut self) -> Poll<(T, U), T::Error> {
        // If we've got an item buffered already, we need to write it to the
        // sink before we can do anything else
        if let Some(item) = self.buffered.take() {
            try_ready!(self.try_start_send(item))
        }

        loop {
            match try!(self.stream_mut().poll()) {
                Async::Ready(Some(item)) => try_ready!(self.try_start_send(item)),
                Async::Ready(None) => {
                    try_ready!(self.sink_mut().poll_complete());
                    return Ok(Async::Ready(self.take_result()))
                }
                Async::NotReady => {
                    try_ready!(self.sink_mut().poll_complete());
                    return Ok(Async::NotReady)
                }
            }
        }
    }
}
