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
use futures::sync::mpsc;
use futures::sync::oneshot;
use futures::{Stream, Future, Sink, Async, Poll, AsyncSink, StartSend};
use libc::c_char;
use serde_json;
use tokio_core::net::{TcpListener, TcpStream};
use tokio_core::reactor::{Core, Timeout, Handle, Interval};
use tokio_tungstenite::{accept_async, WebSocketStream};
use tungstenite::Message;

use MyNotify;
use call::{PythonCall, AutopushPythonCall};
use client::Client;
use errors::*;
use protocol::{ClientMessage, ServerMessage};
use rt::{self, AutopushError, UnwindGuard};
use util::{RcObject, timeout};

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
    notify: RefCell<Arc<MyNotify>>,
    rx: RefCell<Spawn<mpsc::Receiver<PythonCall>>>,

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
pub extern "C" fn autopush_server_next_call(srv: *mut AutopushServer,
                                            err: &mut AutopushError)
    -> *mut AutopushPythonCall
{
    unsafe {
        (*srv).inner.catch(err, |srv| {
            srv.poll_call().map(|call| {
                Box::new(AutopushPythonCall::new(call))
            })
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

impl ServerInner {
    fn new(opts: ServerOptions) -> io::Result<ServerInner> {
        let (donetx, donerx) = oneshot::channel();
        let (inittx, initrx) = oneshot::channel();
        let (tx, rx) = mpsc::channel(100);

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
                    rx: RefCell::new(spawn(rx)),
                    tx: Cell::new(Some(donetx)),
                    thread: Cell::new(Some(thread)),
                    notify: RefCell::new(Arc::new(MyNotify(dummy))),
                })
            }
            Err(_) => {
                panic::resume_unwind(thread.join().unwrap_err());
            }
        }
    }

    fn poll_call(&self) -> Option<PythonCall> {
        let mut rx = self.rx.borrow_mut();
        let notify = self.notify.borrow();
        match rx.poll_stream_notify(&*notify, 0).expect("streams cannot error") {
            Async::Ready(Some(call)) => Some(call),
            Async::Ready(None) => panic!("I/O thread is gone"),
            Async::NotReady => None,
        }
    }

    pub fn notify(&self) -> Arc<MyNotify> {
        self.notify.borrow().clone()
    }

    fn stop(&self) -> Result<()> {
        drop(self.tx.take());
        if let Some(thread) = self.thread.take() {
            thread.join().map_err(ErrorKind::Thread)?;
        }
        Ok(())
    }
}

fn init(opts: ServerOptions, tx: mpsc::Sender<PythonCall>) -> io::Result<Core> {
    let opts = Rc::new(opts);
    let core = Core::new()?;
    let handle = core.handle();
    let addr = format!("127.0.0.1:{}", opts.port);
    let ws_listener = TcpListener::bind(&addr.parse().unwrap(), &handle)?;

    assert!(opts.ssl_key.is_none(), "ssl not supported yet");
    assert!(opts.ssl_cert.is_none(), "ssl not supported yet");
    assert!(opts.ssl_dh_param.is_none(), "ssl not supported yet");

    let handle = core.handle();
    let open_timeout = opts.open_handshake_timeout;
    let max_connections = opts.max_connections.unwrap_or(u32::max_value());
    let open_connections = Rc::new(Cell::new(0));

    let ws_srv = ws_listener.incoming()
        .map_err(|e| Error::from(e))

        .for_each(move |(socket, addr)| {
            // Make sure we're not handling too many clients before we start the
            // websocket handshake.
            if open_connections.get() >= max_connections {
                println!("dropping {} as we already have too many open \
                          connections", addr);
                return Ok(())
            }
            open_connections.set(open_connections.get() + 1);

            // TODO: TCP socket options here?

            // Perform the websocket handshake on each connection, but don't let
            // it take too long.
            let ws = accept_async(socket).chain_err(|| "failed to accept client");
            let ws = timeout(ws, open_timeout, &handle);

            // Once the handshake is done we'll start the main communication
            // with the client, managing pings here and deferring to `Client` to
            // start driving the internal state machine.
            let opts = opts.clone();
            let handle2 = handle.clone();
            let tx = tx.clone();
            let client = ws.and_then(move |ws| {
                PingManager::new(&opts, &handle2, tx, ws)
                    .chain_err(|| "failed to make ping handler")
            }).flatten();

            let open_connections = open_connections.clone();
            handle.spawn(client.then(move |res| {
                open_connections.set(open_connections.get() - 1);
                // TODO: log this? ignore this? unsure.
                println!("{}: {:?}", addr, res);
                Ok(())
            }));

            Ok(())
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

struct PingManager {
    handle: Handle,
    socket: RcObject<WebSocketStream<TcpStream>>,
    ping_interval: Interval,
    timeout: TimeoutState,
    ping_timeout_dur: Duration,
    close_handshake_timeout: Option<Duration>,
    client: State<Client<WebpushSocket<RcObject<WebSocketStream<TcpStream>>>>>,
}

enum TimeoutState {
    None,
    Ping(Timeout),
    Close(Timeout),
}

enum State<T> {
    Exchange(T),
    Closing,
}

impl PingManager {
    fn new(opts: &ServerOptions,
           handle: &Handle,
           tx: mpsc::Sender<PythonCall>,
           socket: WebSocketStream<TcpStream>)
        -> io::Result<PingManager>
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
        let socket = RcObject::new(socket);
        Ok(PingManager {
            handle: handle.clone(),
            ping_interval: Interval::new(opts.auto_ping_interval, handle)?,
            ping_timeout_dur: opts.auto_ping_timeout,
            timeout: TimeoutState::None,
            socket: socket.clone(),
            client: State::Exchange(Client::new(WebpushSocket(socket), tx)),
            close_handshake_timeout: opts.close_handshake_timeout,
        })
    }
}

impl Future for PingManager {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<(), Error> {
        // If it's time for us to send a ping, then queue up a ping to get sent
        // and start the clock for that ping to time out.
        while let Async::Ready(_) = self.ping_interval.poll()? {
            match self.timeout {
                TimeoutState::None => {}
                _ => continue,
            }
            self.socket.borrow_mut().send_ping(Vec::new())?;
            let timeout = Timeout::new(self.ping_timeout_dur, &self.handle)?;
            self.timeout = TimeoutState::Ping(timeout);
        }

        // If the client takes too long to respond to our websocket ping or too
        // long to execute the closing handshake then we terminate the whole
        // connection.
        match self.timeout {
            TimeoutState::None => {}
            TimeoutState::Close(ref mut timeout) => {
                if timeout.poll()?.is_ready() {
                    return Err("close handshake took too long".into())
                }
            }
            TimeoutState::Ping(ref mut timeout) => {
                if timeout.poll()?.is_ready() {
                    return Err("pong not received within timeout".into())
                }
            }
        }

        // Received pongs will clear our ping timeout, but not the close
        // timeout.
        if self.socket.borrow_mut().poll_pong().is_ready() {
            if let TimeoutState::Ping(_) = self.timeout {
                self.timeout = TimeoutState::None;
            }
        }

        // At this point looks our state of ping management A-OK, so try to
        // make progress on our client, and when done with that execute the
        // closing handshake.
        loop {
            match self.client {
                State::Exchange(ref mut client) => try_ready!(client.poll()),
                State::Closing => return Ok(self.socket.close()?),
            }

            self.client = State::Closing;
            if let Some(dur) = self.close_handshake_timeout {
                let timeout = Timeout::new(dur, &self.handle)?;
                self.timeout = TimeoutState::Close(timeout);
            }
        }
    }
}

// Wrapper struct to take a Sink/Stream of `Message` to a Sink/Stream of
// `ClientMessage` and `ServerMessage`.
struct WebpushSocket<T>(T);

impl<T> Stream for WebpushSocket<T>
    where T: Stream<Item = Message>,
          Error: From<T::Error>,
{
    type Item = ClientMessage;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<ClientMessage>, Error> {
        match try_ready!(self.0.poll()) {
            Some(Message::Text(ref s)) => {
                let msg = serde_json::from_str(s).chain_err(|| "invalid json text")?;
                Ok(Some(msg).into())
            }
            Some(Message::Binary(_)) => Err("binary messages not accepted".into()),
            None => Ok(None.into()),
        }
    }
}

impl<T> Sink for WebpushSocket<T>
    where T: Sink<SinkItem = Message>,
          Error: From<T::SinkError>,
{
    type SinkItem = ServerMessage;
    type SinkError = Error;

    fn start_send(&mut self, msg: ServerMessage)
        -> StartSend<ServerMessage, Error>
    {
        let s = serde_json::to_string(&msg).chain_err(|| "failed to serialize")?;
        match self.0.start_send(Message::Text(s))? {
            AsyncSink::Ready => Ok(AsyncSink::Ready),
            AsyncSink::NotReady(_) => Ok(AsyncSink::NotReady(msg)),
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Error> {
        Ok(self.0.poll_complete()?)
    }

    fn close(&mut self) -> Poll<(), Error> {
        Ok(self.0.close()?)
    }
}
