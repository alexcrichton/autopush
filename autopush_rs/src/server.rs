use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::ffi::CStr;
use std::io;
use std::mem;
use std::panic;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use futures::executor::{spawn, Spawn};
use futures::sync::mpsc;
use futures::sync::oneshot;
use futures::task::{self, Task};
use futures::{Stream, Future, Sink, Async, Poll, AsyncSink, StartSend};
use libc::c_char;
use serde_json;
use tokio_core::net::{TcpListener, TcpStream};
use tokio_core::reactor::{Core, Timeout, Handle, Interval};
use tokio_tungstenite::{accept_async, WebSocketStream};
use tungstenite::Message;
use uuid::Uuid;

use MyNotify;
use call::{PythonCall, AutopushPythonCall};
use client::{Client, ClientState, Channel};
use errors::*;
use protocol::{ClientMessage, ServerMessage, Notification, Update};
use rt::{self, AutopushError, UnwindGuard};
use util::{RcObject, timeout};

#[repr(C)]
pub struct AutopushServer {
    inner: UnwindGuard<AutopushServerInner>,
}

struct AutopushServerInner {
    notify: RefCell<Arc<MyNotify>>,
    rx: RefCell<Spawn<mpsc::UnboundedReceiver<PythonCall>>>,

    // Used when shutting down a server
    tx: Cell<Option<oneshot::Sender<()>>>,
    thread: Cell<Option<thread::JoinHandle<()>>>,
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

pub struct Server {
    channels: RefCell<HashMap<Uuid, Channel>>,
    uaids: RefCell<HashMap<Uuid, ClientState>>,
    open_connections: Cell<u32>,
    pub opts: ServerOptions,
    pub handle: Handle,
}

pub struct ServerOptions {
    pub debug: bool,
    pub port: u16,
    pub url: String,
    pub ssl_key: Option<PathBuf>,
    pub ssl_cert: Option<PathBuf>,
    pub ssl_dh_param: Option<PathBuf>,
    pub open_handshake_timeout: Option<Duration>,
    pub auto_ping_interval: Duration,
    pub auto_ping_timeout: Duration,
    pub max_connections: Option<u32>,
    pub close_handshake_timeout: Option<Duration>,
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
        let inner = AutopushServerInner::new(ServerOptions {
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

impl AutopushServerInner {
    /// Creates a new server handle to send to python.
    ///
    /// This will spawn a new server with the `opts` specified, spinning up a
    /// separate thread for the tokio reactor. The returned
    /// `AutopushServerInner` is a handle to the spawned thread and can be used
    /// to interact with it (e.g. shut it down).
    fn new(opts: ServerOptions) -> io::Result<AutopushServerInner> {
        let (donetx, donerx) = oneshot::channel();
        let (inittx, initrx) = oneshot::channel();
        let (tx, rx) = mpsc::unbounded();

        assert!(opts.ssl_key.is_none(), "ssl not supported");
        assert!(opts.ssl_cert.is_none(), "ssl not supported");
        assert!(opts.ssl_dh_param.is_none(), "ssl not supported");

        let thread = thread::spawn(move || {
            let (srv, mut core) = match Server::new(opts, tx) {
                Ok(core) => {
                    inittx.send(None).unwrap();
                    core
                }
                Err(e) => return inittx.send(Some(e)).unwrap(),
            };

            // For now during development spin up a dummy HTTP server which is
            // used to send notifications to clients.
            {
                use hyper::server::Http;

                let handle = core.handle();
                let addr = "127.0.0.1:8081".parse().unwrap();
                let push_listener = TcpListener::bind(&addr, &handle).unwrap();
                let proto = Http::new();
                let push_srv = push_listener.incoming().for_each(move |(socket, addr)| {
                    proto.bind_connection(&handle, socket, addr,
                                          ::http::Push(srv.clone()));
                    Ok(())
                });
                core.handle().spawn(push_srv.then(|res| {
                    println!("Http server {:?}", res);
                    Ok(())
                }));
            }

            drop(core.run(donerx));
        });

        match initrx.wait() {
            Ok(Some(e)) => Err(e),
            Ok(None) => {
                extern fn dummy(_: usize) {}

                Ok(AutopushServerInner {
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

    /// Check to see if there's any requests to call into python, returning if
    /// any have been found.
    fn poll_call(&self) -> Option<PythonCall> {
        let mut rx = self.rx.borrow_mut();
        let notify = self.notify.borrow();
        match rx.poll_stream_notify(&*notify, 0).expect("streams cannot error") {
            Async::Ready(Some(call)) => Some(call),
            Async::Ready(None) => panic!("I/O thread is gone"),
            Async::NotReady => None,
        }
    }

    /// Blocks execution of the calling thread until the helper thread with the
    /// tokio reactor has exited.
    fn stop(&self) -> Result<()> {
        drop(self.tx.take());
        if let Some(thread) = self.thread.take() {
            thread.join().map_err(ErrorKind::Thread)?;
        }
        Ok(())
    }
}

impl Drop for AutopushServerInner {
    fn drop(&mut self) {
        drop(self.stop());
    }
}

impl Server {
    fn new(opts: ServerOptions, tx: mpsc::UnboundedSender<PythonCall>)
        -> io::Result<(Rc<Server>, Core)>
    {
        let core = Core::new()?;
        let srv = Rc::new(Server {
            opts: opts,
            channels: RefCell::new(HashMap::new()),
            uaids: RefCell::new(HashMap::new()),
            open_connections: Cell::new(0),
            handle: core.handle(),
        });
        let addr = format!("127.0.0.1:{}", srv.opts.port);
        let ws_listener = TcpListener::bind(&addr.parse().unwrap(), &srv.handle)?;

        assert!(srv.opts.ssl_key.is_none(), "ssl not supported yet");
        assert!(srv.opts.ssl_cert.is_none(), "ssl not supported yet");
        assert!(srv.opts.ssl_dh_param.is_none(), "ssl not supported yet");

        let handle = core.handle();
        let srv2 = srv.clone();
        let ws_srv = ws_listener.incoming()
            .map_err(|e| Error::from(e))

            .for_each(move |(socket, addr)| {
                // Make sure we're not handling too many clients before we start the
                // websocket handshake.
                let max = srv.opts.max_connections.unwrap_or(u32::max_value());
                if srv.open_connections.get() >= max {
                    println!("dropping {} as we already have too many open \
                              connections", addr);
                    return Ok(())
                }
                srv.open_connections.set(srv.open_connections.get() + 1);

                // TODO: TCP socket options here?

                // Perform the websocket handshake on each connection, but don't let
                // it take too long.
                let ws = accept_async(socket).chain_err(|| "failed to accept client");
                let ws = timeout(ws, srv.opts.open_handshake_timeout, &handle);

                // Once the handshake is done we'll start the main communication
                // with the client, managing pings here and deferring to `Client` to
                // start driving the internal state machine.
                let srv2 = srv.clone();
                let tx = tx.clone();
                let client = ws.and_then(move |ws| {
                    PingManager::new(&srv2, tx, ws)
                        .chain_err(|| "failed to make ping handler")
                }).flatten();

                let srv = srv.clone();
                handle.spawn(client.then(move |res| {
                    srv.open_connections.set(srv.open_connections.get() - 1);
                    if let Err(e) = res {
                        // TODO: log this? ignore this? unsure.
                        println!("{}: {}", addr, e);
                        for err in e.iter().skip(1) {
                            println!("\t{}", err);
                        }
                    }
                    Ok(())
                }));

                Ok(())
            });

        core.handle().spawn(ws_srv.then(|res| {
            println!("srv res: {:?}", res.map(drop));
            Ok(())
        }));

        Ok((srv2, core))
    }

    /// Informs this server that a new `client` has connected
    ///
    /// For now just registers internal state by keeping track of the `client`,
    /// namely its channel to send notifications back.
    pub fn connect_client(&self, client: ClientState) {
        // TODO: handle already-present channels
        for id in client.channel_ids.iter() {
            assert!(self.channels.borrow_mut().insert(*id, Channel {
                uaid: client.uaid,
                current_version: 0,
            }).is_none());
        }

        // TODO: if this is a duplicate we should respond by requesting the
        //       client selects a new uaid
        assert!(self.uaids.borrow_mut().insert(client.uaid, client).is_none());
    }

    /// The client specified by `uaid` has just started listening to the channel
    /// identified as `channel_id`.
    pub fn register_channel(&self, uaid: &Uuid, channel_id: &Uuid) -> bool {
        let mut uaids = self.uaids.borrow_mut();
        let mut channels = self.channels.borrow_mut();
        let client = uaids.get_mut(uaid).unwrap();
        if channels.contains_key(channel_id) {
            false
        } else {
            channels.insert(*channel_id, Channel {
                uaid: *uaid,
                current_version: 0,
            });
            client.channel_ids.push(*channel_id);
            true
        }
    }

    /// The client specified by `uaid` has stopped listening to the channel
    /// specified by `channel_id`.
    pub fn unregister_channel(&self, uaid: &Uuid, channel_id: &Uuid) {
        let mut uaids = self.uaids.borrow_mut();
        let mut channels = self.channels.borrow_mut();
        let client = uaids.get_mut(uaid).unwrap();
        if channels.contains_key(channel_id) {
            if channels[channel_id].uaid == *uaid {
                channels.remove(channel_id);
                client.channel_ids.retain(|c| {
                    c != channel_id
                });
            }
        }
    }

    /// A notification has come for the channel specified by `channel_id` and
    /// the channel is now at the version specified as well.
    pub fn notify_client(&self, channel_id: &Uuid, version: u64) {
        let mut channels = self.channels.borrow_mut();
        let channel = match channels.get_mut(&channel_id) {
            Some(channel) => channel,
            None => return,
        };
        channel.current_version = version;

        let mut uaids = self.uaids.borrow_mut();
        let uaid = uaids.get_mut(&channel.uaid).unwrap();
        let notification = if uaid.use_webpush {
            Notification::WebPush {
                channel_id: *channel_id,
                version: version.to_string(),
            }
        } else {
            Notification::Simple {
                updates: vec![Update {
                    channel_id: *channel_id,
                    version: version,
                }],
            }
        };
        (&uaid.tx).send(notification).unwrap();
    }

    /// The client specified by `uaid` has disconnected.
    pub fn disconnet_client(&self, uaid: &Uuid) {
        let mut uaids = self.uaids.borrow_mut();
        let mut channels = self.channels.borrow_mut();
        for id in uaids.remove(uaid).expect("uaid not registered").channel_ids {
            channels.remove(&id).expect("uaid pointed to missing channel");
        }
    }
}

struct PingManager {
    socket: RcObject<WebpushSocket<WebSocketStream<TcpStream>>>,
    ping_interval: Interval,
    timeout: TimeoutState,
    srv: Rc<Server>,
    client: CloseState<Client<RcObject<WebpushSocket<WebSocketStream<TcpStream>>>>>,
}

enum TimeoutState {
    None,
    Ping(Timeout),
    Close(Timeout),
}

enum CloseState<T> {
    Exchange(T),
    Closing,
}

impl PingManager {
    fn new(srv: &Rc<Server>,
           tx: mpsc::UnboundedSender<PythonCall>,
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
        let socket = RcObject::new(WebpushSocket::new(socket));
        Ok(PingManager {
            ping_interval: Interval::new(srv.opts.auto_ping_interval, &srv.handle)?,
            timeout: TimeoutState::None,
            socket: socket.clone(),
            client: CloseState::Exchange(Client::new(socket, tx, srv)),
            srv: srv.clone(),
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
            self.socket.borrow_mut().ping = true;
            let timeout = Timeout::new(self.srv.opts.auto_ping_timeout, &self.srv.handle)?;
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
                CloseState::Exchange(ref mut client) => try_ready!(client.poll()),
                CloseState::Closing => return Ok(self.socket.close()?),
            }

            self.client = CloseState::Closing;
            if let Some(dur) = self.srv.opts.close_handshake_timeout {
                let timeout = Timeout::new(dur, &self.srv.handle)?;
                self.timeout = TimeoutState::Close(timeout);
            }
        }
    }
}

// Wrapper struct to take a Sink/Stream of `Message` to a Sink/Stream of
// `ClientMessage` and `ServerMessage`.
struct WebpushSocket<T> {
    inner: T,
    pong: Pong,
    ping: bool,
}

enum Pong {
    None,
    Received,
    Waiting(Task),
}

impl<T> WebpushSocket<T> {
    fn new(t: T) -> WebpushSocket<T> {
        WebpushSocket {
            inner: t,
            pong: Pong::None,
            ping: false,
        }
    }

    fn poll_pong(&mut self) -> Async<()> {
        match mem::replace(&mut self.pong, Pong::None) {
            Pong::None => {}
            Pong::Received => return Async::Ready(()),
            Pong::Waiting(_) => {}
        }
        self.pong = Pong::Waiting(task::current());
        Async::NotReady
    }

    fn send_ping(&mut self) -> Poll<(), Error>
        where T: Sink<SinkItem = Message>, Error: From<T::SinkError>
    {
        if self.ping {
            match self.inner.start_send(Message::Ping(Vec::new()))? {
                AsyncSink::Ready => self.ping = false,
                AsyncSink::NotReady(_) => return Ok(Async::NotReady),
            }
        }
        Ok(Async::Ready(()))
    }
}

impl<T> Stream for WebpushSocket<T>
    where T: Stream<Item = Message>,
          Error: From<T::Error>,
{
    type Item = ClientMessage;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<ClientMessage>, Error> {
        loop {
            match try_ready!(self.inner.poll()) {
                Some(Message::Text(ref s)) => {
                    let msg = serde_json::from_str(s).chain_err(|| "invalid json text")?;
                    return Ok(Some(msg).into())
                }

                Some(Message::Binary(_)) => {
                    return Err("binary messages not accepted".into())
                }

                // sending a pong is already managed by lower layers, just go to
                // the next message
                Some(Message::Ping(_)) => {}

                // Wake up tasks waiting for a pong, if any.
                Some(Message::Pong(_)) => {
                    match mem::replace(&mut self.pong, Pong::Received) {
                        Pong::None => {}
                        Pong::Received => {}
                        Pong::Waiting(task) => {
                            self.pong = Pong::None;
                            task.notify();
                        }
                    }
                }

                None => return Ok(None.into()),
            }
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
        if self.send_ping()?.is_not_ready() {
            return Ok(AsyncSink::NotReady(msg))
        }
        let s = serde_json::to_string(&msg).chain_err(|| "failed to serialize")?;
        match self.inner.start_send(Message::Text(s))? {
            AsyncSink::Ready => Ok(AsyncSink::Ready),
            AsyncSink::NotReady(_) => Ok(AsyncSink::NotReady(msg)),
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Error> {
        try_ready!(self.send_ping());
        Ok(self.inner.poll_complete()?)
    }

    fn close(&mut self) -> Poll<(), Error> {
        try_ready!(self.poll_complete());
        Ok(self.inner.close()?)
    }
}
