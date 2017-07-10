use std::cell::RefCell;
use std::sync::Arc;

use futures::{Async, AsyncSink};
use futures::executor::Spawn;
use futures::sync::mpsc;
use tungstenite::Message;

use MyNotify;
use message::AutopushMessage;
use rt::{self, AutopushError, UnwindGuard};
use server::ServerInner;

#[repr(C)]
pub struct AutopushClient {
    inner: UnwindGuard<ClientInner>,
    notify: Arc<MyNotify>,
    id: usize,
}

pub struct ClientInner {
    pub tx: RefCell<Spawn<mpsc::Sender<Message>>>,
    pub rx: RefCell<Spawn<mpsc::Receiver<Message>>>,
}

#[no_mangle]
pub extern "C" fn autopush_client_poll_incoming(client: *mut AutopushClient,
                                                err: &mut AutopushError)
    -> *mut AutopushMessage
{
    unsafe {
        let notify = &(*client).notify;
        let id = (*client).id;
        (*client).inner.catch(err, |client| {
            match client.poll_incoming(&notify, id) {
                Async::Ready(Some(msg)) => {
                    Box::into_raw(Box::new(AutopushMessage::new(msg)))
                }
                Async::Ready(None) => 2 as *mut _,
                Async::NotReady => 1 as *mut _,
            }
        })
    }
}

#[no_mangle]
pub extern "C" fn autopush_client_send(client: *mut AutopushClient,
                                       msg: *mut AutopushMessage,
                                       err: &mut AutopushError)
    -> usize
{
    unsafe {
        let notify = &(*client).notify;
        let id = (*client).id;
        let msg = &mut *msg;
        (*client).inner.catch(err, |client| {
            let mut tx = client.tx.borrow_mut();
            let m = msg.replace(Message::Text(String::new()));
            match tx.start_send_notify(m, notify, id) {
                Ok(AsyncSink::Ready) => 3,
                Ok(AsyncSink::NotReady(m)) => {
                    msg.replace(m);
                    2
                }
                Err(_) => 1,
            }
        })
    }
}

#[no_mangle]
pub extern "C" fn autopush_client_free(client: *mut AutopushClient) {
    rt::abort_on_panic(|| unsafe {
        Box::from_raw(client);
    })
}

impl ClientInner {
    fn poll_incoming(&self,
                     notify: &Arc<MyNotify>,
                     id: usize) -> Async<Option<Message>> {
        let mut rx = self.rx.borrow_mut();
        rx.poll_stream_notify(notify, id).expect("streams cannot error")
    }
}

impl AutopushClient {
    pub fn new(inner: ClientInner,
               srv: &ServerInner,
               id: usize) -> AutopushClient {
        AutopushClient {
            inner: UnwindGuard::new(inner),
            notify: srv.notify().clone(),
            id: id,
        }
    }
}
