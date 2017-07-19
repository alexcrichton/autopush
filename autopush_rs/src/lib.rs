#[macro_use]
extern crate futures;
extern crate libc;
extern crate tokio_core;
extern crate tokio_tungstenite;
extern crate tungstenite;

use futures::executor::Notify;

#[macro_use]
pub mod rt;
pub mod client;
pub mod server;
pub mod message;

pub struct MyNotify(extern fn(usize));

impl Notify for MyNotify {
    fn notify(&self, id: usize) {
        (self.0)(id);
    }
}
