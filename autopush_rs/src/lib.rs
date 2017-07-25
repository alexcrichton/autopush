#[macro_use]
extern crate futures;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate tokio_core;
extern crate tokio_tungstenite;
extern crate tungstenite;
extern crate uuid;
extern crate libc;

#[macro_use]
extern crate error_chain;

use futures::executor::Notify;

mod errors;
mod protocol;
mod util;

#[macro_use]
pub mod rt;
pub mod call;
pub mod client;
pub mod message;
pub mod server;

pub struct MyNotify(extern fn(usize));

impl Notify for MyNotify {
    fn notify(&self, id: usize) {
        (self.0)(id);
    }
}
