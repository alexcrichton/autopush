#[macro_use]
extern crate futures;
extern crate hyper;
extern crate libc;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate time;
extern crate tokio_core;
extern crate tokio_service;
extern crate tokio_tungstenite;
extern crate tungstenite;
extern crate uuid;

#[macro_use]
extern crate error_chain;

use futures::executor::Notify;

mod client;
mod errors;
mod http;
mod protocol;
mod util;

#[macro_use]
pub mod rt;
pub mod call;
pub mod server;

pub struct MyNotify(extern fn(usize));

impl Notify for MyNotify {
    fn notify(&self, id: usize) {
        (self.0)(id);
    }
}
