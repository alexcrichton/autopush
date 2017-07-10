use std::ffi::CStr;
use std::mem;

use libc::c_char;
use tungstenite::Message;

use rt::{self, AutopushError};

pub struct AutopushMessage {
    inner: Message,
}

impl AutopushMessage {
    pub fn new(msg: Message) -> AutopushMessage {
        AutopushMessage {
            inner: msg,
        }
    }

    fn as_ptr(&self) -> *const u8 {
        match self.inner {
            Message::Text(ref s) => s.as_ptr(),
            Message::Binary(ref s) => s.as_ptr(),
        }
    }

    fn len(&self) -> usize {
        match self.inner {
            Message::Text(ref s) => s.len(),
            Message::Binary(ref s) => s.len(),
        }
    }

    pub fn replace(&mut self, msg: Message) -> Message {
        mem::replace(&mut self.inner, msg)
    }
}

#[no_mangle]
pub extern "C" fn autopush_message_new(msg: *const c_char,
                                       err: &mut AutopushError)
    -> *const AutopushMessage
{
    rt::catch(err, || unsafe {
        let msg = CStr::from_ptr(msg).to_str().unwrap();
        Box::new(AutopushMessage {
            inner: Message::Text(msg.to_string()),
        })
    })
}

#[no_mangle]
pub extern "C" fn autopush_message_ptr(msg: *const AutopushMessage,
                                       err: &mut AutopushError)
    -> *const u8
{
    rt::catch(err, || unsafe {
        (&*msg).as_ptr()
    })
}

#[no_mangle]
pub extern "C" fn autopush_message_len(msg: *const AutopushMessage,
                                       err: &mut AutopushError)
    -> usize
{
    rt::catch(err, || unsafe {
        (&*msg).len()
    })
}

#[no_mangle]
pub extern "C" fn autopush_message_free(msg: *mut AutopushMessage) {
    rt::abort_on_panic(|| unsafe {
        Box::from_raw(msg);
    })
}

impl Drop for AutopushMessage {
    fn drop(&mut self) {
        if !cfg!(debug_assertions) {
            return
        }
        let buf = match self.inner {
            Message::Binary(ref mut v) => v,
            Message::Text(ref mut s) => unsafe { s.as_mut_vec() },
        };
        for byte in buf {
            *byte = 0;
        }
    }
}
