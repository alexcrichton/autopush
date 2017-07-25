use std::rc::Rc;
use std::cell::{RefCell, RefMut};

use futures::{Stream, Sink, StartSend, Poll};

// Helper object to turn `Rc<RefCell<T>>` into a `Stream` and `Sink`
pub struct RcObject<T>(Rc<RefCell<T>>);

impl<T> RcObject<T> {
    pub fn new(t: T) -> RcObject<T> {
        RcObject(Rc::new(RefCell::new(t)))
    }

    pub fn borrow_mut(&self) -> RefMut<T> {
        self.0.borrow_mut()
    }
}

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

impl<T> Clone for RcObject<T> {
    fn clone(&self) -> RcObject<T> {
        RcObject(self.0.clone())
    }
}
