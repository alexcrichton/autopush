from autopush_rs._native import ffi, lib
from twisted.internet import reactor
import json

# TODO: should use the new `ffi.def_extern` style callbacks, requires changes to
#       snaek.
@ffi.callback("void(size_t)")
def _dispatch_server_ready_to_reactor(id):
    reactor.callFromThread(_reactor_server_ready, id)

# TODO: can this global state be avoided? Need to figure out how to have some
#       contextual data referenced on a foreign thread and passed to
#       `_dispatch_server_ready_to_reactor` above. Unsure if this is actually
#       safe to do at all.
global_server = None

def _reactor_server_ready(id):
    global global_server
    assert(global_server is not None)
    if id == 0:
        global_server.accept_clients()
    else:
        global_server._client_ready(id)

class AutopushServer:
    def __init__(self, addr):
        global global_server
        assert(global_server is None)

        ptr = _call(lib.autopush_server_new,
                    addr,
                    _dispatch_server_ready_to_reactor)
        self.ffi = ffi.gc(ptr, lib.autopush_server_free)
        self.clients = []
        self.next_idx = 0

        global_server = self

    # Accepts all pending clients
    def accept_clients(self):
        while True:
            client = self._accept_client()
            if client is None:
                break

            # Push this client on our local slab of clients
            id = self.next_idx
            if self.next_idx == len(self.clients):
                self.next_idx += 1
                self.clients.append(None)
            else:
                self.next_idx = self.clients[id]
            self.clients[id] = client

            # Kick off the client's I/O and registration by checking for
            # messages
            self._client_ready(id + 1)

    def _client_ready(self, id):
        id = id - 1
        if id >= len(self.clients):
            return

        # Move the client forward. If it's not done yet we bail out, but if
        # it's finished then we remove its internal resources.
        client = self.clients[id]
        if client._poll():
            return
        self.clients[id] = self.next_idx
        self.next_idx = id
        client._free_ffi()

    # Attempt to accept a client.
    #
    # If `None` is returned then no client was ready to be accepted. When a
    # client is ready to be accepted the global
    # `dispatch_server_ready_to_reactor` callback will be invoked.
    #
    # Otherwise if a client is accepted then this returns an instance of
    # `AutopushClient`
    def _accept_client(self):
        ret = _call(lib.autopush_server_accept_client, self.ffi, self.next_idx + 1)
        if ffi.cast('size_t', ret) == 1:
            return None
        return AutopushClient(ret)

class AutopushClient:
    def __init__(self, ptr):
        self.ffi = ffi.gc(ptr, lib.autopush_client_free)
        self.done = False
        self.message_queue = []

    def _poll(self):
        while not self.done:
            msg = self._poll_incoming()
            if msg is None:
                break
            print('client message: ' + str(msg.json()))
            msg._free_ffi()
            self.message_queue.append(AutopushMessage.from_json({"c": 0}))

        while not self.done and len(self.message_queue) > 0:
            ret = _call(lib.autopush_client_send,
                        self.ffi,
                        self.message_queue[0].ffi)

            # Indicates a write failure to the client, so we just abort and
            # clean ourselves up.
            if ret == 1:
                self.done = True

            # Indicates we're not ready to accept the message yet, so leave the
            # message in our queue and keep going.
            elif ret == 2:
                break

            # Otherwise we successfully sent the message, so take our message
            # out of the queue.
            else:
                msg = self.message_queue.pop(0)
                msg._free_ffi()

        return not self.done

    def _poll_incoming(self):
        ret = _call(lib.autopush_client_poll_incoming, self.ffi)
        cast = ffi.cast('size_t', ret)
        if cast == 1:
            return None
        elif cast == 2:
            self.done = True
            return None
        else:
            return AutopushMessage(ret)

    def _free_ffi(self):
        for msg in self.message_queue:
            msg._free_ffi()
        ffi.gc(self.ffi, None)
        lib.autopush_client_free(self.ffi)
        self.ffi = None

class AutopushMessage:
    def __init__(self, ptr):
        self.ffi = ffi.gc(ptr, lib.autopush_message_free)

    @classmethod
    def from_json(cls, j):
        s = json.dumps(j)
        ptr = _call(lib.autopush_message_new, s)
        return cls(ptr)

    def json(self):
        ptr = _call(lib.autopush_message_ptr, self.ffi)
        len = _call(lib.autopush_message_len, self.ffi) - 1
        buf = ffi.buffer(ptr, len)
        return json.loads(buf[:])

    def _free_ffi(self):
        ffi.gc(self.ffi, None)
        lib.autopush_message_free(self.ffi)
        self.ffi = None

last_err = None

def _call(f, *args):
    # We cache errors across invocations of `_call` to avoid allocating a new
    # error each time we call an FFI function. Each function call, however,
    # needs a unique error, so take the global `last_err`, lazily initializing
    # it if necessary.
    global last_err
    my_err = last_err
    last_err = None
    if my_err is None:
        my_err = ffi.new('AutopushError*')

    # The error pointer is always the last argument, so pass that in and call
    # the actual FFI function. If the return value is nonzero then it was a
    # successful call and we can put our error back into the global slot
    # and return.
    args = args + (my_err,)
    ret = f(*args)
    if ffi.cast('size_t', ret) != 0:
        last_err = my_err
        return ret

    # If an error happened then it means that the Rust side of things panicked
    # which we need to handle here. Acquire the string from the error, if
    # available, and re-raise as a python `RuntimeError`.
    #
    # Note that we're also careful here to clean up the error's internals to
    # avoid memory leaks and then once we're completely done we can restore our
    # local error to its global position.
    errln = lib.autopush_error_msg_len(my_err);
    if errln > 0:
        ptr = lib.autopush_error_msg_ptr(my_err)
        msg = 'rust panic: ' + ffi.buffer(ptr, errln)[:]
        exn = RuntimeError(msg)
    else:
        exn = RuntimeError('unknown error in rust')
    lib.autopush_error_cleanup(my_err)
    last_err = my_err
    raise exn

