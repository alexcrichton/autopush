from autopush_rs._native import ffi, lib
from twisted.application import service
from twisted.internet import reactor
from autopush.websocket import PushServerProtocol
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
    if global_server is None:
        return
    if id == 0:
        global_server._accept_clients()
    else:
        global_server._client_ready(id)

def ffi_from_buffer(s):
    if s is None:
        return ffi.NULL
    else:
        return ffi.from_buffer(s)

class AutopushServer(service.Service):
    def __init__(self, settings, db, agent, clients):
        cfg = ffi.new('AutopushServerOptions*')
        cfg.auto_ping_interval = settings.auto_ping_interval
        cfg.auto_ping_timeout = settings.auto_ping_timeout
        cfg.close_handshake_timeout = settings.close_handshake_timeout
        cfg.max_connections = settings.max_connections
        cfg.open_handshake_timeout = 5
        cfg.port = settings.port
        cfg.ssl_cert = ffi_from_buffer(settings.ssl_cert)
        cfg.ssl_dh_param = ffi_from_buffer(settings.ssl_dh_param)
        cfg.ssl_key = ffi_from_buffer(settings.ssl_key)
        cfg.url = ffi_from_buffer(settings.ws_url)

        ptr = _call(lib.autopush_server_new, cfg)
        self.ffi = ffi.gc(ptr, lib.autopush_server_free)

        self.clients = []
        self.next_idx = 0

        class Factory:
            pass

        self.factory = Factory()
        self.factory.ap_settings = settings
        self.factory.db = db
        self.factory.agent = agent
        self.factory.clients = clients

    def startService(self):
        global global_server
        assert(global_server is None)
        global_server = self
        _call(lib.autopush_server_start,
              self.ffi,
              _dispatch_server_ready_to_reactor)
        self._accept_clients()

    def stopService(self):
        global global_server
        assert(global_server is not None)
        _call(lib.autopush_server_stop, self.ffi)
        while len(self.clients) > 0:
            self._remove_client(0)
        self._free_ffi()
        global_server = None

    # Accepts all pending clients
    def _accept_clients(self):
        while True:
            client = self._accept_client()
            if client is None:
                break

            client.protocol.onConnect(None)

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
        idx = id - 1
        if idx >= len(self.clients):
            return

        # Move the client forward. If it's not done yet keep going, but if
        # it's finished then we remove its internal resources.
        if self.clients[idx]._poll():
            pass
        else:
            self._remove_client(idx)

    def _remove_client(self, idx):
        client = self.clients[idx]
        self.clients[idx] = self.next_idx
        self.next_idx = idx
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
        return AutopushClient(ret, self.factory, self.next_idx + 1)

    def _free_ffi(self):
        ffi.gc(self.ffi, None)
        lib.autopush_server_free(self.ffi)
        self.ffi = None

class AutopushTransport:
    def __init__(self, id):
        self._paused = False
        self._message_queue = []
        self.producer = None
        self.id = id

    def registerProducer(self, producer, streaming):
        assert(streaming)
        self.producer = producer

    def pauseProducing(self):
        self._paused = True

    def resumeProducing(self):
        self._paused = False
        reactor.callLater(0, _reactor_server_ready, self.id)

class AutopushClient:
    def __init__(self, ptr, factory, id):
        self.ffi = ffi.gc(ptr, lib.autopush_client_free)
        self.done = False
        self.id = id
        self.protocol = PushServerProtocol()
        self.protocol.factory = factory
        self.protocol._log_exc = False
        self.protocol.transport = AutopushTransport(id)
        self.protocol.autoPingInterval = factory.ap_settings.auto_ping_interval

        def sendMessage(self, msg, binary):
            assert(not binary)
            self.transport._message_queue.append(AutopushMessage.from_str(msg))
            reactor.callLater(0, _reactor_server_ready, self.transport.id)
        setattr(PushServerProtocol, 'sendMessage', sendMessage)

    def _poll(self):
        while not self.done and not self.protocol.transport._paused:
            msg = self._poll_incoming()
            if msg is None:
                break
            j = msg.json()
            msg._free_ffi()
            self.protocol.onMessage(json.dumps(j), False)

        while not self.done and len(self.protocol.transport._message_queue) > 0:
            ret = _call(lib.autopush_client_send,
                        self.ffi,
                        self.protocol.transport._message_queue[0].ffi)

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
                msg = self.protocol.transport._message_queue.pop(0)
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
        for msg in self.protocol.transport._message_queue:
            msg._free_ffi()
        ffi.gc(self.ffi, None)
        lib.autopush_client_free(self.ffi)
        self.ffi = None

class AutopushMessage:
    def __init__(self, ptr):
        self.ffi = ffi.gc(ptr, lib.autopush_message_free)

    @classmethod
    def from_str(cls, s):
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

