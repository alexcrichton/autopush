from autopush_rs import AutopushServer
from twisted.internet import reactor

print("================================ start")

class Settings:
    pass

settings = Settings()
settings.auto_ping_interval = 60.0
settings.auto_ping_timeout = 5.0
settings.close_handshake_timeout = 0
settings.max_connections = 0
settings.port = 8080
settings.ssl_cert = None
settings.ssl_dh_param = None
settings.ssl_key = None
settings.ws_url = 'a'

class Dispatch:
    def handle(self, call):
        if call.name() == 'add':
            a, b = call.args()
            call.complete(a + b)
        if call.name() == 'verify-hello':
            uaid = call.args()
            print('connecting ' + uaid)
            call.complete(200)
        else:
            call.cancel()
            raise RuntimeError("unknown call: {}".format(call.name()))

srv = AutopushServer(settings, Dispatch())
srv.startService()
reactor.run()
