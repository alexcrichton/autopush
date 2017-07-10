from autopush_rs import AutopushServer
from twisted.internet import reactor

print("================================ start")

srv = AutopushServer("127.0.0.1:8080")
srv.accept_clients()
reactor.run()
