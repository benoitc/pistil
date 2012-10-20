import struct
import os
import socket

from pistil.tcp.arbiter import TcpArbiter
from pistil.tcp.sync_worker import TcpSyncWorker
from pistil.util import close, parse_address
from pistil.arbiter import Arbiter


class Foreman(TcpArbiter):
    "Launch and manage workers."
    def on_init(self, conf):
        TcpArbiter.on_init(self, conf)
        # we return a spec
        return (conf['fanout.worker'], 30, "worker", {}, "worker",)


def read(sock, size):
    "Read some octets on a socket"
    buff = []
    total = size
    while total:
        tmp = sock.recv(total)
        buff.append(tmp)
        total -= len(tmp)
    resp = ''.join(buff)
    return resp


class Worker(TcpSyncWorker):
    def handle(self, sock, addr):
        #FIXME implement timeout
        size = struct.unpack('i', read(sock, 4))[0]
        blob = read(sock, size)
        evt = self.conf['unserializer'](blob)
        self.do_event(**evt)
        close(sock)

    def do_event(self, event):
        raise NotImplementedError()


class Seller(object):
    def __init__(self, address, serialize):
        self.address = parse_address(address)
        self.serialize = serialize

    def tell(self, **msg):
        "tell something to the factory, don't wait for answer"
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(self.address)
        blob = self.serialize(msg)
        sock.sendall(struct.pack('i', len(blob)))
        sock.sendall(blob)
        close(sock)


def factory(worker, num_workers=5, **your_conf):
    sockname = '/tmp/toto.sock'
    try:
        os.remove(sockname)
    except Exception:
        pass
    conf = {"num_workers": num_workers,
            "address": "unix:%s" % sockname,
            "fanout.worker": worker}
    conf.update(your_conf)
    seller = Seller(conf['address'], conf['serializer'])
    return conf, seller


if __name__ == '__main__':
    import time
    import json
    from pistil.worker import Worker as ClassicWorker

    class Test(Worker):
        def do_event(self, msg, **args):
            print self.pid, msg, args
            time.sleep(1)

    class Client(ClassicWorker):
        def handle(self):
            self.conf['seller'].tell(msg="Hello world", age=42)

    conf, seller = factory(Test, serializer=json.dumps, unserializer=json.loads)
    conf['seller'] = seller
    specs = [
        (Foreman, 30, "supervisor", {}, "foreman"),
        (Client, 30, "worker", {}, "client")
    ]

    arbiter = Arbiter(conf, specs)
    arbiter.run()
