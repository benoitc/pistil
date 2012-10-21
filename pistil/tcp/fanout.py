import struct
import os
import socket
import json

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
        response = self.do_event(**evt)
        if response is not None:
            blob = self.conf['serializer'](response)
            sock.sendall(struct.pack('i', len(blob)))
            sock.sendall(blob)
        close(sock)

    def do_event(self, event):
        raise NotImplementedError()


class Seller(object):
    def __init__(self, address, serialize, unserialize):
        self.address = parse_address(address)
        self.serialize = serialize
        self.unserialize = unserialize

    def tell(self, **msg):
        "tell something to the factory, don't wait for answer"
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(self.address)
        blob = self.serialize(msg)
        sock.sendall(struct.pack('i', len(blob)))
        sock.sendall(blob)

    def ask(self, **msg):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(self.address)
        blob = self.serialize(msg)
        sock.sendall(struct.pack('i', len(blob)))
        sock.sendall(blob)
        size = struct.unpack('i', read(sock, 4))[0]
        blob = read(sock, size)
        return self.unserialize(blob)


def factory(worker, num_workers=5, serializer=json.dumps, unserializer=json.loads, **your_conf):
    sockname = '/tmp/toto.sock'  # TODO more dynamic name.
    try:
        os.remove(sockname)
    except Exception:
        pass
    conf = {"num_workers": num_workers,
            "serializer": serializer,
            "unserializer": unserializer,
            "address": "unix:%s" % sockname,
            "fanout.worker": worker}
    conf.update(your_conf)
    seller = Seller(conf['address'], conf['serializer'], conf['unserializer'])
    return conf, seller


if __name__ == '__main__':
    import time
    from pistil.worker import Worker as ClassicWorker

    class Test(Worker):
        def do_event(self, msg, **args):
            print self.pid, msg, args
            time.sleep(1)

    class Client(ClassicWorker):
        def handle(self):
            self.conf['seller'].tell(msg="Hello world", age=42)

    conf, seller = factory(Test)
    conf['seller'] = seller
    specs = [
        (Foreman, 30, "supervisor", {}, "foreman"),
        (Client, 30, "worker", {}, "client")
    ]

    arbiter = Arbiter(conf, specs)
    arbiter.run()
