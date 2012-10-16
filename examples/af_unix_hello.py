# -*- coding: utf-8 -
#
# This file is part of pistil released under the MIT license.
# See the NOTICE for more information.
import os

from pistil.tcp.sync_worker import TcpSyncWorker
from pistil.tcp.arbiter import TcpArbiter

from http_parser.reader import SocketReader


class MyTcpWorker(TcpSyncWorker):

    def handle(self, sock, addr):
        s = SocketReader(sock)
        l = s.readline()
        print l
        sock.send(l)

if __name__ == '__main__':
    try:
        os.remove('/tmp/pistil.sock')
    except Exception:
        pass
    conf = {"num_workers": 3, "address": "unix:/tmp/pistil.sock"}
    spec = (MyTcpWorker, 30, "worker", {}, "worker",)

    arbiter = TcpArbiter(conf, spec)
    print "try to connect with :"
    print "nc -U /tmp/pistil.sock"

    arbiter.run()
