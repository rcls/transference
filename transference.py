#!/usr/bin/python3

from socket import *
from selectors import *
import struct
from typing import Optional, Tuple
from unittest import TestCase

BUFFER_SIZE = 1024

class Dispatchable(object):
    def inready(self) -> None:
        pass
    def outready(self) -> None:
        pass
    def reregister(self, sel : BaseSelector) -> None:
        pass
    def kill(self) -> None:
        pass

class SocketReader(Dispatchable):
    def __init__(self, s : socket):
        self.s = s
        self.wanted = True
        self.isreg = False
    def reregister(self, sel : BaseSelector) -> None:
        if self.wanted and not self.isreg:
            sel.register(self.s, EVENT_READ, self)
        elif self.isreg and not self.wanted:
            sel.unregister(self.s)
        self.isreg = self.wanted
    def kill(self) -> None:
        self.s.close()
        self.wanted = False

class Endpoint(Dispatchable):
    s         : socket
    sinkready : bool
    connected : bool
    peer      : 'Endpoint'
    buffer    : Optional[memoryview]
    regevents : int

    __slots__ = 's', 'sinkready', 'connected', 'peer', 'buffer', 'regevents'
    def __init__(self, s : socket, connected : bool):
        self.s         = s
        self.sinkready = True
        self.connected = connected
        self.buffer    = None # Output buffer
        self.regevents = 0

    def inready(self) -> None:
        if not self.sinkready:
            return

        try:
            data = self.s.recv(BUFFER_SIZE)
            fullread = len(data) == BUFFER_SIZE
        except BlockingIOError as e:
            return
        except OSError as e:
            # Set SO_LINGER and then treat as EOF.
            self.peer.so_linger_0()
            self.kill()
            return

        self.sinkready = self.peer.out(memoryview(data))

    # Data should not be none; use b'' for EOF!
    def do_out(self, data : memoryview) -> bool:
        self.buffer = None
        try:
            n = self.s.send(data)
        except BlockingIOError:
            self.buffer = data
            return False
        except OSError as e:
            # We should get a read error on the input which will propagate...
            return False                # No more data please.

        if n == len(data):
            return True

        self.buffer = data[n:]
        return False

    def out(self, data : memoryview) -> bool:
        assert(self.buffer is None)
        if not data:
            try:
                self.s.shutdown(SHUT_WR)
                self.connected = True
            except OSError as e:
                pass
            return False
        if self.connected:
            return self.do_out(data)
        self.buffer = data
        return False

    def outready(self) -> None:
        self.connected = True
        if self.buffer is not None:
            self.peer.sinkready = self.do_out(self.buffer)

    def reregister(self, sel) -> None:
        for S in self, self.peer:
            wanted = EVENT_READ if S.sinkready and S.connected else 0
            if S.buffer is not None or not S.connected:
                wanted |= EVENT_WRITE
            if wanted == S.regevents:
                continue
            elif S.regevents == 0:
                sel.register(S.s, wanted, S)
            elif wanted == 0:
                sel.unregister(S.s)
            else:
                sel.modify(S.s, wanted, S)
            S.regevents = wanted
        if self.regevents == 0 and self.peer.regevents == 0:
            self.kill()

    def so_linger_0(self) -> None:
        try:
            self.s.setsockopt(SOL_SOCKET, SO_LINGER, struct.pack('ii', 1, 0))
        except OSError:
            pass

    def kill(self) -> None:
        for S in self, self.peer:
            S.s.close()
            S.sinkready = False
            S.connected = True
            S.buffer = None

class Poller:
    selector : BaseSelector
    def __init__(self) -> None:
        self.selector = DefaultSelector()

    def add(self, e : Dispatchable) -> None:
        e.reregister(self.selector)

    def dispatch(self, timeout) -> None:
        L = self.selector.select(timeout)
        for (obj, fd, events, data), ev in L:
            if isinstance(data, Dispatchable):
                if ev & EVENT_READ:
                    data.inready()
                if ev & EVENT_WRITE:
                    data.outready()
            else:
                data(obj, ev)
        for (obj, fd, events, data), ev in L:
            if isinstance(data, Dispatchable):
                data.reregister(self.selector)

class EndPointTests(TestCase):
    a : socket
    b : socket
    c : socket
    d : socket
    B : Endpoint
    C : Endpoint
    E : Poller

    @classmethod
    def lowlatency(self, s):
        s.setsockopt(IPPROTO_TCP, TCP_NODELAY, 1)
        #s.setsockopt(IPPROTO_TCP, TCP_MAXSEG, 88)
        #print(s.getsockopt(IPPROTO_TCP, TCP_MAXSEG))
        #s.setsockopt(IPPROTO_TCP, TCP_QUICKACK, 1)

    @classmethod
    def tcppair(self) -> Tuple[socket, socket]:
        acceptor = socket(AF_INET, SOCK_STREAM)
        acceptor.listen(1)
        client = socket(AF_INET, SOCK_STREAM)
        client.connect(acceptor.getsockname())
        server, _ = acceptor.accept()
        acceptor.close()
        client.settimeout(0)
        server.settimeout(0)
        self.lowlatency(client)
        self.lowlatency(server)
        return client, server

    def setUp(self):
        self.b, self.a = self.tcppair()
        self.d, self.c = self.tcppair()
        self.e = DefaultSelector()
        self.B = Endpoint(self.b, True)
        self.C = Endpoint(self.c, False)
        self.B.peer = self.C
        self.C.peer = self.B
        self.E = Poller()
    def tearDown(self):
        for X in self.a, self.b, self.c, self.d:
            X.close()

    def test_connecting(self):
        a, B, C, d = self.a, self.B, self.C, self.d
        C.connected = False
        a.sendall(b'abcd')
        self.assertTrue(B.sinkready)
        B.inready()
        self.assertFalse(B.sinkready)
        C.outready()
        self.assertTrue(B.sinkready)
        self.assertEqual(d.recv(10), b'abcd')
        B.inready()
        self.assertTrue(B.sinkready)
        self.assertRaises(BlockingIOError, d.recv, 10)

    def test_overload(self):
        # Pump data into the socket until it jams completely.
        # Then check we readout exactly the correct data.
        self.a.setsockopt(SOL_SOCKET, SO_SNDBUF, 12345 * 4)
        self.b.setsockopt(SOL_SOCKET, SO_RCVBUF, 12345 * 3)
        self.c.setsockopt(SOL_SOCKET, SO_SNDBUF, 12345 * 2)
        self.d.setsockopt(SOL_SOCKET, SO_RCVBUF, 12345)

        sent = 0
        prime = 251
        data = memoryview(bytes(range(prime)) * (3 * BUFFER_SIZE // prime))
        one_k = data[0:1024]
        self.E.add(self.B)
        self.E.add(self.C)
        while True:
            self.E.dispatch(0)
            try:
                sent += self.a.send(one_k[sent % prime :])
            except BlockingIOError:
                break
        self.assertTrue(sent > len(one_k))
        self.a.shutdown(SHUT_WR)
        # Now drive the I/O and read back, checking the data.
        recv = 0
        self.E.add(SocketReader(self.d))
        while True:
            self.E.dispatch(1)
            try:
                got = self.d.recv(BUFFER_SIZE)
            except BlockingIOError:
                continue

            if not got:
                break

            expect = data[recv % prime : recv % prime + len(got)]
            self.assertEqual(got, expect)
            recv += len(got)
        self.assertEqual(sent, recv)
        self.assertFalse(self.B.regevents & EVENT_READ)

    def test_reset_propagation(self):
        self.E.add(self.B)
        self.E.add(self.C)
        self.E.add(SocketReader(self.e))
        self.a.setsockopt(SOL_SOCKET, SO_LINGER, struct.pack('ii', 1, 0))
        self.a.close()
        # Now drive until d doesn't give EAGAIN.
        while True:
            self.E.dispatch(1)
            try:
                got = self.d.recv(10)
                self.fail("Shouldn't get here " + str(got))
            except ConnectionResetError:
                break
            except BlockingIOError:
                pass
        self.E.dispatch(0)
