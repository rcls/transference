#!/usr/bin/python3

import pytest
from socket import *
from selectors import BaseSelector, DefaultSelector, EVENT_READ, EVENT_WRITE
import struct
from typing import Optional, Tuple

BUFFER_SIZE = 1024

class Dispatchable(object):
    s: socket
    __slots__ = 's'
    def inready(self) -> None:
        pass
    def outready(self) -> None:
        pass
    def reregister(self, sel: BaseSelector) -> None:
        pass
    def kill(self) -> None:
        pass

class SocketReader(Dispatchable):
    __slots__ = 'wanted', 'isreg'
    def __init__(self, s: socket):
        self.s = s
        self.wanted = True
        self.isreg = False
    def reregister(self, sel: BaseSelector) -> None:
        if self.wanted and not self.isreg:
            sel.register(self.s, EVENT_READ, self)
        elif self.isreg and not self.wanted:
            sel.unregister(self.s)
        self.isreg = self.wanted
    def kill(self) -> None:
        self.s.close()
        self.wanted = False

class Sink(object):
    def do_out(self, data: memoryview) -> bool:
        pass

class Filter(Sink):
    __slots__ = 'chain'

class EndPoint(Dispatchable, Sink):
    sinkready: bool
    connected: bool
    peer     : 'EndPoint'
    buffer   : Optional[memoryview]
    regevents: int

    __slots__ = 'sinkready', 'connected', 'peer', 'buffer', 'regevents'
    def __init__(self, s: socket, connected: bool):
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
        except BlockingIOError as e:
            return
        except OSError as e:
            # Set SO_LINGER and then close everything down.
            self.peer.so_linger_0()
            self.kill()
            return

        self.sinkready = self.peer.out(memoryview(data))

    # Data should not be none; use b'' for EOF!
    def do_out(self, data: memoryview) -> bool:
        self.buffer = None
        try:
            n = self.s.send(data)       # type: ignore
        except BlockingIOError:
            self.buffer = data
            return False
        except OSError as e:
            self.peer.so_linger_0()
            self.kill()
            return False                # No more data please.

        if n == len(data):
            return True

        self.buffer = data[n:]
        return False

    def out(self, data: memoryview) -> bool:
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
    __slots__ = 'selector',
    selector: BaseSelector
    def __init__(self) -> None:
        self.selector = DefaultSelector()

    def add(self, e: Dispatchable) -> None:
        e.reregister(self.selector)

    def dispatch(self, timeout) -> bool:
        L = self.selector.select(timeout)
        for (obj, fd, events, data), ev in L:
            if ev & EVENT_READ:
                data.inready()
            if ev & EVENT_WRITE:
                data.outready()
        for (obj, fd, events, data), ev in L:
            data.reregister(self.selector)
        return bool(L)

class FlowAcceptor(SocketReader):
    __slots__ = 'destfamily', 'destaddress', 'selector', 'last'
    family: int
    selector: BaseSelector
    def __init__(self, s: socket, destfamily, destaddress, sel: BaseSelector):
        super(FlowAcceptor, self).__init__(s)
        self.destfamily  = destfamily
        self.destaddress = destaddress
        self.selector    = sel
    def inready(self) -> None:
        try:
            fd, _ = self.s._accept() # type: ignore
        except OSError:
            return
        ss = socket(fileno=fd)
        ss.settimeout(0)
        A = EndPoint(ss, True)
        cc = socket(self.destfamily, SOCK_STREAM, 0)
        cc.settimeout(0)
        try:
            cc.connect(self.destaddress)
        except BlockingIOError:
            pass
        B = EndPoint(cc, False)
        A.peer = B
        B.peer = A
        A.reregister(self.selector)
        B.reregister(self.selector)
        self.last = A, B

class TestEndPoint(object):
    a: socket
    b: socket
    c: socket
    d: socket
    B: EndPoint
    C: EndPoint
    E: Poller

    AF: int = AF_INET

    @classmethod
    def tcppair(self) -> Tuple[socket, socket]:
        acceptor = socket(self.AF, SOCK_STREAM)
        acceptor.bind(('localhost', 0))
        acceptor.listen(1)
        client = socket(self.AF, SOCK_STREAM)
        client.connect(acceptor.getsockname())
        serverfd, _ = acceptor._accept() # type: ignore
        server = socket(fileno=serverfd)
        acceptor.close()
        for s in client, server:
            s.settimeout(0)
            s.setsockopt(IPPROTO_TCP, TCP_NODELAY, 1)
        return client, server

    def setup_method(self) -> None:
        self.b, self.a = self.tcppair()
        self.d, self.c = self.tcppair()
        self.B = EndPoint(self.b, True)
        self.C = EndPoint(self.c, False)
        self.B.peer = self.C
        self.C.peer = self.B
        self.E = Poller()
        self.E.add(self.B)
        self.E.add(self.C)
        self.E.add(SocketReader(self.d))

    def teardown_method(self) -> None:
        for X in self.a, self.b, self.c, self.d:
            X.close()

    def test_simple(self) -> None:
        a, B, C, d = self.a, self.B, self.C, self.d
        C.connected = False
        a.sendall(b'abcd')
        assert B.sinkready
        B.inready()
        assert not B.sinkready
        C.outready()
        assert B.sinkready
        assert d.recv(10) == b'abcd'
        B.inready()
        assert B.sinkready
        with pytest.raises(BlockingIOError):
            d.recv(10)

    def test_overload(self) -> None:
        # Pump data into the socket until it jams completely.
        # Then check we readout exactly the correct data.
        self.a.setsockopt(SOL_SOCKET, SO_SNDBUF, 12345 * 4)
        self.b.setsockopt(SOL_SOCKET, SO_RCVBUF, 12345 * 3)
        self.c.setsockopt(SOL_SOCKET, SO_SNDBUF, 12345 * 2)
        self.d.setsockopt(SOL_SOCKET, SO_RCVBUF, 12345)

        sent = 0
        prime = 251
        data = memoryview(bytes(range(prime)) * (BUFFER_SIZE // prime + 2))
        one_k = data[0:BUFFER_SIZE]
        while True:
            self.E.dispatch(0)
            try:
                sent += self.a.send(one_k[sent % prime:]) # type: ignore
            except BlockingIOError:
                break
        assert sent > len(one_k)
        self.a.shutdown(SHUT_WR)
        # Now drive the I/O and read back, checking the data.
        recv = 0
        while True:
            assert self.E.dispatch(1)
            try:
                got = self.d.recv(BUFFER_SIZE)
            except BlockingIOError:
                continue

            if not got:
                break

            expect = data[recv % prime : recv % prime + len(got)]
            assert got == expect
            recv += len(got)
        assert sent == recv
        assert ~self.B.regevents & EVENT_READ

    def test_reset_propagation(self) -> None:
        self.a.setsockopt(SOL_SOCKET, SO_LINGER, struct.pack('ii', 1, 0))
        self.a.close()
        # Now drive until d doesn't give EAGAIN.
        while True:
            assert self.E.dispatch(1)
            try:
                got = self.d.recv(10)
                assert False
            except ConnectionResetError:
                break
            except BlockingIOError:
                pass
        assert self.B.regevents == 0
        assert self.C.regevents == 0
        self.E.dispatch(0)

    def test_reset_after_shutdown(self) -> None:
        # Do a shutdown on 'a' and then drive until it comes out 'd'.
        self.a.shutdown(SHUT_WR)
        while True:
            assert self.E.dispatch(1)
            try:
                got = self.d.recv(10)
                break
            except BlockingIOError:
                continue
        assert not got
        self.E.selector.unregister(self.d)
        # Now reset 'a' and check that this comes out 'd'.  It won't
        # spontaneously arrive because we're no longer reading, but eventually
        # a write should get an error.
        self.a.setsockopt(SOL_SOCKET, SO_LINGER, struct.pack('ii', 1, 0))
        self.a.close()
        self.d.send(b'1')
        while self.C.sinkready:
            assert self.E.dispatch(1)
        with pytest.raises(BrokenPipeError):
            self.d.send(b'1')

class TestEndPoint6(TestEndPoint):
    AF = AF_INET6

class MemoReader(SocketReader):
    __slots__ = 'memo'
    memo : bytearray
    def __init__(self, s: socket):
        super(MemoReader, self).__init__(s)
        self.memo = bytearray()
    def inready(self) -> None:
        try:
            b = self.s.recv(BUFFER_SIZE)
        except BlockingIOError:
            return
        if b:
            self.memo += b
        else:
            self.wanted = False

class TestBasicFlow(object):
    AF = AF_INET

    @classmethod
    def listener(self) -> socket:
        s = socket(self.AF, SOCK_STREAM, 0)
        s.bind(('localhost', 0))
        s.listen(1)
        s.settimeout(0)
        return s

    def setup_method(self) -> None:
        self.P = Poller()
        self.DL = SocketReader(self.listener())
        self.BA = FlowAcceptor(self.listener(),
                               self.DL.s.family, self.DL.s.getsockname(),
                               self.P.selector)
        self.BA.s.settimeout(0)
        self.P.add(self.BA)
        self.P.add(self.DL)
        self.A = MemoReader(socket(self.AF, SOCK_STREAM, 0))
        self.A.s.settimeout(0)
        try:
            self.A.s.connect(self.BA.s.getsockname())
        except BlockingIOError:
            pass

    def teardown_method(self) -> None:
        self.A.s.close()
        self.BA.s.close()
        self.DL.s.close()
        self.P.selector.close()

    def test_connect(self) -> None:
        while True:
            assert self.P.dispatch(1)
            try:
                fd, _ = self.DL.s._accept() # type: ignore
                break
            except BlockingIOError:
                continue
        D = MemoReader(socket(fileno=fd))
        self.A.s.send(b'testing123')
        self.A.s.shutdown(SHUT_WR)
        D.s.send(b'otherdirection')
        D.s.shutdown(SHUT_WR)
        self.A.reregister(self.P.selector)
        D.reregister(self.P.selector)
        while D.isreg or self.A.isreg:
            assert self.P.dispatch(1)
        assert not D.wanted
        assert not self.A.wanted
        assert self.A.memo == b'otherdirection'
        assert D.memo == b'testing123'
        D.kill()

class TestBasicFlow6(TestBasicFlow):
    AF = AF_INET6
