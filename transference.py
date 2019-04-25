#!/usr/bin/python3

from socket import *
from selectors import BaseSelector, DefaultSelector, EVENT_READ, EVENT_WRITE
import struct
from typing import Optional, Tuple, Type, TypeVar

BUFFER_SIZE = 1024

class Dispatchable(socket):
    def inready(self) -> None:
        pass
    def outready(self) -> None:
        pass
    def reregister(self, sel: BaseSelector) -> None:
        raise NotImplementedError
    def kill(self) -> None:
        pass

    @classmethod
    def dispatch(self, sel: BaseSelector, timeout: float) -> bool:
        # Windows bug workaround - select() on an empty set does not work.
        if not sel.get_map():
            import time
            time.sleep(timeout)
            return False
        L = sel.select(timeout)
        for (obj, fd, events, data), ev in L:
            if ev & EVENT_READ:
                data.inready()
            if ev & EVENT_WRITE:
                data.outready()
        for (obj, fd, events, data), ev in L:
            data.reregister(sel)
        return bool(L)

class SocketReader(Dispatchable):
    __slots__ = 'wanted', 'isreg'
    def __init__(self, *args, **kwargs):
        super(SocketReader, self).__init__(*args, **kwargs)
        self.wanted = True
        self.isreg = False
    def reregister(self, sel: BaseSelector) -> None:
        if self.wanted and not self.isreg:
            sel.register(self, EVENT_READ, self)
        elif self.isreg and not self.wanted:
            sel.unregister(self)
        self.isreg = self.wanted
    def kill(self) -> None:
        self.close()
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
    def __init__(self, *args, **kwargs):
        super(EndPoint, self).__init__(*args, **kwargs)
        self.sinkready = True
        self.connected = True
        self.buffer    = None # Output buffer
        self.regevents = 0

    def inready(self) -> None:
        if not self.sinkready:
            return

        try:
            data = self.recv(BUFFER_SIZE)
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
            n = self.send(data)         # type: ignore
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
                self.shutdown(SHUT_WR)
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
                sel.register(S, wanted, S)
            elif wanted == 0:
                sel.unregister(S)
            else:
                sel.modify(S, wanted, S)
            S.regevents = wanted
        if self.regevents == 0 and self.peer.regevents == 0:
            self.kill()

    def so_linger_0(self) -> None:
        try:
            self.setsockopt(SOL_SOCKET, SO_LINGER, struct.pack('ii', 1, 0))
        except OSError:
            pass

    def kill(self) -> None:
        for S in self, self.peer:
            S.close()
            S.sinkready = False
            S.connected = True
            S.buffer = None

class FlowAcceptor(SocketReader):
    __slots__ = 'destfamily', 'destaddress', 'selector', 'last'
    family: int
    selector: BaseSelector
    def __init__(self, destfamily: int, destaddress, sel: BaseSelector,
                 *args, **kwargs):
        super(FlowAcceptor, self).__init__(*args, **kwargs)
        self.destfamily  = destfamily
        self.destaddress = destaddress
        self.selector    = sel
    def inready(self) -> None:
        try:
            fd, _ = self._accept()      # type: ignore
        except OSError:
            return
        A = EndPoint(fileno=fd)
        B = EndPoint(self.destfamily, SOCK_STREAM, 0)
        A.settimeout(0)
        B.settimeout(0)
        try:
            B.connect(self.destaddress)
        except BlockingIOError:
            pass
        B.connected = False
        A.peer = B
        B.peer = A
        A.reregister(self.selector)
        B.reregister(self.selector)
        self.last = A, B

X = TypeVar('X', bound=socket)
Y = TypeVar('Y', bound=socket)

class TestEndPoint(object):
    a: SocketReader
    b: EndPoint
    c: EndPoint
    d: SocketReader
    E: BaseSelector

    AF: int = AF_INET

    @classmethod
    def tcppair(self, C: Type[X], S: Type[Y]) -> Tuple[X, Y]:
        acceptor = socket(self.AF, SOCK_STREAM)
        acceptor.bind(('localhost', 0))
        acceptor.listen(1)
        client = C(self.AF, SOCK_STREAM)
        client.connect(acceptor.getsockname())
        serverfd, _ = acceptor._accept() # type: ignore
        server = S(fileno=serverfd)
        acceptor.close()
        client.settimeout(0)
        client.setsockopt(IPPROTO_TCP, TCP_NODELAY, 1)
        server.settimeout(0)
        server.setsockopt(IPPROTO_TCP, TCP_NODELAY, 1)
        return client, server

    def setup_method(self) -> None:
        self.a, self.b = self.tcppair(SocketReader, EndPoint)
        self.c, self.d = self.tcppair(EndPoint, SocketReader)
        self.b.connected = True
        self.c.connected = False
        self.b.peer = self.c
        self.c.peer = self.b
        self.E = DefaultSelector()
        self.b.reregister(self.E)
        self.c.reregister(self.E)
        self.d.reregister(self.E)

    def teardown_method(self) -> None:
        for X in self.a, self.b, self.c, self.d:
            X.close()

    def test_simple(self) -> None:
        a, b, c, d = self.a, self.b, self.c, self.d
        c.connected = False
        a.sendall(b'abcd')
        assert b.sinkready
        b.inready()
        assert not b.sinkready
        c.outready()
        assert b.sinkready
        assert d.recv(10) == b'abcd'
        b.inready()
        assert b.sinkready
        import pytest
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
            Dispatchable.dispatch(self.E, 0)
            try:
                sent += self.a.send(one_k[sent % prime:]) # type: ignore
            except BlockingIOError:
                break
        assert sent > len(one_k)
        self.a.shutdown(SHUT_WR)
        # Now drive the I/O and read back, checking the data.
        recv = 0
        while True:
            assert Dispatchable.dispatch(self.E, 1)
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
        assert ~self.b.regevents & EVENT_READ

    def test_reset_propagation(self) -> None:
        self.a.setsockopt(SOL_SOCKET, SO_LINGER, struct.pack('ii', 1, 0))
        self.a.close()
        # Now drive until d doesn't give EAGAIN.
        while True:
            assert Dispatchable.dispatch(self.E, 1)
            try:
                got = self.d.recv(10)
                assert False
            except ConnectionResetError:
                break
            except BlockingIOError:
                pass
        assert self.b.regevents == 0
        assert self.c.regevents == 0

    def test_reset_after_shutdown(self) -> None:
        # Do a shutdown on 'a' and then drive until it comes out 'd'.
        self.a.shutdown(SHUT_WR)
        while True:
            assert Dispatchable.dispatch(self.E, 1)
            try:
                got = self.d.recv(10)
                break
            except BlockingIOError:
                continue
        assert not got
        assert self.c.sinkready
        self.E.unregister(self.d)
        # Now reset 'a' and check that this comes out 'd'.  It won't
        # spontaneously arrive because we're no longer reading, but eventually
        # a write should get an error.
        self.a.setsockopt(SOL_SOCKET, SO_LINGER, struct.pack('ii', 1, 0))
        self.a.close()
        import pytest
        with pytest.raises(ConnectionError):
            for _ in range(10):
                self.d.send(b'1')
                Dispatchable.dispatch(self.E, 0.1)

class TestEndPoint6(TestEndPoint):
    AF = AF_INET6

class MemoReader(SocketReader):
    __slots__ = 'memo'
    memo : bytearray
    def __init__(self, *args, **kwargs):
        super(MemoReader, self).__init__(*args, **kwargs)
        self.memo = bytearray()
    def inready(self) -> None:
        try:
            b = self.recv(BUFFER_SIZE)
        except BlockingIOError:
            return
        if b:
            self.memo += b
        else:
            self.wanted = False

class TestBasicFlow(object):
    AF = AF_INET
    P: BaseSelector
    A: MemoReader
    BA: FlowAcceptor
    DL: SocketReader

    @classmethod
    def listen(self, s: socket) -> None:
        s.bind(('localhost', 0))
        s.listen(1)
        s.settimeout(0)
        return

    def setup_method(self) -> None:
        self.P = DefaultSelector()
        self.DL = SocketReader(self.AF)
        self.listen(self.DL)
        self.BA = FlowAcceptor(self.DL.family, self.DL.getsockname(),
                               self.P, self.AF)
        self.listen(self.BA)
        self.BA.reregister(self.P)
        self.DL.reregister(self.P)
        self.A = MemoReader(self.AF)
        try:
            self.A.connect(self.BA.getsockname())
        except BlockingIOError:
            pass
        self.A.settimeout(0)

    def teardown_method(self) -> None:
        self.A.close()
        self.BA.close()
        self.DL.close()
        self.P.close()

    def test_connect(self) -> None:
        while True:
            assert Dispatchable.dispatch(self.P, 1)
            try:
                fd, _ = self.DL._accept() # type: ignore
                break
            except BlockingIOError:
                continue
        D = MemoReader(fileno=fd)
        self.A.send(b'testing123')
        self.A.shutdown(SHUT_WR)
        D.send(b'otherdirection')
        D.shutdown(SHUT_WR)
        self.A.reregister(self.P)
        D.reregister(self.P)
        while D.isreg or self.A.isreg:
            assert Dispatchable.dispatch(self.P, 1)
        assert not D.wanted
        assert not self.A.wanted
        assert self.A.memo == b'otherdirection'
        assert D.memo == b'testing123'
        D.kill()

class TestBasicFlow6(TestBasicFlow):
    AF = AF_INET6
