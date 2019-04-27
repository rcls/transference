#!/usr/bin/python3

import argparse
import typing
if typing.TYPE_CHECKING:
    from socket import *
else:
    from _socket import *
from selectors import BaseSelector, DefaultSelector, EVENT_READ, EVENT_WRITE
import struct
from typing import Any, List, Optional, Tuple, Type, TypeVar, Union
import yaml

BUFFER_SIZE = 1024

class Dispatchable(socket):
    def __init__(self, family=-1, type=-1, proto=-1, fileno=None):
        super(Dispatchable, self).__init__(family, type, proto, fileno)
        if fileno is None and self.family == AF_INET6:
            self.setsockopt(IPPROTO_IPV6, IPV6_V6ONLY, 1)
        self.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
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
    wanted : bool
    isreg : bool
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
    __slots__ = 'destfamily', 'destaddress', 'selector'
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

def gai(host: Optional[str], port: Union[str,int,None],
        family: int = 0) -> List[Tuple[int,int,int,str,Any]]:
    # We always use AI_PASSIVE, people who want localhost can say so.
    return getaddrinfo(host, port, family, SOCK_STREAM, IPPROTO_TCP,
                       AI_PASSIVE | AI_ADDRCONFIG)

def load_config(selector: BaseSelector, y):
    for flow_name, flow_config in y.items():
        for K, V in flow_config.items():
            # Any key that is not known is assumed to be a source: dest pair.
            host: Optional[str]
            if isinstance(K, str) and ':' in K:
                host, port = K.rsplit(':', 1)
            else:
                host, port = None, K
            if ':' in V:
                dhost, dport = V.rsplit(':', 1)
            else:
                dhost, dport = V, port
            daf, _, _, _, daddr = gai(dhost, dport)[0]
            for af, kind, proto, _, addr in gai(host, port):
                f = FlowAcceptor(daf, daddr, selector, af, kind, proto)
                f.bind(addr)
                f.listen()
                f.reregister(selector)

def main() -> None:
    parser = argparse.ArgumentParser(description='TCP relay')
    parser.add_argument('config_file', type=argparse.FileType('r'))
    args = parser.parse_args()
    selector = DefaultSelector()
    y = yaml.safe_load(args.config_file)
    load_config(selector, y)
    while True:
        Dispatchable.dispatch(selector, 86400)

X = TypeVar('X', bound=socket)
Y = TypeVar('Y', bound=socket)

class TestBase(object):
    AF:int = AF_INET

    aa : Type[SocketReader] = SocketReader
    dd : Type[SocketReader] = SocketReader
    E: BaseSelector
    a: SocketReader
    b: EndPoint
    c: EndPoint
    d: SocketReader

    def setup_method(self) -> None:
        acceptor = socket(self.AF, SOCK_STREAM)
        acceptor.bind(('localhost', 0))
        acceptor.listen(1)
        ahost, aport = acceptor.getsockname()[:2]
        self.E = DefaultSelector()
        load_config(self.E, {'flow': { f'{ahost}:0': f'{ahost}:{aport}' }})
        # There should be a single item in the map...
        assert len(self.E.get_map()) == 1
        _, _, _, listener = next(iter(self.E.get_map().values()))
        self.a = self.aa(self.AF, SOCK_STREAM)
        self.a.connect(listener.getsockname())
        Dispatchable.dispatch(self.E, 1)
        for _, _, _, s in self.E.get_map().values():
            if s == listener:
                pass
            elif s.connected:
                self.b = s
            else:
                self.c = s
        assert hasattr(self, 'b')
        assert hasattr(self, 'c')
        serverfd, _ = acceptor._accept() # type: ignore
        self.d = self.dd(fileno=serverfd)
        for X in self.a, self.b, self.c, self.d:
            X.settimeout(0)
            X.setsockopt(IPPROTO_TCP, TCP_NODELAY, 1)
            X.reregister(self.E)

    def teardown_method(self) -> None:
        for _, _, _, X in self.E.get_map().values():
            X.close()
        for X in self.a, self.d, self.E:
            X.close()

class TestEndPoint(TestBase):
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
        import pytest                   # type: ignore
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
        import pytest
        with pytest.raises(ConnectionError):
            while True:
                assert Dispatchable.dispatch(self.E, 1)
                try:
                    self.d.recv(10)
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

class TestBasicFlow(TestBase):
    AF = AF_INET
    aa = MemoReader
    dd = MemoReader
    a : MemoReader
    d : MemoReader

    def test_connect(self) -> None:
        self.a.send(b'testing123')
        self.a.shutdown(SHUT_WR)
        self.d.send(b'otherdirection')
        self.d.shutdown(SHUT_WR)
        while self.d.isreg or self.a.isreg:
            assert Dispatchable.dispatch(self.E, 1)
        assert not self.d.wanted
        assert not self.a.wanted
        assert self.a.memo == b'otherdirection'
        assert self.d.memo == b'testing123'

class TestBasicFlow6(TestBasicFlow):
    AF = AF_INET6

if __name__ == "__main__":
    main()
