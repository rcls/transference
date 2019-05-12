#!/usr/bin/python3

import argparse
import typing
if typing.TYPE_CHECKING:
    import socket as socketmodule
    from socket import *
else:
    import _socket as socketmodule
    from _socket import *
import os
from selectors import BaseSelector, DefaultSelector, EVENT_READ, EVENT_WRITE
import struct
from typing import \
    Any, Callable, Dict, List, Optional, Tuple, Type, TypeVar, Union
import yaml

BUFFER_SIZE = 1024

if not 'IPPROTO_IPV6' in globals():
    IPPROTO_IPV6 = 41

class Dispatchable(socket):
    __slots__ = 'selector', 'regevents'
    selector : BaseSelector
    regevents : int
    def __init__(self, family=-1, type=-1, proto=-1, fileno=None):
        super(Dispatchable, self).__init__(family, type, proto, fileno)
        if fileno is None and self.family == AF_INET6:
            self.setsockopt(IPPROTO_IPV6, IPV6_V6ONLY, 1)
        self.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.settimeout(0)
        self.regevents = 0
    def inready(self) -> None:
        pass
    def outready(self) -> None:
        pass
    def reregister(self) -> None:
        raise NotImplementedError
    def do_register(self, wanted: int):
        if wanted != self.regevents:
            if self.regevents == 0:
                self.selector.register(self, wanted, self)
            elif wanted == 0:
                self.selector.unregister(self)
            else:
                self.selector.modify(self, wanted, self)
            self.regevents = wanted
    def so_linger_0(self) -> None:
        try:
            self.setsockopt(SOL_SOCKET, SO_LINGER, struct.pack('ii', 1, 0))
        except OSError:
            pass

    @classmethod
    def dispatch(self, sel: BaseSelector, timeout: float) -> bool:
        # Windows bug workaround - select() on an empty set does not work.
        if not sel.get_map():
            import time
            time.sleep(timeout)
            return False
        L = sel.select(timeout)

        for key, ev in L:
            if ev & EVENT_READ:
                key.data.inready()
            if ev & EVENT_WRITE:
                key.data.outready()
        for key, ev in L:
            key.data.reregister()
        return bool(L)

class SocketReader(Dispatchable):
    wanted : bool
    __slots__ = 'wanted'
    def __init__(self, *args, **kwargs):
        super(SocketReader, self).__init__(*args, **kwargs)
        self.wanted = True
    def reregister(self) -> None:
        self.do_register(EVENT_READ if self.wanted else 0)
    def kill(self) -> None:
        self.close()
        self.wanted = False

class Sink(object):
    def out(self, data: memoryview) -> bool:
        pass

class Filter(Sink):
    chain: Sink
    __slots__ = 'chain'

FilterCreator = Callable[['EndPoint', Any],Optional[Filter]]

class EndPoint(Dispatchable, Sink):
    sinkready: bool
    connected: bool
    peer     : 'EndPoint'
    sink     : Sink
    buffer   : Optional[memoryview]

    __slots__ = 'sinkready', 'connected', 'peer', 'buffer'
    def __init__(self, *args, **kwargs):
        super(EndPoint, self).__init__(*args, **kwargs)
        self.sinkready = True
        self.connected = True
        self.buffer    = None # Output buffer

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

        self.sinkready = self.sink.out(memoryview(data))

    def setfilters(self, creators : List[Tuple[int,FilterCreator,Any]]):
        self.sink = self.peer
        for _, C, A in reversed(creators):
            f = C(self, A)
            if f is not None:
                f.chain = self.sink
                self.sink = f

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

    def reregister(self) -> None:
        for S in self, self.peer:
            wanted = EVENT_READ if S.sinkready and S.connected else 0
            if S.buffer is not None or not S.connected:
                wanted |= EVENT_WRITE
            S.do_register(wanted)
        if self.regevents == 0 and self.peer.regevents == 0:
            self.close()
            self.peer.close()

    def kill(self) -> None:
        for S in self, self.peer:
            S.sinkready = False
            S.connected = True
            S.buffer = None
            if S.regevents != 0:
                S.selector.unregister(S)
                S.regevents = 0
            S.close()

class InitialFilter(Filter):
    initial: bytes
    current: Optional[bytearray]
    endpoint: EndPoint
    def __init__(self, ep: EndPoint, b):
        if isinstance(b, str):
            b = bytes.fromhex(b)
        self.initial = b
        self.current = bytearray()
        self.endpoint = ep
    def out(self, b : memoryview) -> bool:
        if self.current is None:
            return self.chain.out(b)
        self.current += b               # type: ignore
        if len(self.current) >= len(self.initial):
            if self.current.startswith(self.initial):
                # Success...
                mv = memoryview(self.current)
                self.current = None
                return self.chain.out(mv)
        else:
            if self.initial.startswith(self.current) and b:
                return True
        self.endpoint.so_linger_0()
        self.endpoint.peer.so_linger_0()
        self.endpoint.kill()
        return False

filter_list: List[Tuple[str,FilterCreator]]
filter_list=[]
filter_dict: Dict[str,Tuple[int,FilterCreator]]
filter_dict={}

def filter_socket_option(level: int, name: str) -> FilterCreator:
    option: int = getattr(socketmodule, name)
    return lambda ep, value: ep.setsockopt(level, option, value)

for N in dir(socketmodule):
    if N.startswith('SO_'):
        filter_list.append((N, filter_socket_option(SOL_SOCKET, N)))
    if N.startswith('IP_'):
        filter_list.append((N, filter_socket_option(IPPROTO_IP, N)))
    if N.startswith('IPV6_'):
        filter_list.append((N, filter_socket_option(IPPROTO_IPV6, N)))
    if N.startswith('TCP_'):
        filter_list.append((N, filter_socket_option(IPPROTO_TCP, N)))

filter_list.append(('initial', InitialFilter))

for n, (N, F) in enumerate(filter_list):
    filter_dict[N] = n, F

def filter_lookup(key) -> Tuple[str, int, Optional[FilterCreator]]:
    if not isinstance(key, str):
        return '', 0, None
    for prefix in '+-', '+', '-', '':
        if key.startswith(prefix) and key[len(prefix):] in filter_dict:
            n, F = filter_dict[key[len(prefix):]]
            return prefix or '+', n, F
    return '', 0, None

class FlowAcceptor(SocketReader):
    __slots__ = 'destfamily', 'destaddress', 'forwards', 'backwards'
    family: int
    forwards: List[Tuple[int, FilterCreator, Any]]
    backwards: List[Tuple[int, FilterCreator, Any]]
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
        A.peer = B
        B.peer = A
        A.setfilters(self.forwards)
        B.setfilters(self.backwards)
        try:
            B.connect(self.destaddress)
        except BlockingIOError:
            pass
        B.connected = False
        A.selector = self.selector
        B.selector = self.selector
        A.reregister()
        #B.reregister()

def gai(host: Optional[str], port: Union[str,int,None],
        family: int = 0) -> List[Tuple[int,int,int,str,Any]]:
    # We always use AI_PASSIVE, people who want localhost can say so.
    return getaddrinfo(host, port, family, SOCK_STREAM, IPPROTO_TCP,
                       AI_PASSIVE | AI_ADDRCONFIG)

def load_config(selector: BaseSelector, y):
    for flow_name, flow_config in y.items():
        forwards = []
        backwards = []
        for K, V in flow_config.items():
            sign, prio, creator = filter_lookup(K)
            if creator is not None:
                if '+' in sign:
                    forwards.append((prio, creator, V))
                if '-' in sign:
                    backwards.append((prio, creator, V))
                continue
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
                f.selector = selector
                f.reregister()
                f.forwards = forwards
                f.backwards = backwards
        forwards.sort()
        backwards.sort()

def main() -> None:
    parser = argparse.ArgumentParser(description='TCP relay')
    parser.add_argument('config_file', type=argparse.FileType('r'))
    args = parser.parse_args()
    selector = DefaultSelector()
    y = yaml.load(args.config_file) # type: ignore
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
    acceptor: socket
    listener: FlowAcceptor

    @classmethod
    def config(self, ahost: str, aport: int):
        return {'flow': { ahost + ':0' : ahost + ':' + str(aport) }}

    def setup_method(self) -> None:
        self.acceptor = socket(self.AF, SOCK_STREAM)
        self.acceptor.bind(('localhost', 0))
        self.acceptor.listen(1)
        ahost, aport = self.acceptor.getsockname()[:2]
        self.E = DefaultSelector()
        load_config(self.E, self.config(ahost, aport))
        # There should be a single item in the map...
        (_, _, _, self.listener), = self.E.get_map().values()
        self.a = self.aa(self.AF, SOCK_STREAM)
        self.a.selector = self.E
        try:
            self.a.connect(self.listener.getsockname())
        except BlockingIOError:
            pass
        Dispatchable.dispatch(self.E, 1)
        for _, _, _, s in self.E.get_map().values():
            if s == self.listener:
                pass
            elif s.connected:
                self.b = s
            else:
                self.c = s
        assert hasattr(self, 'b')
        assert hasattr(self, 'c')
        serverfd, _ = self.acceptor._accept() # type: ignore
        self.d = self.dd(fileno=serverfd)
        self.d.selector = self.E
        for X in self.a, self.b, self.c, self.d:
            X.setsockopt(IPPROTO_TCP, TCP_NODELAY, 1)
        self.a.reregister()
        self.d.reregister()

    def teardown_method(self) -> None:
        for _, _, _, X in self.E.get_map().values():
            X.close()
        for X in self.a, self.d, self.E, self.acceptor, self.listener:
            X.close()

    def recv_wait(self, s: socket, n: int) -> bytes:
        for _ in range(100):
            assert Dispatchable.dispatch(self.E, 1)
            try:
                return s.recv(n)
            except BlockingIOError:
                pass
        assert False

    def raises(self, C):
        import pytest                   # type: ignore
        return pytest.raises(C)

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
        with self.raises(BlockingIOError):
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
            got = self.recv_wait(self.d, BUFFER_SIZE)
            if not got:
                break

            expect = data[recv % prime : recv % prime + len(got)]
            assert got == expect
            recv += len(got)
        assert sent == recv
        assert ~self.b.regevents & EVENT_READ

    def test_reset_propagation(self) -> None:
        self.E.unregister(self.a)
        self.a.so_linger_0()
        self.a.close()
        # Now drive until d doesn't give EAGAIN.
        with self.raises(ConnectionError):
            self.recv_wait(self.d, 10)
        assert self.b.regevents == 0
        assert self.c.regevents == 0

    def test_reset_after_shutdown(self) -> None:
        # Do a shutdown on 'a' and then drive until it comes out 'd'.
        self.a.shutdown(SHUT_WR)
        got = self.recv_wait(self.d, 10)
        assert not got
        assert self.c.sinkready
        self.E.unregister(self.d)
        # Now reset 'a' and check that this comes out 'd'.  It won't
        # spontaneously arrive because we're no longer reading, but eventually
        # a write should get an error.
        self.E.unregister(self.a)
        self.a.so_linger_0()
        self.a.close()
        with self.raises(ConnectionError):
            for _ in range(10):
                self.d.send(b'1')
                Dispatchable.dispatch(self.E, 0.1)

    def test_non_blocking(self) -> None:
        for X in self.a, self.b, self.c, self.d:
            with self.raises(BlockingIOError):
                X.recv(10)
        with self.raises(BlockingIOError):
            self.listener._accept()     # type: ignore

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
        while self.d.regevents or self.a.regevents:
            assert Dispatchable.dispatch(self.E, 1)
        assert not self.d.wanted
        assert not self.a.wanted
        assert self.a.memo == b'otherdirection'
        assert self.d.memo == b'testing123'

class TestBasicFlow6(TestBasicFlow):
    AF = AF_INET6

class TestInitial(TestBase):
    @classmethod
    def config(self, ahost: str, aport: int):
        return { 'flow': {
            ahost + ':0': ahost + ':' +str(aport),
            'initial': b'arbitrary'.hex() }}

    def test_baddata(self):
        self.a.send(b'arb1tra')
        with self.raises(ConnectionError):
            self.recv_wait(self.d, 10)
        with self.raises(ConnectionError):
            self.recv_wait(self.a, 10)

    def test_earlyclose(self):
        self.a.send(b'arbitra')
        self.a.shutdown(SHUT_WR)
        with self.raises(ConnectionError):
            self.recv_wait(self.d, 10)

    def test_ok(self):
        self.a.send(b'arb')
        self.a.send(b'i')
        self.a.send(b'trary123')
        got = self.recv_wait(self.d, 15)
        assert got == b'arbitrary123'
        self.a.send(b'moremoremore')
        got = self.recv_wait(self.d, 15)
        assert got == b'moremoremore'

    def test_back(self):
        self.d.send(b'goingbackwards')
        got = self.recv_wait(self.a, 15)
        assert got == b'goingbackwards'

class TestReaccept(TestBase):
    def fin_shutter(self):
        self.a.shutdown(SHUT_WR)
        self.d.shutdown(SHUT_WR)
    def rst_shutter(self):
        self.a.so_linger_0()
        self.a.close()

    def run_reaccept(self, shutter):
        filenos = set((self.b.fileno(), self.c.fileno()))
        objects = set(X.data for X in self.E.get_map().values())
        shutter()
        a2 = socket(self.AF, SOCK_STREAM)
        a2.connect(self.listener.getsockname())
        # Now drive everything manually....
        import time
        time.sleep(0.1)                 # I have no pride.
        self.c.outready()
        self.b.inready()
        self.c.inready()
        # This should get b, c idle...
        self.b.reregister()
        self.c.reregister()
        assert self.b.regevents == 0
        assert self.c.regevents == 0

        # Now drive the accept...
        self.listener.inready()
        # Check that we've tested what we wanted to: this should get a fileno we
        # just closed.
        new_objs = set(X.data for X in self.E.get_map().values()) - objects
        assert len(new_objs) == 2
        new_filenos = set(X.fileno() for X in new_objs)
        assert new_filenos == filenos

    def test_fin_reaccept(self):
        self.run_reaccept(self.fin_shutter)

    def test_rst_reaccept(self):
        self.run_reaccept(self.rst_shutter)

if __name__ == "__main__":
    main()
