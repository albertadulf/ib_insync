from enum import IntEnum
import json
from typing import Dict, List

from ib_insync import Object

# Core Protocols
kJoinRequestUri = 1
kJoinResponseUri = 2
kPingUri = 3
kPongUri = 4
# Console Protocols
kConsoleCommandRequestUri = 5
kConsoleCommandResponseUri = 6
# Data channel protocols
kPublishedMarketDataUri = 7

kClientStatusUri = 8
kRequireGroupInfoUri = 9

# WorkerTypes
kWorkerTypeConsole = 0
kWorkerTypeSubscriber = 1
kWorkerTypeIbSimulator = 2
kWorkerTypeIbTrader = 3

# Redis channels
kCmdChannel = 'ib:cmd'
kCmdAllocatorChannel = 'ib:aloc'
kDataChannel = 'ib:data'


def worker_type_str(worker_type: int) -> str:
    if worker_type == kWorkerTypeConsole:
        return 'console worker'
    elif worker_type == kWorkerTypeSubscriber:
        return 'market subscriber'
    elif worker_type == kWorkerTypeIbSimulator:
        return 'simulator service'
    elif worker_type == kWorkerTypeIbTrader:
        return 'trader service'
    return 'unknown worker'


class ResponseStatus(IntEnum):
    kSuccess = 0
    kAlready = 1
    kFailed = 2


class ProtocolBase(Object):
    def pack(self):
        obj = {}
        for k in self.__slots__:
            v = getattr(self, k)
            if v:
                obj[k] = v
        return json.dumps(obj)

    def unpack(self, obj: Dict):
        for k, default in self.__class__.defaults.items():
            if k in obj:
                v = obj[k]
            else:
                v = default
            typ = type(default)
            if typ is str:
                setattr(self, k, str(v) if v else default)
            elif typ is int:
                setattr(self, k, int(v) if v else default)
            elif typ is float:
                setattr(self, k, float(v) if v else default)
            elif typ is bool:
                setattr(self, k, bool(v) if v else default)
            elif typ is list:
                setattr(self, k, v if len(v) else default)
        return self


class Transporter(object):
    @staticmethod
    def serialize(msg: str) -> bytes:
        result = len(msg).to_bytes(4, 'big') + msg.encode()
        return result

    def __init__(self) -> None:
        self._bytes: bytes = b''

    def deserialize(self, data: bytes) -> List[str]:
        self._bytes += self._bytes + data
        results: List[str] = []
        while True:
            byte_length = len(self._bytes)
            if byte_length < 4:
                return results
            data_length = int.from_bytes(self._bytes[:4], 'big', signed=False)
            if byte_length < data_length + 4:
                return results
            data = self._bytes[4:4 + data_length].decode()
            results.append(data)
            self._bytes = self._bytes[4 + data_length:]
        return results


class WorkerJoinRequest(ProtocolBase):
    defaults = dict(
        uri=kJoinRequestUri,
        sid='',
        wtype=kWorkerTypeConsole)
    __slots__ = defaults.keys()


class WorkerJoinResponse(ProtocolBase):
    defaults = dict(
        uri=kJoinResponseUri,
        sid='',
        channel='',
        status=0)
    __slots__ = defaults.keys()


class Ping(ProtocolBase):
    defaults = dict(
        uri=kPingUri,
        sid='',
        ts=0)
    __slots__ = defaults.keys()


class Pong(ProtocolBase):
    defaults = dict(
        uri=kPongUri,
        client_ts=0)
    __slots__ = defaults.keys()


class ConsoleCommandRequest(ProtocolBase):
    defaults = dict(
        uri=kConsoleCommandRequestUri,
        sid='',
        peer_sid='',
        cmd='')
    __slots__ = defaults.keys()


class ConsoleCommandResponse(ProtocolBase):
    defaults = dict(
        uri=kConsoleCommandResponseUri,
        status=0,
        sid='',
        peer_sid='',
        msg='')
    __slots__ = defaults.keys()


class PublishedMarketData(ProtocolBase):
    defaults = dict(
        uri=kPublishedMarketDataUri,
        alias='',
        ts=0.0,
        bid_prices=[],
        bid_sizes=[],
        ask_prices=[],
        ask_sizes=[])
    __slots__ = defaults.keys()


class NotifyClientStatusChanged(ProtocolBase):
    defaults = dict(
        uri=kClientStatusUri,
        sid='',
        wtype=0,
        online=False)
    __slots__ = defaults.keys()


class RequireGroupInfo(ProtocolBase):
    defaults = dict(
        uri=kRequireGroupInfoUri,
        sid='')
    __slots__ = defaults.keys()


def test():
    base = ProtocolBase()
    typ = type(base)
    if typ is ProtocolBase:
        print('test')
    req = WorkerJoinRequest()
    print(req.pack())


if __name__ == '__main__':
    test()
