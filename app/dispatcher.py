import asyncio
from typing import Any, Callable, Dict, List
import json

from app.server.protocols import ProtocolBase, WorkerJoinRequest


class Dispatcher(object):
    def __init__(self) -> None:
        self._handlers: Dict[int, List[Any]] = {}

    def add_dispatcher(
            self, protocol_type: ProtocolBase, handler: Callable) -> None:
        protocol = protocol_type()
        uri = protocol.uri
        if uri not in self._handlers:
            self._handlers[uri] = [protocol_type, handler]

    async def on_message(self, message: str) -> None:
        obj = json.loads(message)
        if 'uri' not in obj:
            return
        uri = obj['uri']
        if uri in self._handlers:
            packet = self._handlers[uri][0]()
            packet.unpack(obj)
            await self._handlers[uri][1](packet)


async def test():
    async def request_handler(req: WorkerJoinRequest):
        print('sid {}, uri {}'.format(req.sid, req.uri))

    dispatcher = Dispatcher()
    dispatcher.add_dispatcher(
        WorkerJoinRequest.uri, WorkerJoinRequest, request_handler)
    req = WorkerJoinRequest()
    req.sid = 'test'
    message = req.pack()
    print(message)
    res = WorkerJoinRequest().unpack(message)
    print(res.sid)
    print(res.uri)
    print(res.wtype)
    # await dispatcher.on_message(req.pack())


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(test())
