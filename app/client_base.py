import asyncio
from typing import Any, Callable

from app.dispatcher import Dispatcher
from app.redis.redis_client import RedisHandler
from app.server.protocols import (
    kCmdChannel,
    kCmdAllocatorChannel,
    ResponseStatus,
    ProtocolBase,
    Transporter,
    WorkerJoinRequest,
    WorkerJoinResponse,
    Ping,
    Pong,
)
from app.utils.common_util import unique_id, tick_ms
from app.utils.log import Log


class ClientBase(object):
    def __init__(self, logger: Any, worker_type: int,
                 cmd_redis_ip: str = 'localhost',
                 cmd_redis_port: int = 6379) -> None:
        self._logger = logger
        self._worker_type: int = worker_type
        self._cmd_redis_ip: str = cmd_redis_ip
        self._cmd_redis_port: int = cmd_redis_port
        self._dispatcher: Dispatcher = Dispatcher()
        self._last_active_ts: int = 0
        self._rtt: int = -1
        self._sid: str = None
        self._redis: RedisHandler = None
        self._join_task: asyncio.Task = None
        self._keep_alive_task: asyncio.Task = None
        self._cmd_channel: str = None
        self._transporter = Transporter()
        self._queue: asyncio.Queue = asyncio.Queue()
        self._handle_task = asyncio.create_task(self._handle_process())
        self.add_dispatcher(WorkerJoinResponse, self._on_join_response)
        self.add_dispatcher(Pong, self._on_pong)

    async def _handle_process(self) -> None:
        while True:
            message = await self._queue.get()
            await self._dispatcher.on_message(message)

    async def _on_message(self, message: bytes) -> None:
        self._last_active_ts = tick_ms()
        messages = self._transporter.deserialize(message)
        for data in messages:
            self._queue.put_nowait(data)

    async def _on_join_timeout(self) -> None:
        await asyncio.sleep(2)
        self._logger.error('join timeout')
        await self.join_channel()

    async def _keep_alive(self) -> None:
        while True:
            await asyncio.sleep(2)
            now = tick_ms()
            interval = now - self._last_active_ts
            if interval > 4000:
                self._logger.error(
                    'failed to connect server with %d ms', interval)
                await self._redis.unsubscribe(self._cmd_channel)
                await self.join_channel()
                return
            ping = Ping()
            ping.sid = self.sid()
            ping.ts = tick_ms()
            await self.send_packet(ping)

    async def _on_join_response(self, response: WorkerJoinResponse) -> None:
        if response.sid != self.sid():
            return
        if response.channel == '':
            self._logger.warning('join response with empty channel')
            return
        self._logger.info('join response, sid: %s, channel: %s, status: %d',
                          response.sid, response.channel, response.status)
        if response.status == ResponseStatus.kSuccess \
           or response.status == ResponseStatus.kAlready:
            self._cmd_channel = response.channel
            self._join_task.cancel()
            self._join_task = None
            await self._redis.unsubscribe(kCmdAllocatorChannel)
            await self._redis.subscribe(response.channel, self._on_message)
            self._keep_alive_task = asyncio.create_task(self._keep_alive())
            await self.on_client_ready()

    async def _on_pong(self, pong: Pong) -> None:
        self._rtt = tick_ms() - pong.client_ts

    async def send_packet(self, packet: ProtocolBase) -> None:
        data = Transporter.serialize(packet.pack())
        await self._redis.publish(kCmdChannel, data)

    async def on_client_ready(self) -> None:
        # need to implement by derived client
        print('on client ready')
        pass

    async def join_channel(self) -> None:
        self._join_task = asyncio.create_task(self._on_join_timeout())
        await self._redis.subscribe(kCmdAllocatorChannel, self._on_message)
        request = WorkerJoinRequest()
        request.sid = self.sid()
        request.wtype = self._worker_type
        self._logger.info('start join channel sid: %s, type: %d',
                          self.sid(), self._worker_type)
        await self.send_packet(request)

    async def initialize(self) -> None:
        self._redis = await RedisHandler.create(
            ip=self._cmd_redis_ip, port=self._cmd_redis_port)
        self._sid = unique_id()
        await self.join_channel()

    def sid(self) -> str:
        return self._sid

    def add_dispatcher(
            self, protocol: ProtocolBase, handler: Callable) -> None:
        self._dispatcher.add_dispatcher(protocol, handler)


async def test():
    logger = Log.create(Log.path('test')).get_logger('test')
    client = ClientBase(logger, 1)
    await client.initialize()

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(test())
    asyncio.get_event_loop().run_forever()
