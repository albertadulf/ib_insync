import asyncio
from dataclasses import dataclass
from typing import Any, Callable, Dict

from app.dispatcher import Dispatcher
from app.ib_config import IbConfig, loadConfig
from app.redis.redis_client import RedisHandler
from app.server.protocols import (
    kCmdChannel,
    kCmdAllocatorChannel,
    ProtocolBase,
    Transporter,
    ResponseStatus,
    worker_type_str,
    WorkerJoinRequest,
    WorkerJoinResponse,
    Ping,
    Pong,
)
from app.utils.common_util import tick_ms
from app.utils.log import Log

kWorkerExpiredPeriod = 4000


@dataclass
class WorkerItem(object):
    sid: str
    worker_type: int = 0
    channel: str = ''
    last_active_ts: int = 0

    def touch(self) -> None:
        self.last_active_ts = tick_ms()


class ServerBase(object):
    def __init__(self, logger: Any, config: IbConfig) -> None:
        self._logger = logger
        self._config: IbConfig = config
        self._cmd_channel: str = f'{kCmdChannel}:{config.group}'
        self._allocator_channel: str = f'{kCmdAllocatorChannel}:{config.group}'
        self._dispatcher: Dispatcher = Dispatcher()
        self._clients: Dict[str, WorkerItem] = {}
        self._redis: RedisHandler = None
        self._available_channel_id: int = 0
        self._sweep_task: asyncio.Task = None
        self._transporter: Transporter = Transporter()

    def _get_available_channel(self) -> str:
        cid = self._available_channel_id
        self._available_channel_id += 1
        return f'ib:clt:{self._config.group}:{cid}'

    async def _on_cmd(self, message: bytes) -> None:
        messages = self._transporter.deserialize(message)
        for data in messages:
            await self._dispatcher.on_message(data)

    async def _on_join_request(self, request: WorkerJoinRequest) -> None:
        response = WorkerJoinResponse()
        response.sid = request.sid
        if request.sid == '':
            response.status = ResponseStatus.kFailed
            self._logger.info(
                'Join request: %s, sid: %s, invalid sid',
                worker_type_str(request.wtype), request.sid)
            await self.send_packet(response, channel=self._allocator_channel)
            return
        if request.sid in self._clients:
            client = self._clients[request.sid]
            client.touch()
            response.channel = client.channel
            response.status = ResponseStatus.kAlready
            self._logger.info(
                'Join request: %s, sid: %s, already joined with channel: %s',
                worker_type_str(request.wtype), client.sid, client.channel)
            await self.send_packet(response, channel=self._allocator_channel)
            return
        channel = self._get_available_channel()
        item = WorkerItem(request.sid, request.wtype, channel)
        item.touch()
        self._clients[request.sid] = item
        response.channel = channel
        response.status = ResponseStatus.kSuccess
        self._logger.info(
            'Join request: %s, sid: %s, successfully with channel: %s',
            worker_type_str(request.wtype), request.sid, channel)
        await self.send_packet(response, channel=self._allocator_channel)
        await self.on_client_joined(item)

    async def _on_ping(self, ping: Ping) -> None:
        if not self.touch(ping.sid):
            return
        pong = Pong()
        pong.client_ts = ping.ts
        await self.send_packet(pong, sid=ping.sid)

    async def _sweep(self) -> None:
        while True:
            # check it every 2 seconds.
            await asyncio.sleep(2)
            now = tick_ms()
            remove_list = []
            for client in self._clients.values():
                if client.last_active_ts + kWorkerExpiredPeriod < now:
                    remove_list.append(client.sid)
                    await self.on_client_quit(client)
            if len(remove_list) > 0:
                self._logger.warning(
                    'remove timeout clients: %s', str(remove_list))
                for sid in remove_list:
                    self._clients.pop(sid)

    async def on_client_joined(self, item: WorkerItem) -> None:
        # need to implement by derived server
        pass

    async def on_client_quit(self, item: WorkerItem) -> None:
        # need to implement by derived server
        pass

    def touch(self, sid: str) -> bool:
        if sid in self._clients:
            self._clients[sid].touch()
            return True
        return False

    def add_dispatcher(self, protocol: ProtocolBase,
                       handler: Callable) -> None:
        self._dispatcher.add_dispatcher(protocol, handler)

    async def send_packet(self, packet: ProtocolBase,
                          sid: str = '', channel: str = '') -> None:
        if sid != '':
            if sid in self._clients:
                channel = self._clients[sid].channel
            else:
                return
        data = Transporter.serialize(packet.pack())
        if channel != '':
            await self._redis.publish(channel, data)
        else:
            await self._redis.publish(self._allocator_channel, data)

    async def initialize(self) -> None:
        self._redis = await RedisHandler.create(
            ip=self._config.cmd_redis_ip, port=self._config.cmd_redis_port)
        self.add_dispatcher(WorkerJoinRequest, self._on_join_request)
        self.add_dispatcher(Ping, self._on_ping)
        await self._redis.subscribe(self._cmd_channel, self._on_cmd)
        self._sweep_task = asyncio.create_task(self._sweep())


async def test():
    logger = Log.create(Log.path('test')).get_logger('test')
    config_file = 'app/config.json'
    config = await loadConfig(config_file)
    server = ServerBase(logger, config)
    await server.initialize()

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(test())
    asyncio.get_event_loop().run_forever()
