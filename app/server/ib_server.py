import asyncio
from dataclasses import dataclass
import sys
from typing import Callable, Dict

from app.ib_config import IbConfig, loadConfig
from app.console.console_handler import ConsoleHandler
from app.dispatcher import Dispatcher
from app.redis.redis_client import RedisHandler
from app.server.base_server import BaseServer
from app.server.ib_manager import IbManager
from app.server.protocols import (
    kCmdChannel,
    kCmdAllocatorChannel,
    ResponseStatus,
    worker_type_str,
    ProtocolBase,
    Transporter,
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


class IbServer(BaseServer):
    log_file = 'ib_server'

    def __init__(self, config: IbConfig) -> None:
        self._config: IbConfig = config
        self._log = Log.create(Log.path(self.log_file))
        self._logger = self._log.get_logger('server')
        self._clients: Dict[str, WorkerItem] = {}
        self._redis: RedisHandler = None
        self._clients: Dict[str, WorkerItem] = {}
        self._available_channel_id: int = 0
        self._sweep_task: asyncio.Task = None
        self._console_handler: ConsoleHandler = None
        self._transporter = Transporter()
        self._client_id = config.client_id
        self._ib_manager: IbManager = IbManager(
            self._config.ib_ip, self._config.ib_port, self._client_id)

    async def initialize(self) -> None:
        self._redis = await RedisHandler.create(
            ip=self._config.cmd_redis_ip, port=self._config.cmd_redis_port)
        self._dispatcher = Dispatcher()
        self.add_dispatcher(WorkerJoinRequest, self.on_join_request)
        self.add_dispatcher(Ping, self.on_ping)
        await self._redis.subscribe(kCmdChannel, self.on_cmd)
        self._sweep_task = asyncio.create_task(self._sweep())
        await self._ib_manager.initialize()
        self._console_handler = ConsoleHandler(self, self._redis, self._log)
        self._console_handler.make_default_contracts(
            self._config.default_contracts)

    def ib_manager(self) -> IbManager:
        return self._ib_manager

    def valid_client(self, sid: str) -> bool:
        return sid in self._clients

    def touch(self, sid: str) -> None:
        if sid in self._clients:
            self._clients[sid].touch()

    def add_dispatcher(
            self, protocol: ProtocolBase, handler: Callable) -> None:
        self._dispatcher.add_dispatcher(protocol, handler)

    async def _sweep(self):
        while True:
            await asyncio.sleep(2)
            now = tick_ms()
            remove_list = []
            for client in self._clients.values():
                if client.last_active_ts + kWorkerExpiredPeriod < now:
                    remove_list.append(client.sid)
            if len(remove_list) > 0:
                self._logger.warning(
                    'remove timeout clients: %s', str(remove_list))
                for sid in remove_list:
                    self._clients.pop(sid)

    def _get_available_channel(self) -> str:
        cid = self._available_channel_id
        self._available_channel_id += 1
        return f'ib:clt:{cid}'

    async def send_packet(self, channel: str, packet: ProtocolBase) -> None:
        data = Transporter.serialize(packet.pack())
        await self._redis.publish(channel, data)

    async def send_packet_by_sid(self, sid: str, packet: ProtocolBase) -> None:
        if sid in self._clients:
            await self.send_packet(self._clients[sid].channel, packet)

    async def on_cmd(self, message: bytes) -> None:
        messages = self._transporter.deserialize(message)
        for data in messages:
            await self._dispatcher.on_message(data)

    async def on_join_request(self, request: WorkerJoinRequest):
        response = WorkerJoinResponse()
        response.sid = request.sid
        if request.sid == '':
            response.status = ResponseStatus.kFailed
            self._logger.info(
                'Join request: %s, sid: %s, invalid sid',
                worker_type_str(request.wtype), request.sid)
            await self.send_packet(kCmdAllocatorChannel, response)
            return
        if request.sid in self._clients:
            client = self._clients[request.sid]
            client.touch()
            response.channel = client.channel
            response.status = ResponseStatus.kAlready
            self._logger.info(
                'Join request: %s, sid: %s, already joined with channel: %s',
                worker_type_str(request.wtype), client.sid, client.channel)
            await self.send_packet(kCmdAllocatorChannel, response)
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
        await self.send_packet(kCmdAllocatorChannel, response)

    async def on_ping(self, ping: Ping) -> None:
        if ping.sid not in self._clients:
            return
        self._clients[ping.sid].touch()
        pong = Pong()
        pong.client_ts = ping.ts
        await self.send_packet_by_sid(ping.sid, pong)


async def main():
    config = await loadConfig()
    if len(sys.argv) > 1:
        config.client_id = sys.argv[1]
    server = IbServer(config)
    await server.initialize()

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
    asyncio.get_event_loop().run_forever()
