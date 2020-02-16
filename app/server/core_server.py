import asyncio
import sys

from app.server_base import ServerBase, WorkerItem
from app.ib_config import IbConfig, loadConfig
from app.server.protocols import (
    ConsoleCommandRequest,
    ConsoleCommandResponse,
    NotifyClientStatusChanged,
    RequireGroupInfo,
)
from app.utils.log import Log


class CoreServer(ServerBase):
    log_file = 'core_server'

    def __init__(self, config: IbConfig) -> None:
        self._log = Log.create(Log.path(self.log_file))
        ServerBase.__init__(self, self._log.get_logger('coreserver'), config)
        self.add_dispatcher(ConsoleCommandRequest, self._on_request)
        self.add_dispatcher(ConsoleCommandResponse, self._on_response)
        self.add_dispatcher(RequireGroupInfo, self._on_group_info_require)

    async def _on_request(self, request: ConsoleCommandRequest) -> None:
        self.touch(request.sid)
        await self.send_packet(request, sid=request.peer_sid)

    async def _on_response(self, response: ConsoleCommandResponse) -> None:
        self.touch(response.sid)
        await self.send_packet(response, sid=response.peer_sid)

    async def _on_group_info_require(self, request: RequireGroupInfo) -> None:
        self.touch(request.sid)
        notification = NotifyClientStatusChanged()
        notification.online = True
        for client in self._clients.values():
            if client.sid == request.sid:
                continue
            notification.sid = client.sid
            notification.wtype = client.worker_type
            await self.send_packet(notification, request.sid)

    async def on_client_joined(self, item: WorkerItem) -> None:
        notification = NotifyClientStatusChanged()
        notification.sid = item.sid
        notification.wtype = item.worker_type
        notification.online = True
        await self.send_packet(notification)

    async def on_client_quit(self, item: WorkerItem) -> None:
        notification = NotifyClientStatusChanged()
        notification.sid = item.sid
        notification.wtype = item.worker_type
        notification.online = False
        await self.send_packet(notification)


async def main():
    config_file = 'app/config.json'
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    config = await loadConfig(config_file)
    server = CoreServer(config)
    await server.initialize()

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
    asyncio.get_event_loop().run_forever()
