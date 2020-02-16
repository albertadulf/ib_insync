import asyncio
import sys

from app.client_base import ClientBase
from app.console.console_handler import ConsoleHandler
from app.contract_manager import ContractManager
from app.ib_config import IbConfig, loadConfig
from app.server.ib_manager import IbManager
from app.server.market_data_handler import MarketDataHandler
from app.server.protocols import (
    kWorkerTypeSubscriber,
    kWorkerTypeIbSimulator,
    kWorkerTypeIbTrader,
    ResponseStatus,
    ConsoleCommandRequest,
    ConsoleCommandResponse,
)
from app.server.server_events import ServerEvents
from app.utils.log import Log


class IbServiceClient(ClientBase):
    log_file = 'ib_service'

    def __init__(self, config: IbConfig) -> None:
        if config.data_subscriber:
            worker_type = kWorkerTypeSubscriber
        else:
            if config.ib_port == 4003:
                worker_type = kWorkerTypeIbTrader
            else:
                worker_type = kWorkerTypeIbSimulator
        self._log = Log.create(Log.path(self.log_file))
        self._contract_manager: ContractManager = ContractManager()
        self._market_data_handler: MarketDataHandler = None
        self._events: ServerEvents = ServerEvents()
        self._ib_manager: IbManager = IbManager(
            config.ib_ip, config.ib_port,
            config.client_id, self._contract_manager, self._events,
            config.data_subscriber)
        self._console_handler: ConsoleHandler = ConsoleHandler(
            self._log, self._ib_manager)
        ClientBase.__init__(
            self, self._log.get_logger('ibserviceclient'), worker_type, config)
        self.add_dispatcher(ConsoleCommandRequest, self._on_command)

    async def _on_command(self, request: ConsoleCommandRequest) -> None:
        response = ConsoleCommandResponse()
        response.peer_sid = request.sid
        response.sid = self._sid
        try:
            response.msg = await self._console_handler.handle_cmd(request.cmd)
            response.status = ResponseStatus.kSuccess
        except Exception as e:
            if request.peer_sid == '':
                return
            response.status = ResponseStatus.kFailed
            response.msg = str(e)
        await self.send_packet(response)

    async def initialize(self) -> None:
        await ClientBase.initialize(self)
        await self._contract_manager.initialize()
        await self._ib_manager.initialize()
        self._market_data_handler = MarketDataHandler(
            self._ib_manager._ib, self._config.data_subscriber,
            self._redis, self._events)
        await self._market_data_handler.initialize()


async def main():
    config_file = 'app/config.json'
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    config = await loadConfig(config_file)
    service = IbServiceClient(config)
    await service.initialize()

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
    asyncio.get_event_loop().run_forever()
