from aioconsole import ainput
import asyncio
from dataclasses import dataclass
import json

from app.client_base import ClientBase
from app.config_loader import ConfigLoader
from app.server.protocols import (
    ResponseStatus,
    kWorkerTypeConsole,
    ConsoleCommandRequest,
    ConsoleCommandResponse,
)
from app.utils.log import Log


@dataclass
class ConsoleConfig(object):
    cmd_redis_ip: str = 'localhost'
    cmd_redis_port: int = 6379


class ConsoleClient(ClientBase):
    log_file = 'console'

    def __init__(self, config: ConsoleConfig) -> None:
        self._log = Log.create(Log.path(self.log_file))
        self._config = config
        ClientBase.__init__(
            self, self._log.get_logger('consoleclient'), kWorkerTypeConsole,
            cmd_redis_ip=self._config.cmd_redis_ip,
            cmd_redis_port=self._config.cmd_redis_port)
        self._cmd_task: asyncio.Task = None
        self.add_dispatcher(ConsoleCommandResponse,
                            self.on_console_cmd_response)

    async def _handle_console_cmd(self):
        while True:
            cmd = await ainput('>>> ')
            if cmd:
                request = ConsoleCommandRequest()
                request.sid = self.sid()
                request.cmd = cmd
                await self.send_packet(request)

    async def on_console_cmd_response(self, response: ConsoleCommandResponse):
        if response.status == ResponseStatus.kSuccess:
            self._logger.info('success response:')
            obj = json.loads(response.msg)
            if type(obj) is list:
                for i, value in enumerate(obj):
                    if i % 5 == 0:
                        await asyncio.sleep(0.001)
                    self._logger.info('%s', str(value))
            else:
                self._logger.info('%s', str(obj))
        else:
            self._logger.info('response failed with status: %d, msg: %s',
                              response.status, response.msg)

    async def on_client_ready(self) -> None:
        if self._cmd_task is None:
            self._cmd_task = asyncio.create_task(self._handle_console_cmd())


async def test():
    config = ConsoleConfig()
    config = await ConfigLoader.load('app/console/config.json', config)
    client = ConsoleClient(config)
    await client.initialize()


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(test())
    asyncio.get_event_loop().run_forever()
