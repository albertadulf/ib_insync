import asyncio
from dataclasses import dataclass
import json
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.eventloop.defaults import use_asyncio_event_loop
from prompt_toolkit.patch_stdout import patch_stdout
from prompt_toolkit.shortcuts.prompt import PromptSession
from prompt_toolkit.styles import Style

from app.client_base import ClientBase
from app.config_loader import ConfigLoader
from app.server.protocols import (
    ResponseStatus,
    kWorkerTypeConsole,
    ConsoleCommandRequest,
    ConsoleCommandResponse,
)
from app.utils.log import Log

completer = WordCompleter([
    'find_symbols', 'subscribe_market', 'unsubscribe_market',
    'subscribe_market_depth', 'unsubscribe_market_depth', 'order',
    'cancel_order', 'contract', 'orders', 'portfolio', 'cash',
    'list_strategies', 'list_running_strategies', 'start_strategy',
    'stop_strategy', 'run_mock_strategy'], ignore_case=True)
style = Style.from_dict({
    'completion-menu.completion': 'bg:#008888 #ffffff',
    'completion-menu.completion.current': 'bg:#00aaaa #000000',
    'scrollbar.background': 'bg:#88aaaa',
    'scrollbar.button': 'bg:#222222',
})


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
        self._prompt_session: PromptSession = PromptSession(
            completer=completer, style=style)
        self.add_dispatcher(ConsoleCommandResponse,
                            self.on_console_cmd_response)

    async def _handle_console_cmd(self):
        while True:
            with patch_stdout():
                cmd = await self._prompt_session.prompt(
                    '>>> ', async_=True, auto_suggest=AutoSuggestFromHistory())
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
    use_asyncio_event_loop()
    asyncio.get_event_loop().run_until_complete(test())
    asyncio.get_event_loop().run_forever()
