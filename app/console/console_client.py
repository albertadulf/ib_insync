import asyncio
import json
import sys
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.eventloop.defaults import use_asyncio_event_loop
from prompt_toolkit.patch_stdout import patch_stdout
from prompt_toolkit.shortcuts.prompt import PromptSession
from prompt_toolkit.styles import Style

from app.client_base import ClientBase
from app.console.peer_handler import PeerHanlder
from app.ib_config import IbConfig, loadConfig
from app.server.protocols import (
    ResponseStatus,
    kWorkerTypeConsole,
    ConsoleCommandRequest,
    ConsoleCommandResponse,
    NotifyClientStatusChanged,
    RequireGroupInfo,
)
from app.utils.log import Log

completer = WordCompleter([
    'find_symbols', 'subscribe_market', 'unsubscribe_market',
    'subscribe_market_depth', 'unsubscribe_market_depth', 'order', 'services',
    'cancel_order', 'contracts', 'add_contract', 'orders', 'portfolio', 'cash',
    'list_strategies', 'list_running_strategies', 'start_strategy', 'select',
    'stop_strategy', 'run_mock_strategy', 'print_market'], ignore_case=True)
style = Style.from_dict({
    'completion-menu.completion': 'bg:#008888 #ffffff',
    'completion-menu.completion.current': 'bg:#00aaaa #000000',
    'scrollbar.background': 'bg:#88aaaa',
    'scrollbar.button': 'bg:#222222',
})


class ConsoleClient(ClientBase):
    log_file = 'console'

    def __init__(self, config: IbConfig) -> None:
        self._log = Log.create(Log.path(self.log_file))
        ClientBase.__init__(
            self, self._log.get_logger('consoleclient'),
            kWorkerTypeConsole, config)
        self._cmd_task: asyncio.Task = None
        self._prompt_session: PromptSession = PromptSession(
            completer=completer, style=style)
        self._peer_handler: PeerHanlder = PeerHanlder(self._logger)
        self.add_dispatcher(ConsoleCommandResponse,
                            self.on_console_cmd_response)
        self.add_dispatcher(NotifyClientStatusChanged,
                            self.on_client_status_changed)

    async def _handle_console_cmd(self):
        while True:
            with patch_stdout():
                target = self._peer_handler.get_selected_nick()
                cmd = await self._prompt_session.prompt(
                    f'({target})>>> ', async_=True,
                    auto_suggest=AutoSuggestFromHistory())
            if cmd:
                retransfer, peer_sid, message = \
                    await self._peer_handler.handle_command(cmd)
                if message:
                    self._logger.info('%s', message)
                if retransfer:
                    request = ConsoleCommandRequest()
                    request.sid = self.sid()
                    request.peer_sid = peer_sid
                    request.cmd = cmd
                    await self.send_packet(request)

    async def on_console_cmd_response(self, response: ConsoleCommandResponse):
        if response.peer_sid != self._sid:
            return
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

    async def on_client_status_changed(
            self, notification: NotifyClientStatusChanged) -> None:
        if notification.online:
            await self._peer_handler.on_client_online(
                notification.sid, notification.wtype)
        else:
            await self._peer_handler.on_client_offline(
                notification.sid, notification.wtype)

    async def on_client_ready(self) -> None:
        if self._cmd_task is None:
            self._cmd_task = asyncio.create_task(self._handle_console_cmd())
        self._peer_handler.clear()
        request = RequireGroupInfo()
        request.sid = self._sid
        await self.send_packet(request)


async def test():
    config_file = 'app/config.json'
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    config = await loadConfig(config_file)
    client = ConsoleClient(config)
    await client.initialize()


if __name__ == '__main__':
    use_asyncio_event_loop()
    asyncio.get_event_loop().run_until_complete(test())
    asyncio.get_event_loop().run_forever()
