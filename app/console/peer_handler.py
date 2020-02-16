import json
from typing import Any, Callable, List, Tuple

from app.console.console_handler import CommandHandler
from app.server.protocols import (
    kWorkerTypeSubscriber,
    kWorkerTypeIbSimulator,
    kWorkerTypeIbTrader,
)


class PeerHanlder(object):
    def __init__(self, logger: Any):
        self._logger = logger
        self._subscriber_sid_list: List[str] = []
        self._simulator_sid_list: List[str] = []
        self._trader_sid_list: List[str] = []
        self._selected_sid: Tuple[str, str, int] = ('', '', -1)
        self._command_sid: str = ''
        self._command_handler: CommandHandler = CommandHandler()
        self._add_handler('select', self._select)
        self._add_handler('services', self._show_services)
        self._add_handler('subscribe_market', self._pick_subscriber)
        self._add_handler('unsubscribe_market', self._pick_subscriber)
        self._add_handler('subscribe_market_depth', self._pick_subscriber)
        self._add_handler('unsubscribe_market_depth', self._pick_subscriber)

    def clear(self) -> None:
        self._subscriber_sid_list = []
        self._simulator_sid_list = []
        self._trader_sid_list = []
        self._empty_selected_sid()

    def get_selected_nick(self) -> str:
        if self._selected_sid[1] == '':
            return 'Unselected'
        return self._selected_sid[1]

    def _select(self, role: str, idx: str = '0') -> Tuple[str]:
        idx = int(idx)
        if role not in ('simulator', 'trader'):
            return f'invalid argument select simulator or trader'
        worker_type = {'simulator': kWorkerTypeIbSimulator,
                       'trader': kWorkerTypeIbTrader}[role]
        nick, sid_list = self._get_nick_and_list(worker_type)
        if len(sid_list) <= idx:
            return f'invalid argument select {role} index {idx} out' \
                + f'of bound, total length is {len(sid_list)}'
        self._selected_sid = (sid_list[idx], f'{nick}{idx}', worker_type)
        return f'Success selected {self._selected_sid[1]}'

    def _show_services(self) -> str:
        return f'{len(self._subscriber_sid_list)} subscriber services, ' \
            + f'{len(self._simulator_sid_list)} simulator services, ' \
            + f'{len(self._trader_sid_list)} trader services'

    def _pick_subscriber(self, *args) -> str:
        if len(self._subscriber_sid_list) == 0:
            return 'Failed no subscriber'
        self._command_sid = self._subscriber_sid_list[0]
        return ''

    def _empty_selected_sid(self) -> None:
        self._selected_sid = ('', '', -1)

    def _update_selected_nick(self) -> None:
        nick, sid_list = self._get_nick_and_list(self._selected_sid[2])
        if sid_list is None:
            self._empty_selected_sid()
        elif self._selected_sid[0] not in sid_list:
            self._empty_selected_sid()
        else:
            idx = sid_list.index(self._selected_sid[0])
            self._selected_sid = (self._selected_sid[0],
                                  f'{nick}{idx}', self._selected_sid[2])

    def _get_nick_and_list(self, worker_type: int) -> Tuple[str, List[str]]:
        if worker_type == kWorkerTypeSubscriber:
            return ('subscriber', self._subscriber_sid_list)
        elif worker_type == kWorkerTypeIbSimulator:
            return ('simulator', self._simulator_sid_list)
        elif worker_type == kWorkerTypeIbTrader:
            return ('trader', self._trader_sid_list)
        return ('', None)

    def _add_handler(self, op: str, handler: Callable) -> None:
        self._command_handler.add_handler(op, handler)

    async def handle_command(self, command: str) -> Tuple[bool, str, str]:
        try:
            message = ''
            message = await self._command_handler.handle_command(command)
            message = json.loads(message)
        except Exception:
            self._command_sid = self._selected_sid[0]
        finally:
            if message != '':
                return (False, '', message)
            return (True, self._command_sid, '')

    async def on_client_online(self, sid: str, worker_type: int) -> None:
        nick, sid_list = self._get_nick_and_list(worker_type)
        if sid_list is not None and sid not in sid_list:
            sid_list.append(sid)
            self._logger.info(f'new {nick} joined: {sid}')

    async def on_client_offline(self, sid: str, worker_type: int) -> None:
        nick, sid_list = self._get_nick_and_list(worker_type)
        if sid_list is not None and sid in sid_list:
            sid_list.remove(sid)
            self._logger.info(f'{nick} {sid} quit')
        if worker_type == self._selected_sid[2]:
            self._update_selected_nick()
