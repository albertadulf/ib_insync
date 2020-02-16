import asyncio
from functools import partial
import json
from typing import Callable, Dict, List

from app.server.ib_manager import IbManager
from app.server.protocols import (
    PublishedMarketData,
)
from app.trader.mock_trade_manager import MockTradeManager
from app.trader.normal_trade_manager import NormalTradeManager
from app.trader.random_trader import RandomTrader
from app.trader.strategy_manager import Strategies
from app.utils.log import Log
from ib_insync import Contract


class InvalidCommandExecption(Exception):
    def __init__(self, cmd: str):
        self._cmd = cmd

    def __str__(self):
        return f'Invalid command: {self._cmd}'

    __repr__ = __str__


def iscoroutinefunction(obj):
    while isinstance(obj, partial):
        obj = obj.func
    return asyncio.iscoroutinefunction(obj)


class CommandHandler(object):
    def __init__(self):
        self._handlers: Dict[str, Callable] = {}

    def add_handler(self, op: str, handler: Callable) -> None:
        self._handlers[op] = handler

    async def handle_command(self, command: str) -> str:
        cmds = command.split(' ')
        args = []
        kwargs = {}
        for item in cmds:
            if '=' in item:
                v = item.split('=')
                kwargs[v[0]] = v[1]
            else:
                args.append(item)
        return await self.on_command(args.pop(0), *args, **kwargs)

    async def on_command(self, op: str, *args, **kwargs) -> str:
        if op not in self._handlers:
            raise InvalidCommandExecption(op)
        if iscoroutinefunction(self._handlers[op]):
            result = await self._handlers[op](*args, **kwargs)
        else:
            result = self._handlers[op](*args, **kwargs)
        return json.dumps(result)


class ConsoleHandler(object):
    def __init__(self, log: Log, ib_manager: IbManager) -> None:
        self._logger = log.get_logger('consolehandler')
        self._ib_manager: IbManager = ib_manager
        self._trade_manager: NormalTradeManager = NormalTradeManager(
            self._ib_manager)
        self._mock_manager: MockTradeManager = MockTradeManager(
            self._ib_manager._recorder)
        self._command_handler: CommandHandler = CommandHandler()
        self._contracts: Dict[int, Contract] = {}
        self._trader: RandomTrader = RandomTrader(
            self._ib_manager._ib, self._ib_manager)
        self._print_market_symbol: str = ''
        self._ib_manager._events.market_data += self._print_market_data
        if self._ib_manager._data_subscriber:
            self.add_handler('subscribe_market', self._ib_manager.sub_market)
            self.add_handler('unsubscribe_market',
                             self._ib_manager.unsub_market)
            self.add_handler('subscribe_market_depth',
                             self._ib_manager.sub_market_depth)
            self.add_handler('unsubscribe_market_depth',
                             self._ib_manager.unsub_market_depth)
        else:
            self.add_handler('find_symbols', self._ib_manager.find_symbols)
            self.add_handler('order', self._ib_manager.place_order)
            self.add_handler('cancel_order', self._ib_manager.cancel_order)
            self.add_handler('contracts', self._ib_manager.get_contracts)
            self.add_handler('add_contract', self._ib_manager.add_contract)
            self.add_handler('orders', self._ib_manager.orders)
            self.add_handler('portfolio', self._ib_manager.portfolio)
            self.add_handler('tstart', self.start_trader)
            self.add_handler('tstop', self.stop_trader)
            self.add_handler('cash', self._ib_manager._account_recorder.cash)
            self.add_handler('list_strategies', self.list_strategies)
            self.add_handler('list_running_strategies',
                             self._trade_manager.list_running_strategies)
            self.add_handler('start_strategy',
                             self._trade_manager.start_strategy)
            self.add_handler('stop_strategy',
                             self._trade_manager.stop_strategy)
            self.add_handler('run_mock_strategy',
                             self._mock_manager.test_strategy)
            self.add_handler('tod', self._trade_manager.place_order)
            self.add_handler('tcod', self._trade_manager.cancel_order)
            self.add_handler('print_market', self._print_market)

    def _print_market_data(self, data: PublishedMarketData) -> None:
        if data.alias == self._print_market_symbol:
            print(data)

    def _print_market(self, symbol: str) -> str:
        self._print_market_symbol = symbol.upper()

    def list_strategies(self) -> List[str]:
        return [s for s in Strategies.keys()]

    def add_handler(self, op: str, handler: Callable) -> None:
        self._command_handler.add_handler(op, handler)

    async def handle_cmd(self, cmd: str) -> str:
        return await self._command_handler.handle_command(cmd)

    def start_trader(self, alias: str) -> str:
        contract = self._ib_manager.get_contract(alias)
        if contract is None:
            return f'start failed: no contract for symbol {alias}'
        self._trader.start(contract)
        return f'start trader for {str(contract)} success'

    def stop_trader(self) -> str:
        self._trader.stop()
        return 'stop trader success'
