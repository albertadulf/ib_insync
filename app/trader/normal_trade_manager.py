import asyncio
from dataclasses import dataclass
from typing import Dict, List, Set

from app.recorder.recorder import Recorder
from app.server.ib_manager import IbManager
from app.trader.strategy_base import MarketData, StrategyBase
from app.trader.strategy_manager import Strategies
from app.trader.trade_manager_base import TradeManagerBase
from ib_insync import Contract, IB, Ticker, Trade


@dataclass
class StrategyItem(object):
    sid: int = 0
    name: str = ''
    contract: Contract = None
    impl: StrategyBase = None
    shares: int = 0
    value: float = 0.0
    trade_times: int = 0

    def __str__(self) -> str:
        return f'{self.name}({self.sid}): {str(self.contract)},' \
            + f' shares: {self.shares}, value: {self.value},' \
            + f' trade_times: {self.trade_times}'

    __repr__ = __str__


class NormalTradeManager(TradeManagerBase):
    recorder_file = 'normal_trade'

    def __init__(self, ib_manager: IbManager):
        self._ib: IB = ib_manager._ib
        self._ib_manager: IbManager = ib_manager
        self._recorder: Recorder = ib_manager._recorder
        self._running_strategies: Dict[int, StrategyItem] = {}
        self._available_sid: int = 0
        # orderId: strategyId
        self._pending_orders: Dict[int, int] = {}
        self._ib.orderStatusEvent += self._on_order_status_changed
        self._ib.pendingTickersEvent += self._on_market_data
        self._task: asyncio.Task = asyncio.create_task(self._recording())
        self._queue: asyncio.Queue = asyncio.Queue()

    async def _recording(self) -> None:
        while True:
            data = await self._queue.get()
            await self._recorder.consume(self.recorder_file, data)

    def _get_available_sid(self) -> int:
        sid = self._available_sid
        self._available_sid += 1
        return sid

    def _on_order_filled(
            self, item: StrategyItem,
            value: float, shares: int, side: str, fill_time: int) -> None:
        avg_price = value / shares
        if side.upper() == 'BUY':
            item.shares += shares
            item.value -= value
            change = -value
        else:
            item.shares -= shares
            item.value += value
            change = value
        item.trade_times += 1
        data = (fill_time, item.name, item.contract.symbol,
                side, shares, avg_price, change)
        self._queue.put_nowait(data)

    def _on_market_data(self, tickers: Set[Ticker]) -> None:
        for ticker in tickers:
            depth = min(len(ticker.domBids), len(ticker.domAsks))
            if depth == 0:
                continue
            bids_price = []
            bids_amount = []
            asks_price = []
            asks_amount = []
            for i in range(0, depth):
                bids_price.append(float(ticker.domBids[i].price))
                bids_amount.append(int(ticker.domBids[i].size))
                asks_price.append(float(ticker.domAsks[i].price))
                asks_amount.append(int(ticker.domAsks[i].size))
            data = MarketData(
                int(ticker.time.timestamp() * 1000),
                tuple(bids_price), tuple(bids_amount),
                tuple(asks_price), tuple(asks_amount))
            for strategy in self._running_strategies.values():
                if strategy.contract == ticker.contract:
                    strategy.impl.on_market_data(data)

    def _on_order_status_changed(self, trade: Trade) -> None:
        if trade.order.orderId not in self._pending_orders:
            return
        oid = trade.order.orderId
        sid = self._pending_orders[oid]
        if trade.orderStatus.status == 'Cancelled':
            print(f'Cancelled id: {oid}, {str(trade.order)}')
            self._running_strategies[sid].impl.on_canceled(oid)
            self._pending_orders.pop(oid)
        elif trade.orderStatus.status == 'Filled':
            value = 0
            shares = 0
            action = trade.order.action
            fill_time = 0
            for fill in trade.fills:
                value += fill.execution.price * fill.execution.shares
                shares += fill.execution.shares
                fill_time = int(fill.time.timestamp() * 1000)
            print(f'Filled {action}: {shares}, value: {value},'
                  + f' id: {oid}, {str(trade.order)}')
            self._on_order_filled(
                self._running_strategies[sid],
                value, shares, action, fill_time)
            self._running_strategies[sid].impl.on_commission(oid, value)
            self._pending_orders.pop(oid)
        elif trade.orderStatus.status != 'Submitted':
            print(f'order status: {trade.orderStatus.status}')
            print(str(trade))

    def start_strategy(self, contract: Contract, name: str) -> str:
        if name not in Strategies:
            return f'Failed: no {name} strategy'
        sid = self._get_available_sid()
        item = StrategyItem(
            sid, name, contract, Strategies[name](sid, self))
        self._running_strategies[sid] = item
        return f'start straregy {str(item)})'

    def stop_strategy(self, strategy_id: int) -> str:
        strategy_id = int(strategy_id)
        if strategy_id in self._running_strategies.keys():
            item = self._running_strategies[strategy_id]
            self._running_strategies.pop(strategy_id)
            return f'success stop strategy {str(item)}'
        return f'no strategy id: {strategy_id}'

    def list_running_strategies(self) -> List[str]:
        return [str(item) for item in self._running_strategies.values()]

    def place_order(
            self, strategy_id: int, side: str, price: float, size: int) -> int:
        strategy_id = int(strategy_id)
        if strategy_id not in self._running_strategies.keys():
            return -1
        item = self._running_strategies[strategy_id]
        contract = item.contract
        print(f'[{item.name}({item.sid})]: place order {contract.symbol},'
              + f' {side}: {size}@{price}')
        trade = self._ib_manager._place_order(contract, side, size, price)
        self._pending_orders[trade.order.orderId] = strategy_id
        return trade.order.orderId

    def cancel_order(self, order_id: int) -> None:
        self._ib_manager.cancel_order(order_id)


async def test():
    manager = IbManager('localhost', 4002, 3)
    await manager.initialize()
    trade_manager = NormalTradeManager(manager)

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(test())
    asyncio.get_event_loop().run_forever()
