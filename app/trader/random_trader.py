import asyncio
from dataclasses import dataclass
from random import randint
from typing import Set

from app.server.ib_manager import IbManager
from app.utils.common_util import tick_ms
from ib_insync import Contract, IB, Ticker


@dataclass
class TradeItem(object):
    bid_price: float = 0
    bid_size: int = 0
    ask_price: float = 0
    ask_size: int = 0
    ts: int = 0

    def __str__(self) -> str:
        return f'bid: {self.bid_price}, ask: {self.ask_price}'

    __repr__ = __str__


@dataclass
class OrderItem(object):
    ts: int = 0
    order_id: int = 0
    side: str = ''
    price: float = 0.0


class RandomTrader(object):
    def __init__(
            self, ib: IB, ib_manager: IbManager) -> None:
        self._ib: IB = ib
        self._ib_manager: IbManager = ib_manager
        self._contract: Contract = None
        self._queue: asyncio.Queue = asyncio.Queue()
        self._default_size: int = 1000
        self._task: asyncio.Task = None
        self._trading_times: int = 0
        self._is_stopping: bool = False
        self._is_stopped: bool = True
        self._start: int = 0
        self._order_item: OrderItem = None
        self._ib.pendingTickersEvent += self.on_data_update
        self._ib.commissionReportEvent += self.on_commission_report

    def start(self, contract: Contract) -> None:
        if not self._is_stopped:
            return
        self._contract = contract
        self._task = asyncio.create_task(self._process())
        self._is_stopped = False
        self._start = randint(0, 1)

    def stop(self) -> None:
        if self._is_stopped:
            return
        if self._trading_times % 2 == 1:
            self._is_stopping = True
            self._is_stopped = False
        else:
            self._task.cancel()
            self._task = None
            self._is_stopping = False
            self._is_stopped = True

    async def _process(self) -> None:
        trade_fun = [self.buy_trade, self.sell_trade]
        while True:
            item = await self._queue.get()
            order_item = self._order_item
            if order_item:
                now = tick_ms()
                if order_item.ts + 10000 < now:
                    print(f'cancel {order_item.side}: {order_item.price}')
                    self._ib_manager.cancel_order(order_item.order_id)
                    self._order_item = None
                continue
            op_id = randint(0, 5)
            if op_id == 0:
                trade_fun[(self._start + self._trading_times) % 2](item)
                if self._is_stopping:
                    self._is_stopped = True
                    task = self._task
                    self._task = None
                    task.cancel()

    def buy_trade(self, item: TradeItem) -> None:
        print(f'place buy {self._default_size}@{item.ask_price}')
        trade = self._ib_manager._place_order(
            self._contract, 'buy', self._default_size, item.ask_price)
        self._order_item = OrderItem(
            tick_ms(), trade.order.orderId, 'buy', trade.order.lmtPrice)

    def sell_trade(self, item: TradeItem) -> None:
        print(f'place sell {self._default_size}@{item.bid_price}')
        trade = self._ib_manager._place_order(
            self._contract, 'sell', self._default_size, item.bid_price)
        self._order_item = OrderItem(
            tick_ms(), trade.order.orderId, 'sell', trade.order.lmtPrice)

    def on_data_update(self, tickers: Set[Ticker]) -> None:
        if self._task is None:
            return
        for ticker in tickers:
            if ticker.contract == self._contract:
                if len(ticker.domBids) > 0 and len(ticker.domAsks) > 0:
                    self._queue.put_nowait(
                        TradeItem(ticker.domBids[0].price,
                                  ticker.domBids[0].size,
                                  ticker.domAsks[0].price,
                                  ticker.domAsks[0].size,
                                  ticker.time.timestamp() * 1000))

    def on_commission_report(self, trade, fill, report) -> None:
        if self._task is None:
            return
        if trade.contract.symbol == self._contract.symbol:
            self._trading_times += 1
            self._order_item = None
