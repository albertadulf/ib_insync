import asyncio
from dataclasses import dataclass
from typing import Tuple

from app.recorder.recorder import Recorder
from ib_insync import IB


@dataclass
class CommissionItem(object):
    ts: int = 0
    change: float = 0.0
    symbol: str = ''
    side: str = ''
    quantity: int = 0
    price: float = 0.0
    commission: float = 0.0

    def to_dict(self):
        return {
            'ts': self.ts,
            'change': self.change,
            'symbol': self.symbol,
            'side': self.side,
            'quantity': self.quantity,
            'price': self.price,
            'commission': self.commission
        }

    def to_tuple(self) -> Tuple:
        return (
            self.ts, self.change, self.symbol, self.side,
            self.quantity, self.price, self.commission)

    def __str__(self) -> str:
        return f'{self.ts}#{self.symbol}#{self.side} ' \
            + f'{self.quantity}@{self.price} {self.commission}' \
            + f' {self.change}'

    __repr__ = __str__


class AccountRecorder(object):
    account_file = 'trade_info'

    def __init__(self, ib: IB, recorder: Recorder) -> None:
        self._ib = ib
        self._recorder = recorder
        self._queue: asyncio.Queue = asyncio.Queue()
        self._task: asyncio.Task = asyncio.create_task(self._process())
        self._ib.updatePortfolioEvent += self.update_portfolio
        self._ib.accountValueEvent += self.update_account_value
        self._ib.orderStatusEvent += self.update_order_status
        self._ib.commissionReportEvent += self.update_commission
        self._ib.execDetailsEvent += self.on_executed
        self._account: str = ''
        self._cash_value: float = 0.0
        self._commission_item: CommissionItem = CommissionItem()

    async def _process(self) -> None:
        while True:
            data = await self._queue.get()
            await self._recorder.consume(self.account_file, data)

    def cash(self) -> float:
        return self._cash_value

    def is_ready(self) -> bool:
        return self._cash_value != 0.0

    def update_account(self, account: str) -> None:
        self._account = account

    def update_portfolio(self, item) -> None:
        # print(f'portfolio: {item}')
        pass

    def update_account_value(self, item) -> None:
        if item.account == self._account and item.tag == 'TotalCashValue':
            if float(item.value) != self._cash_value:
                self._cash_value = float(item.value)

    def update_order_status(self, trade) -> None:
        # 3print(f'order status, {status}')
        return

    def update_commission(self, trade, fill, report) -> None:
        if not self.is_ready():
            return
        side = fill.execution.side
        price = fill.execution.price
        quantity = fill.execution.shares
        commission = fill.commissionReport.commission
        quantity = quantity if side == 'BOT' else -1 * quantity
        change = -1 * quantity * price - commission
        self._commission_item.symbol = fill.contract.symbol
        self._commission_item.ts = int(fill.time.timestamp() * 1000)
        self._commission_item.side = side
        self._commission_item.price = price
        self._commission_item.quantity = quantity
        self._commission_item.commission = commission
        self._commission_item.change = change
        self._queue.put_nowait(self._commission_item.to_tuple())
        print(self._commission_item)

    def on_executed(self, trade, fill) -> None:
        return
