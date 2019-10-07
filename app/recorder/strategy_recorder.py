import asyncio
from dataclasses import dataclass
from typing import List, Tuple

from app.recorder.recorder import Recorder
from app.utils.common_util import tick_ms


@dataclass
class TradeInfoItem(object):
    ts: int = 0
    side: str = ''
    size: int = 0
    avg_price: float = 0.0
    removed: bool = False

    def to_tuple(self) -> Tuple:
        value = self.size * self.avg_price
        value = value if self.side == 'sell' else -1 * value
        return (self.ts, self.side, self.size, self.avg_price, value)


class StrategyRecorder(object):
    def __init__(self, recorder: Recorder, filename: str) -> None:
        self._recorder = recorder
        self._buy_count = 0
        self._sell_count = 0
        self._buy_index_list: List[int] = []
        self._sell_index_list: List[int] = []
        self._trade_info_list: List[TradeInfoItem] = []
        self._task: asyncio.Task = None
        self._file_name: str = '' if filename == '' \
                               else f'{filename}_{tick_ms()}'

    def destroy(self):
        if self._task:
            self._task.cancel()

    def add_trade_info(
            self, ts: int, side: str, size: int, value: float) -> None:
        if size == 0:
            return
        info = TradeInfoItem(ts, side, size, value / size)
        index = len(self._trade_info_list)
        if side == 'buy':
            self._buy_count += 1
            self._buy_index_list.append(index)
        else:
            self._sell_count += 1
            self._sell_index_list.append(index)
        self._trade_info_list.append(info)

    def polish_result(self) -> None:
        if self._buy_count < self._sell_count:
            for i in range(0, self._sell_count - self._buy_count):
                idx = self._sell_index_list[-1]
                del self._sell_index_list[-1]
                self._trade_info_list[idx].removed = True
            self._sell_count = self._buy_count
        elif self._sell_count < self._buy_count:
            for i in range(0, self._buy_count - self._sell_count):
                idx = self._buy_index_list[-1]
                del self._buy_index_list[-1]
                self._trade_info_list[idx].removed = True
            self._buy_count = self._sell_count

    def save_result(self) -> None:
        self._task = asyncio.create_task(self._process())

    async def _process(self) -> None:
        if self._file_name:
            for item in self._trade_info_list:
                if item.removed:
                    continue
                await self._recorder.consume(self._file_name, item.to_tuple())
            await self._recorder.close(self._file_name)
        self.description()

    def description(self) -> None:
        sell_count = 0
        buy_count = 0
        net_profit = 0
        for item in self._trade_info_list:
            if item.removed:
                continue
            if item.side == 'buy':
                buy_count += 1
                net_profit -= item.avg_price * item.size
            else:
                sell_count += 1
                net_profit += item.avg_price * item.size
        print(f'sell: {sell_count}, buy: {buy_count}, profit: {net_profit}')
