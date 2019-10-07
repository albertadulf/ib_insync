import asyncio
from dataclasses import dataclass
from typing import Any, Dict, Tuple
import os

from app.recorder.recorder import Recorder
from app.recorder.strategy_recorder import StrategyRecorder
from app.trader.market_file_loader import MarketFileLoader
from app.trader.mean_strategy import MeanStrategy
from app.trader.strategy_base import MarketData, StrategyBase
from app.trader.strategy_manager import Strategies
from app.trader.trade_manager_base import TradeManagerBase
from app.utils.log import Log


kResetInterval = 60 * 1000


@dataclass
class OrderItem(object):
    side: str = ''
    price: float = 0.0
    size: int = 0
    filled_value: float = 0.0
    filled_size: int = 0


class MockTradeManager(TradeManagerBase):
    def __init__(self, recorder: Recorder):
        self._recorder: Recorder = recorder
        self._strategy: StrategyBase = None
        self._available_order_id: int = 1
        self._pending_orders: Dict[int, OrderItem] = {}
        self._last_feed_ts: int = 0
        self._loader: MarketFileLoader = None
        self._strategy_recorder: StrategyRecorder = None

    def test_strategy(
            self, name: str, data_file: str, result_file: str = '') -> str:
        if name not in Strategies.keys():
            return f'no strategy {name} to test'
        if not os.path.exists(data_file):
            return f'{data_file} not exists'
        self._strategy = Strategies[name](0, self)
        self.reset()
        if self._loader:
            self._loader.stop()
        if self._strategy_recorder:
            self._strategy_recorder.destroy()
        self._strategy_recorder = StrategyRecorder(self._recorder, result_file)
        self._loader = MarketFileLoader(data_file, self)
        return f'start to run mock strategy {name} with {data_file}'

    def reset(self) -> None:
        if self._strategy:
            self._strategy.on_reset()
        self._available_order_id = 1
        self._pending_orders.clear()
        self._last_feed_ts = 0

    def _get_available_order_id(self) -> int:
        order_id = self._available_order_id
        self._available_order_id += 1
        return order_id

    def place_order(
            self, strategy_id: int, side: str, price: float, size: int) -> int:
        order_id = self._get_available_order_id()
        self._pending_orders[order_id] = OrderItem(side, price, size)
        return order_id

    def cancel_order(self, order_id: int) -> None:
        if order_id not in self._pending_orders:
            return
        self._pending_orders.pop(order_id)
        self._strategy.on_canceled(order_id)

    def transaction(
            self, item: OrderItem, data: MarketData) -> Tuple:
        success_size = 0
        success_value = 0
        if item.side == 'buy':
            for i in range(0, len(data.asks_price)):
                ask_price = data.asks_price[i]
                ask_amount = data.asks_amount[i]
                if item.price >= ask_price:
                    size = min(item.size, ask_amount)
                    item.size -= size
                    success_size += size
                    success_value += size * ask_price
                else:
                    break
                if item.size == 0:
                    break
            return (success_size, success_value)
        else:
            for i in range(0, len(data.bids_price)):
                bid_price = data.bids_price[i]
                bid_amount = data.bids_amount[i]
                if item.price <= bid_price:
                    size = min(item.size, bid_amount)
                    item.size -= size
                    success_size += size
                    success_value += size * bid_price
                else:
                    break
                if item.size == 0:
                    break
            return (success_size, success_value)

    def feed_data(self, data: MarketData) -> None:
        if len(self._pending_orders) > 0:
            remove_list = []
            for order_id, item in self._pending_orders.items():
                success_size, success_value = self.transaction(item, data)
                item.filled_size += success_size
                item.filled_value += success_value
                if success_size > 0:
                    if item.size == 0:
                        self._strategy_recorder.add_trade_info(
                            data.ts, item.side,
                            item.filled_size, item.filled_value)
                        remove_list.append(order_id)
                        self._strategy.on_commission(order_id, success_value)
                    else:
                        self._strategy.on_partial_filled(
                            order_id, success_size, success_value)
            for order_id in remove_list:
                self._pending_orders.pop(order_id)
        if self._last_feed_ts != 0 and \
           self._last_feed_ts + kResetInterval < data.ts:
            self.reset()
            self._strategy_recorder.polish_result()
        self._strategy.on_market_data(data)
        self._last_feed_ts = data.ts

    def feed_end(self) -> None:
        self._strategy_recorder.polish_result()
        self._strategy_recorder.save_result()
        self.reset()


async def test():
    recorder = Recorder(Log.create(Log.path('test')))
    manager = MockTradeManager(recorder)
    strategy = MeanStrategy(0, manager)
    manager.test_strategy(
        strategy, 'app/data/XAGUSD_5_2019-10-02.csv', 'XAGUSD_1002_random')

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(test())
    asyncio.get_event_loop().run_forever()
