from dataclasses import dataclass
from typing import Tuple

from app.trader.trade_manager_base import TradeManagerBase


@dataclass
class MarketData(object):
    ts: int = 0
    bids_price: Tuple[float] = tuple()
    bids_amount: Tuple[int] = tuple()
    asks_price: Tuple[float] = tuple()
    asks_amount: Tuple[int] = tuple()


class StrategyBase(object):
    def __init__(self, strategy_id: int, manager: TradeManagerBase) -> None:
        self._sid = strategy_id
        self._manager = manager

    def on_market_data(self, data: MarketData) -> None:
        pass

    def on_commission(self, order_id: int, value: float) -> None:
        pass

    def on_partial_filled(self, order_id: int,
                          filled_amount: int, filled_value: float) -> None:
        pass

    def on_canceled(self, order_id: int) -> None:
        pass

    def on_reset(self) -> None:
        pass
