from random import randint

from app.trader.strategy_base import MarketData, StrategyBase, TradeManagerBase


kOrderTimeout = 10 * 60 * 1000


class RandomStrategy(StrategyBase):
    def __init__(self, strategy_id: int, manager: TradeManagerBase) -> None:
        StrategyBase.__init__(self, strategy_id, manager)
        self._pending = False
        self._last_order_ts = 0
        self._trade_pair = []
        self._default_amount = 1000
        self._order_id: int = 0

    def _trade_side(self, side: int) -> str:
        return {0: 'buy', 1: 'sell'}[side]

    def on_market_data(self, data: MarketData) -> None:
        if self._pending:
            if data.ts > self._last_order_ts + kOrderTimeout:
                self._manager.cancel_order(self._order_id)
            return
        op_id = randint(0, 5)
        if op_id != 0:
            return
        if len(self._trade_pair) == 0:
            first_side = randint(0, 1)
            self._trade_pair.append(first_side)
            side = self._trade_side(first_side)
        else:
            second_side = 1 - self._trade_pair[0]
            self._trade_pair.append(second_side)
            side = self._trade_side(second_side)
        price = data.asks_price[0] - 0.005 \
            if side == 'buy' else data.bids_price[0] + 0.005
        self._pending = True
        self._last_order_ts = data.ts
        self._order_id = self._manager.place_order(
            self._sid, side, price, self._default_amount)

    def on_commission(self, order_id: int, value: float) -> None:
        if self._order_id != order_id:
            return
        self._pending = False
        if len(self._trade_pair) == 2:
            self._trade_pair.clear()

    def on_partial_filled(self, order_id: int,
                          filled_amount: int, filled_value: float) -> None:
        pass

    def on_canceled(self, order_id: int) -> None:
        if self._order_id != order_id:
            return
        self._pending = False
        if len(self._trade_pair) > 0:
            del self._trade_pair[-1]

    def on_reset(self) -> None:
        self._pending = False
        self._trade_pair.clear()
