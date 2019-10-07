from app.trader.strategy_base import MarketData, StrategyBase, TradeManagerBase


kMinIntervals = 300 * 1000
kMeanDuration = 60 * 1000
kDeltaThreshold = 0.01
kOrderTimeout = 10 * 60 * 1000


class MeanStrategy(StrategyBase):
    def __init__(self, strategy_id: int, manager: TradeManagerBase) -> None:
        StrategyBase.__init__(self, strategy_id, manager)
        self._amount = 1000
        self._pending = False
        self._pending_side = ''
        self._order_id = 0
        self._last_success_side = ''
        self._data_list = []
        self._last_data_ts = 0
        self._last_order_ts = 0

    def can_buy(self) -> None:
        return self._last_success_side == '' \
            or self._last_success_side == 'sell'

    def can_sell(self) -> None:
        return self._last_success_side == '' \
            or self._last_success_side == 'buy'

    def on_market_data(self, data: MarketData) -> None:
        if len(self._data_list) > 0:
            gap = data.ts - self._data_list[-1][0]
            self._data_list[-1].append(gap)
        self._data_list.append(
            [data.ts, data.bids_price[0], data.asks_price[0],
             (data.bids_price[0] + data.asks_price[0]) / 2])
        if self._pending:
            return
        if data.ts < self._data_list[0][0] + kMinIntervals:
            return
        if data.asks_amount[0] < self._amount \
           or data.bids_amount[0] < self._amount:
            return
        total_duration = 0
        total_value = 0
        for item in self._data_list[-2::-1]:
            total_value += item[3] * item[4]
            total_duration += item[4]
            if item[0] + kMeanDuration < data.ts:
                break
        m = total_value / total_duration
        price = self._data_list[-1][3]
        if self.can_sell() and price >= m + kDeltaThreshold:
            self._pending = True
            self._pending_side = 'sell'
            print(f'place sell order: {price}')
            self._order_id = self._manager.place_order(
                self._sid, 'sell', price, self._amount)
            self._last_order_ts = data.ts
        elif self.can_buy() and price <= m - kDeltaThreshold:
            self._pending = True
            self._pending_side = 'buy'
            print(f'place buy order: {price}')
            self._order_id = self._manager.place_order(
                self._sid, 'buy', price, self._amount)
            self._last_order_ts = data.ts

    def on_commission(self, order_id: int, value: float) -> None:
        if self._order_id != order_id:
            return
        self._pending = False
        self._last_success_side = self._pending_side

    def on_partial_filled(self, order_id: int,
                          filled_amount: int, filled_value: float) -> None:
        pass

    def on_canceled(self, order_id: int) -> None:
        if self._order_id != order_id:
            return
        self._pending = False

    def on_reset(self) -> None:
        self._data_list = []
        self._pending = False
        self._pending_side = ''
        self._order_id = 0
        self._last_success_side = ''
