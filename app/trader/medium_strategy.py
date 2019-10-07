from app.trader.strategy_base import MarketData, StrategyBase, TradeManagerBase


kMinIntervals = 300 * 1000
kMeanDuration = 90 * 1000
kThreshold = 0.01
kOrderTimeout = 10 * 60 * 1000


class MediumStrategy(StrategyBase):
    def __init__(self, strategy_id: int, manager: TradeManagerBase) -> None:
        StrategyBase.__init__(self, strategy_id, manager)
        self._amount = 1000
        self._pending = False
        self._buy_order_id = 0
        self._sell_order_id = 0
        self._data_list = []
        self._last_data_ts = 0
        self._last_order_ts = 0

    def on_market_data(self, data: MarketData) -> None:
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
        price = self._data_list[-1][3]
        max_value = price
        min_value = price
        for item in self._data_list[-2::-1]:
            if max_value < item[3]:
                max_value = item[3]
            if min_value > item[3]:
                min_value = item[3]
            if item[0] + kMeanDuration < data.ts:
                break
        if price + kThreshold > max_value \
           or price - kThreshold < min_value:
            return
        sell_price = (max_value + price) / 2
        buy_price = (price + min_value) / 2
        self._pending = True
        self._last_order_ts = data.ts
        self._buy_order_id = self._manager.place_order(
            self._sid, 'buy', buy_price, self._amount)
        self._sell_order_id = self._manager.place_order(
            self._sid, 'sell', sell_price, self._amount)
        print(f'place order buy: {buy_price}, sell: {sell_price}')

    def on_commission(self, order_id: int, value: float) -> None:
        if self._buy_order_id != order_id \
           and self._sell_order_id != order_id:
            return
        if self._buy_order_id == order_id:
            self._buy_order_id = -1
        if self._sell_order_id == order_id:
            self._sell_order_id = -1
        if self._sell_order_id == -1 and self._buy_order_id == -1:
            self._pending = False

    def on_partial_filled(self, order_id: int,
                          filled_amount: int, filled_value: float) -> None:
        pass

    def on_canceled(self, order_id: int) -> None:
        if self._buy_order_id != order_id \
           and self._sell_order_id != order_id:
            return
        if self._buy_order_id == order_id:
            self._buy_order_id = -1
        if self._sell_order_id == order_id:
            self._sell_order_id = -1
        if self._sell_order_id == -1 and self._buy_order_id == -1:
            self._pending = False

    def on_reset(self) -> None:
        self._data_list = []
        self._pending = False
        self._sell_order_id = -1
        self._buy_order_id = -1
