class TradeManagerBase(object):
    def place_order(
            self, strategy_id: int, side: str, price: float, size: int) -> int:
        return 0

    def cancel_order(self, order_id: int) -> None:
        pass
