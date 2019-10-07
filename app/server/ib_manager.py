from typing import List

from app.recorder.account_recorder import AccountRecorder
from app.recorder.market_recorder import MarketRecorder
from app.recorder.recorder import Recorder
from app.utils.log import Log
from ib_insync import Contract, IB, LimitOrder, Order, Trade


class IbManager(object):
    log_file = 'ib_manager'

    def __init__(self, ip: str, port: int, client_id: int):
        self._ib = IB()
        self._ib_ip: str = ip
        self._ib_port: int = port
        self._client_id: int = client_id
        self._subscribed_mkt_contracts: List[str] = []
        self._subscribed_mkt_depth_contracts: List[str] = []
        self._log: Log = Log.create(Log.path(self.log_file))
        self._recorder: Recorder = Recorder(self._log)
        self._market_recorder: MarketRecorder = MarketRecorder(
            self._ib, self._recorder)
        self._account_recorder: AccountRecorder = AccountRecorder(
            self._ib, self._recorder)

    async def initialize(self):
        await self._ib.connectAsync(
            self._ib_ip, self._ib_port, clientId=self._client_id)
        accounts = self._ib.managedAccounts()
        if len(accounts) > 0:
            self._account_recorder.update_account(accounts[0])
        self.update_account()

    def update_account(self):
        self._ib.reqAccountSummaryAsync()

    async def find_symbols(self, pattern: str) -> List[str]:
        symbols = await self._ib.reqMatchingSymbolsAsync(pattern)
        contracts = [symbol.contract.nonDefaults() for symbol in symbols]
        return contracts

    def make_contract(self, **kwargs) -> Contract:
        return Contract.create(**kwargs)

    def sub_market(self, contract: Contract) -> str:
        self._ib.reqMktData(contract)
        return 'subscribe {} success'.format(str(contract))

    def unsub_market(self, contract: Contract) -> str:
        self._ib.cancelMktData(contract)
        return 'unsubscribe {} success'.format(str(contract))

    def sub_market_depth(self, contract: Contract) -> str:
        self._ib.reqMktDepth(contract)
        return 'subscribe depth {} success'.format(str(contract))

    def unsub_market_depth(self, contract: Contract) -> str:
        self._ib.cancelMktDepth(contract)
        return 'unsubscribe depth {} success'.format(str(contract))

    def place_order(
            self, contract: Contract, side: str,
            size: int, price: float) -> str:
        trade = self._place_order(contract, side, size, price)
        return str(trade)

    def _place_order(
            self, contract: Contract, side: str,
            size: int, price: float) -> Trade:
        side = side.upper()
        if side not in ('SELL', 'BUY'):
            return [f'invalid order type: {side}']
        price = round(price, 4)
        order = LimitOrder(side, size, price)
        trade = self._ib.placeOrder(contract, order)
        return trade

    def cancel_order(self, order_id: int) -> str:
        order_id = int(order_id)
        order = Order(orderId=order_id)
        trade = self._ib.cancelOrder(order)
        return str(trade)

    async def orders(self) -> List[str]:
        orders = await self._ib.reqOpenOrdersAsync()
        return [str(order) for order in orders]

    def portfolio(self) -> List[str]:
        results = self._ib.portfolio()
        return [str(value) for value in results]
