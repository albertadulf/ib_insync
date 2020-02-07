import asyncio
from typing import List

from app.contract_manager import ContractManager
from app.recorder.account_recorder import AccountRecorder
from app.recorder.market_recorder import MarketRecorder
from app.recorder.recorder import Recorder
from app.utils.log import Log
from ib_insync import Contract, IB, LimitOrder, Order, Trade


class IbManager(object):
    log_file = 'ib_manager'

    def __init__(self, ip: str, port: int,
                 client_id: int, contract_manager: ContractManager):
        self._ib = IB()
        self._ib_ip: str = ip
        self._ib_port: int = port
        self._client_id: int = client_id
        self._contract_manager: ContractManager = contract_manager
        self._subscribed_mkt_contracts: List[str] = []
        self._subscribed_mkt_depth_contracts: List[str] = []
        self._log: Log = Log.create(Log.path(self.log_file))
        self._logger = self._log.get_logger('ibmanager')
        self._recorder: Recorder = Recorder(self._log)
        self._market_recorder: MarketRecorder = MarketRecorder(
            self._ib, self._recorder)
        self._account_recorder: AccountRecorder = AccountRecorder(
            self._ib, self._recorder)
        self._keep_connection_task: asyncio.Task = None
        self._ib.connectedEvent += self.on_ib_connected
        self._ib.disconnectedEvent += self.on_ib_disconnected
        self._reconnect_flag: bool = False

    def on_ib_connected(self) -> None:
        self._logger.info('connected with ib')
        self._reconnect_flag = False
        self._recover_subscriptions()

    def on_ib_disconnected(self) -> None:
        self._logger.warning('disconnected with ib')
        self._reconnect_flag = True
        if self._keep_connection_task is None:
            self._keep_connection_task = asyncio.create_task(self._reconnect())

    async def _reconnect(self) -> None:
        while self._reconnect_flag:
            await asyncio.sleep(20)
            self._logger.info('try to reconnect ib gateway')
            await self.initialize()
        self._keep_connection_task = None

    def _recover_subscriptions(self) -> None:
        for contract in self._subscribed_mkt_contracts:
            self._logger.info(f'recover subscribe {str(contract)}')
            self._ib.reqMktData(contract)
        for contract in self._subscribed_mkt_depth_contracts:
            self._logger.info(f'recover subscribe depth {str(contract)}')
            self._ib.reqMktDepth(contract)

    async def initialize(self):
        if self._ib.isConnected():
            return
        try:
            await self._ib.connectAsync(
                self._ib_ip, self._ib_port, clientId=self._client_id)
            accounts = self._ib.managedAccounts()
            if len(accounts) > 0:
                self._account_recorder.update_account(accounts[0])
                self.update_account()
        except Exception:
            pass

    def update_account(self):
        self._ib.reqAccountSummaryAsync()

    async def find_symbols(self, pattern: str) -> List[str]:
        symbols = await self._ib.reqMatchingSymbolsAsync(pattern)
        contracts = [symbol.contract.nonDefaults() for symbol in symbols]
        return contracts

    def get_contracts(self) -> str:
        return str([alias for alias
                    in self._contract_manager.get_available_contracts()])

    def add_contract(self, *args) -> str:
        if len(args) < 3:
            return 'invalid arguments'
        self._contract_manager.add_contract(*args)
        return f'successfully add contract {args}'

    def get_contract(self, alias: str) -> Contract:
        return self._contract_manager.get_contract(alias)

    def make_contract(self, **kwargs) -> Contract:
        return Contract.create(**kwargs)

    def sub_market(self, alias: str) -> str:
        if alias == "":
            return "empty symbol"
        alias = alias.upper()
        if alias in self._subscribed_mkt_contracts:
            return f'already subscribe {alias}'
        contract = self._contract_manager.get_contract(alias)
        if contract is None:
            return f'subscribe failed, no such symbol: {alias}'
        self._subscribed_mkt_contracts.append(alias)
        self._ib.reqMktData(contract)
        return f'subscribe {alias} success'

    def unsub_market(self, alias: str) -> str:
        alias = alias.upper()
        if alias not in self._subscribed_mkt_contracts:
            return f'not ever subscribe {alias}'
        contract = self._contract_manager.get_contract(alias)
        self._ib.cancelMktData(contract)
        self._subscribed_mkt_contracts.remove(alias)
        return f'unsubscribe {alias} success'

    def sub_market_depth(self, alias: str) -> str:
        if alias == "":
            return "empty symbol"
        alias = alias.upper()
        if alias in self._subscribed_mkt_depth_contracts:
            return f'already subscribe depth {alias}'
        contract = self._contract_manager.get_contract(alias)
        if contract is None:
            return f'subscribe depth failed, no such symbol: {alias}'
        self._subscribed_mkt_depth_contracts.append(alias)
        self._ib.reqMktDepth(contract)
        return f'subscribe depth {alias} success'

    def unsub_market_depth(self, alias: str) -> str:
        alias = alias.upper()
        if alias not in self._subscribed_mkt_depth_contracts:
            return f'not ever subscribe depth {alias}'
        contract = self._contract_manager.get_contract(alias)
        self._ib.cancelMktDepth(contract)
        self._subscribed_mkt_depth_contracts.remove(alias)
        return f'unsubscribe depth {alias} success'

    def place_order(
            self, alias: str, side: str,
            size: int, price: float) -> str:
        alias = alias.upper()
        contract = self._contract_manager.get_contract(alias)
        if contract is None:
            return f'order failed: no symbol {alias}'
        trade = self._place_order(contract, side, size, price)
        return str(trade)

    def _place_order(
            self, contract: Contract, side: str,
            size: int, price: float) -> Trade:
        side = side.upper()
        if side not in ('SELL', 'BUY'):
            return [f'invalid order type: {side}']
        price = float(f'{round(float(price), 3):.3f}')
        order = LimitOrder(side, size, price, tif='GTC')
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
