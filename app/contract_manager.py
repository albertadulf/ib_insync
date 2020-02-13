import asyncio
import aiofiles
import os

from dataclasses import dataclass
from typing import Dict, List
from ib_insync import Contract


@dataclass
class ContractItem(object):
    alias: str
    sec_type: str
    currency: str
    contract: Contract

    def __init__(self, alias: str, sec_type: str, currency: str) -> None:
        self.alias = alias.upper()
        self.sec_type = sec_type
        self.currency = currency
        self.contract = Contract.create(
            secType=sec_type, symbol=alias,
            exchange='SMART', currency=currency)

    @staticmethod
    def generate_item(line):
        props = line.split(',')
        if len(props) != 3:
            return None
        item = ContractItem(*props)
        return item

    def pack(self) -> str:
        return f'{self.alias},{self.sec_type},{self.currency}'

    def __str__(self) -> str:
        return f'alias:{self.alias},type:{self.sec_type},' + \
            f'currency:{self.currency}'

    def __repr__(self) -> str:
        return self.__str__()


class ContractManager(object):
    def __init__(self, contract_file: str = ''):
        if contract_file == '' or not os.path.exists(contract_file):
            contract_file = 'configs/contracts.dat'
        self._contract_file: str = contract_file
        self._contracts: Dict[str, ContractItem] = {}
        self._write_back_task: asyncio.Task = None

    def _set_dirty(self) -> None:
        if self._write_back_task is None:
            self._write_back_task = asyncio.create_task(self._write_back())

    async def _write_back(self) -> None:
        async with aiofiles.open(self._contract_file, 'w') as f:
            await f.write('\n'.join(
                [item.pack() for item in self._contracts.values()]))
        self._write_back_task = None

    def _add_contract_item(self, item: ContractItem) -> bool:
        if item.alias is not None and \
           item.alias != "" and item.alias not in self._contracts:
            self._contracts[item.alias] = item
            return True
        return False

    def get_contract(self, alias: str) -> Contract:
        alias = alias.upper()
        if alias in self._contracts:
            return self._contracts[alias].contract
        return None

    def get_available_contracts(self) -> List[str]:
        return self._contracts.keys()

    def add_contract(self, alias: str, sec_type: str = 'STK',
                     currency: str = 'USD') -> None:
        alias = alias.upper()
        if self._add_contract_item(ContractItem.generate_item(
                f'{alias},{sec_type},{currency}')):
            self._set_dirty()

    def remove_contract(self, alias: str) -> None:
        alias = alias.upper()
        if alias in self._contracts:
            self._contracts.pop(alias)
            self._set_dirty()

    async def initialize(self) -> None:
        async with aiofiles.open(self._contract_file) as f:
            contents = await f.read()
            lines = contents.split('\n')
            for line in lines:
                item = ContractItem.generate_item(line)
                if item is None:
                    continue
                self._add_contract_item(item)


async def test():
    manager = ContractManager()
    await manager.initialize()
    print(manager.get_available_contracts())

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(test())
