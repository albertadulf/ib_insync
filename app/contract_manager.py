import asyncio
import aiofiles
import os

from dataclasses import dataclass
from typing import Dict, List
from ib_insync import Contract


@dataclass
class ContractItem(object):
    alias: str
    con_id: int
    sec_type: str
    contract: Contract

    def __init__(self, alias: str, con_id: int, sec_type: str) -> None:
        self.alias = alias.upper()
        self.con_id = con_id
        self.sec_type = sec_type
        self.contract = Contract.create(conId=con_id, secType=sec_type)

    @staticmethod
    def generate_item(line):
        props = line.split(',')
        if len(props) != 3:
            return None
        item = ContractItem(*props)
        return item

    def pack(self) -> str:
        return f'{self.alias},{self.con_id},{self.sec_type}'

    def __str__(self) -> str:
        return f'alias:{self.alias},id:{self.con_id},type:{self.sec_type}'

    def __repr__(self) -> str:
        return self.__str__()


class ContractManager(object):
    def __init__(self, contract_file: str = ''):
        if contract_file == '' or not os.path.exists(contract_file):
            contract_file = 'configs/contracts.dat'
        self._contract_file: str = contract_file
        self._contracts: Dict[str, ContractItem] = {}
        self._write_back_task: asyncio.Task = None
        self._initialized: bool = False

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

    def add_contract(self, alias: str, con_id: int, sec_type: str) -> None:
        if self._add_contract_item(ContractItem.generate_item(
                f'{alias},{con_id},{sec_type}')):
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
        self._initialized = True

    def initialized(self) -> bool:
        return self._initialized


async def test():
    manager = ContractManager()
    await manager.initialize()
    print(manager.get_available_contracts())

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(test())
