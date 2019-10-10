import aiofiles
import asyncio
from typing import Any

from app.trader.strategy_base import MarketData


class MarketFileLoader(object):
    def __init__(self, filename: str, observer: Any) -> None:
        self._filename = filename
        self._observer = observer
        self._task = asyncio.create_task(self._process2())

    def stop(self) -> None:
        self._task.cancel()

    async def _process2(self) -> None:
        async with aiofiles.open(self._filename, mode='r') as f:
            async for line in f:
                t = line.strip().split(',')
                ts = int(float(t.pop(0)) * 1000)
                bids_price = []
                bids_amount = []
                asks_price = []
                asks_amount = []
                for i, data in enumerate(t):
                    if i % 4 == 0:
                        bids_price.append(float(data))
                    elif i % 4 == 1:
                        bids_amount.append(int(data))
                    elif i % 4 == 2:
                        asks_price.append(float(data))
                    else:
                        asks_amount.append(int(data))
                data = MarketData(ts, tuple(bids_price), tuple(bids_amount),
                                  tuple(asks_price), tuple(asks_amount))
                self._observer.feed_data(data)

    async def _process(self) -> None:
        async with aiofiles.open(self._filename, mode='r') as f:
            async for line in f:
                t = line.strip().split(',')
                if len(t) < 21:
                    continue
                ts = int(float(t.pop(0)) * 1000)
                bids_price = [float(v) for i, v in enumerate(t)
                              if i % 2 == 0 and i < 10]
                bids_amount = [int(v) for i, v in enumerate(t)
                               if i % 2 == 1 and i < 10]
                asks_price = [float(v) for i, v in enumerate(t)
                              if i % 2 == 0 and i >= 10]
                asks_amount = [int(v) for i, v in enumerate(t)
                               if i % 2 == 1 and i >= 10]
                data = MarketData(ts, tuple(bids_price), tuple(bids_amount),
                                  tuple(asks_price), tuple(asks_amount))
                self._observer.feed_data(data)
        self._observer.feed_end()


async def test():
    class TestObserver(object):
        def __init__(self):
            self._print_times = 0
            self._lines = 0

        def feed_data(self, data: MarketData) -> None:
            self._lines += 1
            if self._print_times >= 10:
                return
            self._print_times += 1
            print(f'ts: {data.ts}, bid_price: {data.bids_price}, '
                  + f'bid_size: {data.bids_amount}, '
                  + f'ask_price: {data.asks_price}, '
                  + f'ask_size: {data.asks_amount}')

        def feed_end(self) -> None:
            print(f'finished: {self._lines} lines')

    observer = TestObserver()
    data_file = 'app/data/XAGUSD_5_2019-10-02.csv'
    loader = MarketFileLoader(data_file, observer)


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(test())
    asyncio.get_event_loop().run_forever()
