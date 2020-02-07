import asyncio
from typing import Any

from app.recorder.recorder import Recorder
from ib_insync import IB


class MarketRecorder(object):
    def __init__(self, ib: IB, recorder: Recorder) -> None:
        self._ib = ib
        self._recorder = recorder
        self._ib.pendingTickersEvent += self.on_market_data
        self._queue: asyncio.Queue = asyncio.Queue()
        self._task: asyncio.Task = asyncio.create_task(self._process())

    async def _process(self):
        while True:
            file_name, *data = await self._queue.get()
            await self._recorder.consume(file_name, data)

    def on_market_data(self, tickers: Any) -> None:
        for ticker in tickers:
            symbol = ticker.contract.symbol
            file_name = f'{symbol}'
            data = [file_name, ticker.time.timestamp()]
            depth = min(len(ticker.domBids), len(ticker.domAsks))
            if depth == 0:
                data.append(ticker.bid)
                data.append(ticker.bidSize)
                data.append(ticker.ask)
                data.append(ticker.askSize)
            else:
                for i in range(0, depth):
                    data.append(ticker.domBids[i].price)
                    data.append(ticker.domBids[i].size)
                    data.append(ticker.domAsks[i].price)
                    data.append(ticker.domAsks[i].size)
            print(data)
            self._queue.put_nowait(tuple(data))
