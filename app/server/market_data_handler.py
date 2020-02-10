import asyncio
import math
from typing import Any

from app.dispatcher import Dispatcher
from app.redis.redis_client import RedisHandler
from app.server.protocols import (
    kDataChannel,
    Transporter,
    MarketData,
)
from ib_insync import IB


class MarketDataHandler(object):
    def __init__(self, ib: IB, data_subscriber: bool,
                 redis: RedisHandler) -> None:
        self._ib = ib
        self._data_subscriber = data_subscriber
        self._redis: RedisHandler = redis
        self._transporter: Transporter = Transporter()
        self._dispatcher: Dispatcher = Dispatcher()
        if self._data_subscriber:
            self._ib.pendingTickersEvent += self.broadcast_market_data
            self._queue: asyncio.Queue = asyncio.Queue()
            self._task: asyncio.Task = asyncio.create_task(
                self._deliver_data())
        else:
            self._dispatcher.add_dispatcher(MarketData, self.on_market_data)

    async def _deliver_data(self):
        while True:
            data = await self._queue.get()
            await self._redis.publish(kDataChannel, data)

    async def initialize(self) -> None:
        if not self._data_subscriber:
            await self._redis.subscribe(kDataChannel, self.on_data)

    def broadcast_market_data(self, tickers: Any) -> None:
        for ticker in tickers:
            market_data = MarketData()
            market_data.alias = ticker.contract.symbol
            market_data.ts = ticker.time.timestamp()
            if len(ticker.domBids) == 0:
                if math.isnan(ticker.bid):
                    continue
                market_data.bid_prices = [ticker.bid]
                market_data.bid_sizes = [ticker.bidSize]
                market_data.ask_prices = [ticker.ask]
                market_data.ask_sizes = [ticker.askSize]
            else:
                market_data.bid_prices = [bid.price for bid in ticker.domBids]
                market_data.bid_sizes = [bid.size for bid in ticker.domBids]
                market_data.ask_prices = [ask.price for ask in ticker.domAsks]
                market_data.ask_sizes = [ask.size for ask in ticker.domAsks]
            data = Transporter.serialize(market_data.pack())
            print(data)
            self._queue.put_nowait(data)

    async def on_data(self, message: bytes) -> None:
        messages = self._transporter.deserialize(message)
        for data in messages:
            await self._dispatcher.on_message(data)

    async def on_market_data(self, data: MarketData):
        print(data)
