import aiofiles
import asyncio
import datetime
from typing import Dict, Tuple

from app.utils.common_util import tick_ms
from app.utils.log import Log


kFilePath = 'app/data/'
kFileExpireInterval = 10 * 60 * 1000


class FileItem(object):
    def generate_file_name(self):
        self._filename = '{}{}_{}.csv'.format(
            kFilePath, self._name, self._date)

    def __init__(self, name: str, date: str):
        self._name = name
        self._date = date
        self.generate_file_name()
        self._file = None
        self._last_active_ts = tick_ms()

    async def consume(self, values: Tuple) -> None:
        if self._file is None:
            self._file = await aiofiles.open(self._filename, 'a')
        self._last_active_ts = tick_ms()
        record = ','.join([str(value) for value in values]) + '\n'
        await self._file.write(record)
        await self._file.flush()

    async def close(self) -> None:
        if self._file is not None:
            await self._file.close()
            self._file = None

    async def update_date(self, date: str) -> None:
        if self._file is not None:
            await self._file.close()
            self._file = None
        self._date = date
        self.generate_file_name()

    def is_deprecated(self, now: int) -> bool:
        if now > self._last_active_ts + kFileExpireInterval:
            return True
        return False


class Recorder(object):
    def __init__(self, log: Log) -> None:
        self._logger = log.get_logger('recorder')
        self._date = str(datetime.date.today())
        self._timer_task = asyncio.create_task(self._timer())
        self._file_items: Dict[str, FileItem] = {}

    async def _timer(self) -> None:
        while True:
            await asyncio.sleep(60 * 10)
            date = str(datetime.date.today())
            if date != self._date:
                self._logger.info(
                    'update date from %s to %s', self._date, date)
                self._date = date
                for item in self._file_items.values():
                    await item.update_date(date)
            deprecated_files = []
            now = tick_ms()
            for f, item in self._file_items.items():
                if item.is_deprecated(now):
                    self._logger.info('close deprecated file: %s',
                                      item._filename)
                    deprecated_files.append(f)
                    await item.close()
            for f in deprecated_files:
                self._file_items.pop(f)

    def get_file(self, name: str) -> FileItem:
        if name in self._file_items.keys():
            return self._file_items[name]
        item = FileItem(name, self._date)
        self._file_items[name] = item
        self._logger.info('create file: %s', item._filename)
        return item

    async def consume(self, name: str, values: Tuple) -> None:
        item = self.get_file(name)
        await item.consume(values)

    async def close(self, name: str) -> None:
        if name not in self._file_items.keys():
            return
        await self._file_items[name].close()
        self._file_items.pop(name)
