import asyncio
import aioredis
from typing import Callable, Dict, Set


class SubscribedItem(object):
    def __init__(self, channel: aioredis.Channel):
        self._channel = channel
        self._callbacks: Set[Callable] = set()
        self._task: asyncio.Task = None
        self._in_callbacks: bool = False

    def add_callback(self, callback: Callable):
        self._callbacks.add(callback)
        if self._task is None:
            self._task = asyncio.create_task(self._process())

    def remove_callback(self, callback: Callable):
        if callback in self._callbacks:
            self._callbacks.remove(callback)
        if len(self._callbacks) == 0 and self._task and not self._in_callbacks:
            self._task.cancel()

    def removeall_callbacks(self):
        self._callbacks.clear()
        if self._task and not self._in_callbacks:
            self._task.cancel()

    def is_finished(self) -> bool:
        return len(self._callbacks) == 0

    async def _process(self):
        while await self._channel.wait_message():
            msg = await self._channel.get()
            self._in_callbacks = True
            for callback in self._callbacks:
                await callback(msg)
            self._in_callbacks = False


class RedisHandler(object):
    prefix = ':r:'
    hash_prefix = ':r:h:'
    list_prefix = ':r:l:'
    string_prefix = ':r:s:'

    @staticmethod
    def is_basic_type(value):
        return type(value) in (float, int, complex, str, bytes, bool)

    @staticmethod
    def is_list_type(value):
        return type(value) in (list, tuple)

    @staticmethod
    def is_hash_type(value):
        return type(value) is dict or hasattr(value, '__dict__')

    @staticmethod
    def is_set_type(value):
        return type(value) is set

    @staticmethod
    def redis_type(key):
        if type(key) is not str:
            return None
        if key.startswith(RedisHandler.prefix) and len(key) > 5:
            return {'h': 'hash',
                    'l': 'list',
                    's': 'string'}.get(key[3])
        return None

    @staticmethod
    def get_redis_key(key, value):
        if key.startswith(RedisHandler.prefix):
            key = key[5:]
        if RedisHandler.is_basic_type(value):
            return RedisHandler.string_prefix + key
        elif RedisHandler.is_list_type(value):
            return RedisHandler.list_prefix + key
        elif RedisHandler.is_hash_type(value):
            return RedisHandler.hash_prefix + key
        return key

    @classmethod
    async def create(cls, ip: str = 'localhost', port: int = 6379):
        self = RedisHandler()
        self.conn = await aioredis.create_connection(
            (ip, port), encoding='utf-8')
        self.sub_conn = await aioredis.create_connection(
            (ip, port), encoding='utf-8')
        return self

    def __init__(self):
        self._subscribed_channels: Dict[str, SubscribedItem] = {}
        self._psubscribed_channels: Dict[str, SubscribedItem] = {}

    async def set(self, key, value):
        await self.remove(key)
        if self.is_basic_type(value):
            return await self._set_string(key, str(value))
        elif self.is_list_type(value):
            return await self._set_list(key, value)
        elif self.is_hash_type(value):
            return await self._set_hash(key, value)
        elif self.is_set_type(value):
            return await self._set_set(key, value)
        else:
            print('Error: unsupported value type: {}'.format(type(value)))
            return False

    async def _set_string(self, key, value):
        return await self.conn.execute('set', key, value) == 'OK'

    async def _set_list(self, key, value):
        rlist = []
        for i, v in enumerate(value):
            if self.is_basic_type(v):
                rlist.append(str(v))
            else:
                subkey = '{}:{}'.format(self.get_redis_key(key, v), i)
                rlist.append(subkey)
                res = await self.set(subkey, v)
        return await self.conn.execute('rpush', key, *rlist) == len(rlist)

    async def _set_hash(self, key, value):
        if hasattr(value, '__dict__'):
            value = value.__dict__
        pairs = []
        for k in value:
            if self.is_basic_type(value[k]):
                pairs.append(k)
                pairs.append(str(value[k]))
            else:
                subkey = '{}:{}'.format(self.get_redis_key(key, value[k]), k)
                pairs.append(k)
                pairs.append(subkey)
                if not await self.set(subkey, value[k]):
                    return False
        return await self.conn.execute('hmset', key, *pairs) == 'OK'

    async def _set_set(self, key, value):
        return await self.conn.execute('sadd', key, *value) == len(value)

    async def get(self, key):
        rtype = self.redis_type(key)
        if not rtype:
            rtype = await self.conn.execute('type', key)
        return await {'none': self._get_none,
                      'string': self._get_string,
                      'list': self._get_list,
                      'hash': self._get_hash,
                      'set': self._get_set}[rtype](key)

    async def _get_none(self, key):
        return None

    async def _get_string(self, key):
        return await self.conn.execute('get', key)

    async def _get_list(self, key):
        rlist = await self.conn.execute('lrange', key, 0, -1)
        for i, item in enumerate(rlist):
            if self.redis_type(item):
                rlist[i] = await self.get(item)
        return rlist

    async def _get_hash(self, key):
        rhash = await self.conn.execute('hgetall', key)
        obj = {}
        for index in range(1, len(rhash), 2):
            if self.redis_type(rhash[index]):
                obj[rhash[index - 1]] = await self.get(rhash[index])
            else:
                obj[rhash[index - 1]] = rhash[index]
        return obj

    async def _get_set(self, key):
        return set(await self.conn.execute('smembers', key))

    async def remove(self, key):
        rtype = self.redis_type(key)
        if not rtype:
            rtype = await self.conn.execute('type', key)
        await {'none': self._remove_none,
               'string': self._remove_string,
               'list': self._remove_list,
               'hash': self._remove_hash,
               'set': self._remove_set}[rtype](key)

    async def _remove_none(self, key):
        pass

    async def _remove_string(self, key):
        await self.conn.execute('del', key)

    async def _remove_list(self, key):
        rlist = await self.conn.execute('lrange', key, 0, -1)
        for item in rlist:
            if self.redis_type(item):
                await self.remove(item)
        await self.conn.execute('del', key)

    async def _remove_hash(self, key):
        rvalues = await self.conn.execute('hvals', key)
        for item in rvalues:
            if self.redis_type(item):
                await self.remove(item)
        await self.conn.execute('del', key)

    async def _remove_set(self, key):
        await self.conn.execute('del', key)

    async def get_keys(self, pattern):
        return await self.conn.execute('keys', pattern)

    async def publish(self, channel: str, message: bytes):
        return await self.conn.execute('publish', channel, message)

    async def subscribe(self, channel_name: str, callback: Callable):
        if channel_name in self._subscribed_channels:
            item = self._subscribed_channels[channel_name]
            item.add_callback(callback)
            return
        channel = aioredis.Channel(channel_name, is_pattern=False)
        await self.sub_conn.execute_pubsub('subscribe', channel)
        item = SubscribedItem(channel)
        item.add_callback(callback)
        self._subscribed_channels[channel_name] = item

    async def unsubscribe(self, channel_name: str, callback: Callable = None):
        if channel_name in self._subscribed_channels:
            item = self._subscribed_channels[channel_name]
            if callback:
                item.remove_callback(callback)
            else:
                item.removeall_callbacks()
            if item.is_finished():
                await self.sub_conn.execute_pubsub(
                    'unsubscribe', item._channel)
                self._subscribed_channels.pop(channel_name)

    async def psubscribe(self, channel_name: str, callback: Callable):
        if channel_name in self._psubscribed_channels:
            item = self._psubscribed_channels[channel_name]
            item.add_callback(callback)
            return
        channel = aioredis.Channel(channel_name, is_pattern=True)
        await self.sub_conn.execute_pubsub('psubscribe', channel)
        item = SubscribedItem(channel)
        item.add_callback(callback)
        self._psubscribed_channels[channel_name] = item

    async def punsubscribe(self, channel_name: str, callback: Callable = None):
        if channel_name in self._psubscribed_channels:
            item = self._psubscribed_channels[channel_name]
            if callback:
                item.remove_callback(callback)
            else:
                item.removeall_callbacks()
            if item.is_finished():
                await self.sub_conn.execute_pubsub(
                    'punsubscribe', item._channel)
                self._psubscribed_channels.pop(channel_name)


async def testother(msg):
    print('other channel {}'.format(msg))


async def testpsubscribe(msg):
    print(msg)


async def test():
    handler = await RedisHandler.create()
    # res = await handler.is_valid_user('admin', 'QuantAccount1233')
    # await handler.set('ttt', 'ggg')
    # a = {'ccc':1}
    # await handler.set_hash('aaa', a)
    # print(await handler.get_hash('aaa'))
    # print(res)

    class B(object):
        def __init__(self):
            self.a = 1
            self.b = 123
    b = B()
    print(await handler.set('ccc', b))
    # print(await handler.set('bbb', [1, 3.43, ['n', [{'a': 123, 'b': [1, 2, 3]}]], (9, 8)]))
    # print(await handler.get('bbb'))
    # print(await handler.set('aaa', {'a': [1, 2, 3], 'b': 'bbb', 'c': {'c1': 1, 'c2': 2}}))
    # print(await handler.set_dict('aaa', {'a': [1, 2, 3], 'b': 'bbb', 'c': {'c1': 1, 'c2': 2}}))
    # print(await handler.get('aaa'))
    # print(RedisHandler.redis_type(':r:n:aning'))
    # print(await handler.set('aaab', set((1, 2, 3, 4, 5))))
    # print(await handler.remove('aaa'))
    # await handler.set('aaa', bytes('abccc', 'utf-8'))
    print(await handler.get('ccc'))
    print(await handler.get_keys('huobi*'))
    await handler.publish('okex-tfff', '112233')
    await handler.subscribe('okex-test', testother)
    await handler.psubscribe('okex-*', testpsubscribe)
    await asyncio.sleep(10)
    await handler.unsubscribe('okex-test')
    await handler.punsubscribe('okex-*')
    await asyncio.sleep(10)


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(test())
