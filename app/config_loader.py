import asyncio
import aiofiles
import json
import os


class ConfigLoader(object):
    @staticmethod
    async def load(config_file, default_object):
        if not os.path.exists(config_file):
            return default_object
        async with aiofiles.open(config_file) as f:
            contents = await f.read()
            obj = json.loads(contents)
            for key, value in obj.items():
                default_object.__dict__[key] = value
        return default_object


async def test():
    class Tester(object):
        def __init__(self):
            self.redis_ip = 'localhost'
    tester = Tester()
    config = await ConfigLoader.load('core/config1.json', tester)
    print(config.redis_ip)
    # print(config.redis_port)


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(test())
