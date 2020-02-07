from dataclasses import dataclass
from typing import Any, Dict, List

from app.config_loader import ConfigLoader


@dataclass
class IbConfig(object):
    cmd_redis_ip: str = 'localhost'
    cmd_redis_port: int = 6379
    ib_ip: str = '127.0.0.1'
    ib_port: int = 4002
    client_id: int = 3
    master_qq: int = 413707375
    default_contracts: List[Dict[str, Any]] = (
        {'secType': 'CMDTY',
         'symbol': 'XAGUSD',
         'exchange': 'SMART',
         'currency': 'USD'},
        {'secType': 'CMDTY',
         'symbol': 'XAUUSD',
         'exchange': 'SMART',
         'currency': 'USD'},
        {'secType': 'STK',
         'symbol': 'TSLA',
         'exchange': 'SMART',
         'currency': 'USD'}, )


async def loadConfig(config_file) -> IbConfig:
    config = IbConfig()
    config = await ConfigLoader.load(config_file, config)
    return config
