from typing import Callable

from app.server.ib_manager import IbManager
from app.server.protocols import ProtocolBase


class BaseServer(object):
    def ib_manager(self) -> IbManager:
        return None

    def valid_client(self, sid: str) -> bool:
        return True

    def touch(self, sid: str) -> None:
        pass

    async def send_packet_by_sid(self, sid: str, packet: ProtocolBase) -> None:
        pass

    def add_dispatcher(
            self, protocol: ProtocolBase, handler: Callable) -> None:
        pass
