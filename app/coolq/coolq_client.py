import asyncio
import json
import nonebot
from os import path
from typing import Optional


from app.client_base import ClientBase
from app.ib_config import IbConfig, loadConfig
import app.coolq.config as coolq_config
from app.server.protocols import (
    kWorkerTypeConsole,
    ResponseStatus,
    ConsoleCommandRequest,
    ConsoleCommandResponse,
)
from app.utils.log import Log

kMaxErrorReportTimes = 3


class CoolqClient(ClientBase):
    log_file = 'coolq'

    def __init__(self, config: IbConfig) -> None:
        self._log = Log.create(Log.path(self.log_file))
        self._config = config
        ClientBase.__init__(
            self, self._log.get_logger('coolqclient'), kWorkerTypeConsole,
            cmd_redis_ip=self._config.cmd_redis_ip,
            cmd_redis_port=self._config.cmd_redis_port)
        self._check_task: asyncio.Task = None
        self._error_report_times = 0
        self.add_dispatcher(ConsoleCommandResponse,
                            self.on_console_cmd_response)

    async def _check_process(self) -> None:
        while True:
            await asyncio.sleep(1)
            find_gateway_command = 'grep ibgateway'
            process = await asyncio.create_subprocess_shell(
                f'ps -A | {find_gateway_command}',
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE)
            stdout, stderr = await process.communicate()
            if process.returncode == 0:
                found = False
                process_list = stdout.decode().strip().split('\n')
                for running_process in process_list:
                    if find_gateway_command in running_process:
                        continue
                    found = True
                    self._error_report_times = 0
                if not found \
                   and self._error_report_times < kMaxErrorReportTimes:
                    try:
                        await self.send_message('No Gateway!!')
                        self._error_report_times += 1
                    except Exception:
                        pass

    async def send_command(self, command: str) -> None:
        if command:
            request = ConsoleCommandRequest()
            request.sid = self.sid()
            request.cmd = command
            await self.send_packet(request)

    async def on_console_cmd_response(
            self, response: ConsoleCommandResponse) -> None:
        if response.status == ResponseStatus.kSuccess:
            obj = json.loads(response.msg)
            if type(obj) is list and len(obj) > 0:
                message = '\n'.join([str(v) for v in obj])
            else:
                message = str(obj)
            await self.send_message(message)
        else:
            await self.send_message(f'{response.status}: {response.msg}')

    async def on_client_ready(self) -> None:
        self._check_task = asyncio.create_task(self._check_process())

    async def send_message(self, message: str) -> None:
        await nonebot.get_bot().send_private_msg(
            user_id=self._config.master_qq, message=message)


_coolq_client: Optional[CoolqClient] = None


async def init():
    config = await loadConfig()
    global _coolq_client
    _coolq_client = CoolqClient(config)
    await _coolq_client.initialize()


def get_coolq_client() -> CoolqClient:
    return _coolq_client


asyncio.get_event_loop().run_until_complete(init())

if __name__ == '__main__':
    nonebot.init(coolq_config)
    nonebot.load_plugins(
        path.join(path.dirname(__file__), 'plugins'),
        'app.coolq.plugins')
    nonebot.run()
