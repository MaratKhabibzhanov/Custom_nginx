import asyncio
import time
from typing import List, Dict, Optional

from proxy.config import config
from proxy.data_classes import Upstream, Connection
from proxy.logger import warn_logger
from proxy.utils import singleton


@singleton
class UpstreamPool:
    def __init__(self):
        self._upstream_info: List[Upstream] = config.UPSTREAMS
        self._idx = -1
        self._length = len(self._upstream_info)
        self.load_info: Dict[str, int] = {}
        self._upstream_queue: asyncio.Queue = asyncio.Queue()


    def get_upstream(self) -> Upstream:
        self._idx += 1
        _upstream = self._upstream_info[self._idx % self._length]
        return _upstream

    @staticmethod
    def _get_timestamp():
        return time.time() + config.TIMEOUT_KEEP_ALIVE

    @staticmethod
    def _check_timestamp(timestamp: time.time) -> bool:
        return timestamp > time.time()

    def _check_alive_connection(self, connection: Connection) -> bool:
        return (self._check_timestamp(connection.timestamp)
                and not connection.reader.at_eof()
                and not connection.writer.is_closing())

    async def connect_to_upstream(self, upstream: Upstream) -> Optional[Connection]:
        try:
            reader, writer =  await asyncio.wait_for(
                asyncio.open_connection(upstream.host, upstream.port),
                config.CONNECT_TIMEOUT
            )
            to_info = str(upstream)
            self.load_info[to_info] = self.load_info.setdefault(to_info, 0) + 1
            return Connection(reader, writer, self._get_timestamp())
        except asyncio.TimeoutError:
            warn_logger.warning(f'Timeout connecting to {upstream.host}:{upstream.port}')


    async def get_connection(self) -> Optional[Connection]:
        while not self._upstream_queue.empty():
            connection = self._upstream_queue.get_nowait()
            if self._check_alive_connection(connection):
                return connection
            connection.writer.close()
            await connection.writer.wait_closed()
        return None

    async def release_connection(self, connection: Connection) -> None:
        if self._check_alive_connection(connection):
            connection.timestamp = self._get_timestamp()
            self._upstream_queue.put_nowait(connection)
        else:
            connection.writer.close()
            await connection.writer.wait_closed()
