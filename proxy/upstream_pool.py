import asyncio
from typing import List, Dict, Tuple, Optional

from asyncio.streams import StreamReader, StreamWriter
from proxy.config import config
from proxy.data_classes import Upstream
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


    async def connect_to_upstream(self, upstream: Upstream) -> Optional[Tuple[StreamReader, StreamWriter]]:
        try:
            reader, writer =  await asyncio.wait_for(
                asyncio.open_connection(upstream.host, upstream.port),
                config.CONNECT_TIMEOUT
            )
            to_info = str(upstream)
            self.load_info[to_info] = self.load_info.setdefault(to_info, 0) + 1
            return reader, writer
        except asyncio.TimeoutError:
            warn_logger.warning(f'Timeout connecting to {upstream.host}:{upstream.port}')

    @staticmethod
    async def _is_alive(reader):
        try:
            await asyncio.wait_for(reader.read(1), timeout=0.01)
            return False
        except asyncio.TimeoutError:
            return True
        except (ConnectionResetError, BrokenPipeError):
            return False

    async def get_connection(self) -> Optional[Tuple[StreamReader, StreamWriter]]:
        try:
            reader, writer =  self._upstream_queue.get_nowait()
            if await self._is_alive(reader):
                return reader, writer
        except asyncio.QueueEmpty:
            return None

    async def release_connection(self, connection: Tuple[StreamReader, StreamWriter]) -> None:
        self._upstream_queue.put_nowait(connection)
