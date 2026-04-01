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


    async def _get_upstream(self) -> Upstream:
        self._idx += 1
        _upstream = self._upstream_info[self._idx % self._length]
        to_info = str(_upstream)
        self.load_info[to_info] = self.load_info.setdefault(to_info, 0) + 1
        return _upstream

    async def _connect_to_upstream(self) -> Optional[Tuple[StreamReader, StreamWriter]]:
        upstream = await self._get_upstream()
        async with upstream.semaphore:
            try:
                return await asyncio.wait_for(
                    asyncio.open_connection(upstream.host, upstream.port),
                    config.CONNECT_TIMEOUT
                )
            except asyncio.TimeoutError:
                warn_logger.warning(f'Timeout connecting to {upstream.host}:{upstream.port}')

    async def get_connection(self) -> Optional[Tuple[StreamReader, StreamWriter]]:
        try:
            reader, writer =  self._upstream_queue.get_nowait()
            if not writer.transport.is_closing():
                return reader, writer
        except asyncio.QueueEmpty:
            pass
        return await self._connect_to_upstream()

    async def release_connection(self, connection: Tuple[StreamReader, StreamWriter]) -> None:
        self._upstream_queue.put_nowait(connection)
