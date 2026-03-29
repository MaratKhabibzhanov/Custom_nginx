from typing import List, Dict
import multiprocessing
from proxy.config import config
from proxy.data_classes import Upstream
from proxy.utils import singleton
from tests.echo_app import run_app


@singleton
class UpstreamPool:
    def __init__(self):
        self._upstream_pool: List[Upstream] = config.UPSTREAMS
        self._idx = -1
        self._length = len(self._upstream_pool)
        self.load_info: Dict[str, int] = {}


    async def get_upstream(self) -> Upstream:
        self._idx += 1
        _upstream = self._upstream_pool[self._idx % self._length]
        to_info = str(_upstream)
        self.load_info[to_info] = self.load_info.setdefault(to_info, 0) + 1
        return _upstream
