import asyncio
from typing import List, Tuple

import yaml
from yaml import SafeLoader

from proxy.data_classes import Upstream
from proxy.utils import singleton


@singleton
class ConfigLoader:
    def __init__(self):
        self._conf_filename = '../proxy/conf.yml'
        self._conf_data = self._load_config()
        # timeouts
        self.CONNECT_TIMEOUT = self._conf_data['timeouts']['connect_ms'] / 1000
        self.READ_TIMEOUT = self._conf_data['timeouts']['read_ms'] / 1000
        self.WRITE_TIMEOUT = self._conf_data['timeouts']['write_ms'] / 1000
        self.TOTAL_TIMEOUT = self._conf_data['timeouts']['total_ms'] / 1000
        self.TIMEOUT_KEEP_ALIVE = self._conf_data['timeouts']['keep_alive_ms'] / 1000
        # connections_count
        self.MAX_CLIENT_CONNECTIONS = self._conf_data['limits']['max_client_conns']
        self.MAX_CONNECTIONS_PER_UPSTREAM = self._conf_data['limits']['max_conns_per_upstream']

        self.PROXY_SERVER_HOST = self._conf_data['listen']['host']
        self.PROXY_SERVER_PORT = self._conf_data['listen']['port']
        self.UPSTREAMS = self._get_upstreams()
        self.CHUNK_SIZE = self._conf_data['chunk_size']
        self.LOG_LEVEL = self._conf_data['logging']['level']


    def _load_config(self):
        with open(self._conf_filename, 'r') as f:
            return yaml.load(f, Loader=SafeLoader)

    def _get_upstreams(self) -> List[Upstream]:
        return [
            Upstream(
                host=ups['host'],
                port=ups['port'],
                semaphore=asyncio.Semaphore(self.MAX_CONNECTIONS_PER_UPSTREAM)
            ) for ups in self._conf_data['upstreams']
        ]

config = ConfigLoader()
