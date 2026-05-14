import socket
import time
from typing import List, Dict, Optional
import queue
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
        queue_size = config.MAX_CONNECTIONS_PER_UPSTREAM * self._length
        self._upstream_queue: queue.Queue = queue.Queue(maxsize=queue_size)


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
                and not connection.upstream_socket.fileno() == -1)

    def connect_to_upstream(self, upstream: Upstream) -> Optional[Connection]:
        upstream_sock = socket.socket()
        upstream_sock.connect((upstream.host, upstream.port))
        upstream_sock.settimeout(config.SOCKET_TIMEOUT)
        to_info = str(upstream)
        self.load_info[to_info] = self.load_info.setdefault(to_info, 0) + 1
        return Connection(upstream_sock, self._get_timestamp())


    def get_connection(self) -> Optional[Connection]:
        while not self._upstream_queue.empty():
            connection = self._upstream_queue.get_nowait()
            if self._check_alive_connection(connection):
                return connection
            connection.upstream_socket.close()
        return None

    def release_connection(self, connection: Connection) -> None:
        if self._check_alive_connection(connection):
            connection.timestamp = self._get_timestamp()
            try:
                self._upstream_queue.put(connection, config.UPSTREAM_TIMEOUT)
            except queue.Full:
                warn_logger.warning("Upstream timeout")
                connection.upstream_socket.close()
        else:
            connection.upstream_socket.close()
