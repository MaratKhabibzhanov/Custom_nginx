import socket
from time import time
from dataclasses import dataclass


@dataclass
class Upstream:
    host: str
    port: int
    def __str__(self):
        return f"Upstream({self.host}, {self.port})"


@dataclass
class Connection:
    upstream_socket: socket.socket
    timestamp: time
