import asyncio
from time import time
from asyncio.streams import StreamReader, StreamWriter
from dataclasses import dataclass


@dataclass
class Upstream:
    host: str
    port: int
    semaphore: asyncio.Semaphore

    def __str__(self):
        return f"Upstream({self.host}, {self.port})"


@dataclass
class Connection:
    reader: StreamReader
    writer: StreamWriter
    timestamp: time
