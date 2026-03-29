import asyncio
from dataclasses import dataclass


@dataclass
class Upstream:
    host: str
    port: int
    semaphore: asyncio.Semaphore

    def __str__(self):
        return f"Upstream({self.host}, {self.port})"
