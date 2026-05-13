import socket
from typing import Optional

from proxy.config import config
from proxy.logger import warn_logger


def read_data(read_sock: socket.socket) -> Optional[bytes]:
    address = read_sock.getpeername()
    try:
        chunk = read_sock.recv(config.CHUNK_SIZE)
        return chunk
    except BlockingIOError:
        warn_logger.warning(f'Read connection reset error address - {address}')


def write_data(write_sock: socket.socket, data: bytes) -> bool:
    address = write_sock.getpeername()
    try:
        write_sock.send(data)
        return True
    except BlockingIOError:
        warn_logger.warning(f'Write connection reset error address - {address}')
        return False
