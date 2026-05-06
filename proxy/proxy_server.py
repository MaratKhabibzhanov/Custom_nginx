import asyncio
import multiprocessing
import queue
import selectors
from selectors import SelectorKey
import threading
from concurrent.futures import ThreadPoolExecutor

import uvloop
import socket
from asyncio.streams import StreamReader, StreamWriter
from typing import Tuple, List

from proxy.config import config
from proxy.data_classes import Connection
from proxy.logger import logger, warn_logger
from proxy.parser import QueryParser
from proxy.timeouts import timeout_writer, timeout_reader
from proxy.upstream_pool import UpstreamPool
from proxy.utils import singleton


@singleton
class ProxyServer:
    def __init__(self,
                 server_host: str,
                 server_port: int):
        self._server_host = server_host
        self._server_port = server_port
        self._chunk_size = 1024
        self._client_semaphore = asyncio.Semaphore(config.MAX_CLIENT_CONNECTIONS)
        self._upstream_pool = UpstreamPool()
        self._parser = QueryParser()
        self._stream_queue = queue.Queue()
        self._main_queue = queue.Queue()
        # self._lock = threading.Lock()
        self._selector = selectors.DefaultSelector()

    def _create_server(self) -> socket.socket:
        server_socket = socket.socket()
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        server_socket.bind((self._server_host, self._server_port))
        server_socket.listen()
        server_socket.setblocking(False)
        logger.info(f"Start serving on {self._server_host}:{self._server_port}")
        return server_socket

    def _accept_conn(self, server_socket: socket.socket) -> None:
        try:
            connection, address = server_socket.accept()
            connection.setblocking(False)
            p_name = multiprocessing.current_process().name
            logger.info(f"Connection from {address} in process {p_name}")
            self._selector.register(connection, selectors.EVENT_READ)
        except socket.error as error:
            logger.info(f"Error accepting connection: {error}")

    def _client_reader(self) -> None:
        try:
            while True:
                client_sock = self._main_queue.get()
                if client_sock.fileno() == -1:
                    continue
                upstream_sock = socket.socket()
                address = ("127.0.0.1", 9001)
                upstream_sock.connect(address)
                upstream_sock.setblocking(False)

                self._stream_queue.put_nowait((client_sock, upstream_sock))
                self._handler(client_sock, upstream_sock)
        except socket.error as error:
            logger.warning(f"Error receiving data: {error}")
            client_sock.close()

    @staticmethod
    def _stream_body(read_sock: socket.socket,
                     write_sock: socket.socket,
                     data: bytes,
                     content_length: int) -> None:
        while True:
            if data:
                try:
                    write_sock.send(data)
                except BlockingIOError:
                    break
            content_length -= len(data)
            if content_length < 1:
                break
            try:
                data = read_sock.recv(1024)
                if not data:
                    break
            except BlockingIOError:
                break

    def _handler(self,
                 read_sock: socket.socket,
                 write_sock: socket.socket,) -> None:
        _buffer = bytes()
        body = bytes()
        content_length = 0
        while True:
            len_buffer = len(_buffer)
            if len_buffer > 8192:
                warn_logger.warning(f'request header fields too large: {len_buffer}')
            try:
                chunk = read_sock.recv(1024)
            except BlockingIOError:
                continue
            if not chunk:
                break
            _buffer += chunk
            if b'\r\n\r\n' not in _buffer:
                continue
            head, body, content_length, log_message = self._parser.parse_query(_buffer)
            logger.info(log_message)
            try:
                write_sock.send(head)
            except BlockingIOError:
                pass
            break
        if content_length:
            self._stream_body(read_sock, write_sock, body, content_length)


    def _event_loop(self, server_socket: socket.socket) -> None:
        logger.info("run event loop")
        self._selector.register(server_socket, selectors.EVENT_READ)

        while True:
            try:
                events: List[Tuple[SelectorKey, int]] = self._selector.select(timeout=1)
            except Exception as e:
                warn_logger.warning(f"error on sockets {e}")
                continue
            if len(events) == 0:
                continue
            for event, _ in events:
                event_socket = event.fileobj
                if event_socket is server_socket:
                    self._accept_conn(event_socket)
                elif event_socket not in self._main_queue.queue and event_socket.fileno() != -1:
                    self._main_queue.put_nowait(event_socket)
                    self._selector.unregister(event_socket)

    def _upstream_reader(self):
        while True:
            client_sock, upstream_sock = self._stream_queue.get()
            if client_sock.fileno() == -1:
                continue
            self._handler(upstream_sock, client_sock)
            upstream_sock.close()
            client_sock.close()

    @staticmethod
    def _task(args):
        function, *sub_args = args
        return function()

    def run_proxy(self):
        pool = ThreadPoolExecutor()
        pool.map(self._task, [(self._client_reader,), (self._upstream_reader,)])
        server_socket = self._create_server()
        self._event_loop(server_socket)

def start_proxy():
    proxy_server = ProxyServer(config.PROXY_SERVER_HOST, config.PROXY_SERVER_PORT)
    proxy_server.run_proxy()
