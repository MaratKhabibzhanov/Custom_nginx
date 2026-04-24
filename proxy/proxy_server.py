import asyncio
import multiprocessing
import queue
import select
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
        self._sockets = []

    def _create_server(self) -> socket.socket:
        server_socket = socket.socket()
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        server_socket.bind((self._server_host, self._server_port))
        server_socket.listen()
        # server_socket.setblocking(False)
        logger.info(f"Start serving on {self._server_host}:{self._server_port}")
        return server_socket

    @staticmethod
    def _accept_conn(server_sock: socket.socket, sockets: List[socket.socket]) -> None:
        try:
            conn, addr = server_sock.accept()
            p_name = multiprocessing.current_process().name
            logger.info(f"Connection from {addr} in process {p_name}")
            sockets.append(conn)
        except socket.error as error:
            logger.info(f"Error accepting connection: {error}")

    def _client_reader(self) -> None:
        try:
            while True:
                print("start client read")
                client_sock = self._main_queue.get()
                upstream_sock = socket.socket()
                address = ("127.0.0.1", 9001)
                print(address)
                upstream_sock.connect(address)
                print("connected")
                self._stream_queue.put_nowait((client_sock, upstream_sock))
                self._stream_data(client_sock, upstream_sock)
        except socket.error as error:
            logger.warning(f"Error receiving data: {error}")
            client_sock.close()

    def _stream_data(self,
                     read_sock: socket.socket,
                     write_sock: socket.socket,) -> None:
        while True:
            # with self._lock:
            data = read_sock.recv(1024)
            print(f"before {data}")
            if data == b'':
                try:
                    self._sockets.remove(read_sock)
                except ValueError:
                    pass
                break
            print(f"after {data}")
            write_sock.send(data)


    def _event_loop(self, server_socket: socket.socket) -> None:
        print("run event loop")
        self._sockets = [server_socket]
        print(self._sockets)

        while self._sockets:
            print("before select")
            sockets_for_read, _, _ = select.select(self._sockets, [], [])
            print("after select",sockets_for_read)
            for sock in sockets_for_read:
                if sock is server_socket:
                    print("waiting for connection")
                    self._accept_conn(sock, self._sockets)
                else:
                    self._main_queue.put_nowait(sock)

    def _upstream_reader(self):
        while True:
            print("run upstream reader")
            client_sock, upstream_sock = self._stream_queue.get()
            print("upstream geted data")
            self._stream_data(upstream_sock, client_sock)
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
