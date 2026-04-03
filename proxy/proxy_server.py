import asyncio
from asyncio.streams import StreamReader, StreamWriter
from typing import Tuple, Optional

from proxy.config import config
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

    @staticmethod
    def _get_timeout_answer() -> bytes:
        return (b"HTTP/1.1 504 Gateway Timeout\r\nContent-Type: text/plain; "
                b"charset=utf-8\r\nContent-Length: 23\r\nConnection: close\r\n\r\nThe upstream timed out.")

    @staticmethod
    async def _stream_body(reader: StreamReader,
                           writer: StreamWriter,
                           data: bytes,
                           content_length: int) -> None:
        while True:
            if data:
                writer.write(data)
                if not await timeout_writer(writer):
                    break
            content_length -= len(data)
            if not content_length:
                break
            data = await timeout_reader(reader)
            if not data:
                break

    async def _handler(self,
                       reader: StreamReader,
                       writer: StreamWriter) -> None:
        _buffer = bytes()
        body = bytes()
        content_length = 0
        while True:
            chunk = await timeout_reader(reader)
            if not chunk:
                break
            _buffer += chunk
            if b'\r\n\r\n' not in _buffer:
                continue
            head, body, content_length, log_message = self._parser.parse_query(_buffer.decode('utf-8'))
            logger.info(log_message)

            writer.write(head)
            await timeout_writer(writer)
            break
        if content_length:
            await self._stream_body(reader, writer, body, content_length)

    async def _run_stream(self,
                          client_reader: StreamReader, client_writer: StreamWriter,
                          up_reader: StreamReader, up_writer: StreamWriter) -> None:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._handler(client_reader, up_writer))
            tg.create_task(self._handler(up_reader, client_writer))

    async def _process_connection(self,
                                  up_connection: Tuple[StreamReader, StreamWriter],
                                  client_connection: Tuple[StreamReader, StreamWriter],
                                  address: str) -> None:
        up_reader, up_writer = up_connection
        client_reader, client_writer = client_connection
        try:
            await asyncio.wait_for(self._run_stream(client_reader, client_writer,
                                                    up_reader, up_writer), config.TOTAL_TIMEOUT)
        except asyncio.TimeoutError:
            warn_logger.warning(f'Timeout processing to {address}')
            client_writer.write(self._get_timeout_answer())
            await client_writer.drain()
        finally:
            await self._upstream_pool.release_connection(up_connection)
            client_writer.close()
            logger.info(f'Queue size {self._upstream_pool._upstream_queue.qsize()}')
            logger.info(f'Stop serving {address}')
            logger.info(f'Round-robin: {self._upstream_pool.load_info}')


    async def _client_connected(self, client_reader: StreamReader, client_writer: StreamWriter) -> None:
        async with self._client_semaphore:
            address = client_writer.get_extra_info('peername')
            logger.info(f'Start serving {address}')

            connection = await self._upstream_pool.get_connection()
            if connection:
                return await self._process_connection(connection, (client_reader, client_writer), address)

            upstream = self._upstream_pool.get_upstream()
            async with upstream.semaphore:
                connection = await self._upstream_pool.connect_to_upstream(upstream)
                return await self._process_connection(connection, (client_reader, client_writer), address)


    async def run_proxy_server(self):
        logger.info('Start proxy server')
        srv = await asyncio.start_server(self._client_connected, self._server_host, self._server_port)
        async with srv:
            await srv.serve_forever()

if __name__ == '__main__':
    proxy_server = ProxyServer(config.PROXY_SERVER_HOST, config.PROXY_SERVER_PORT)
    asyncio.run(proxy_server.run_proxy_server())
