import asyncio
from asyncio.streams import StreamReader, StreamWriter
from typing import Optional

from proxy.config import config
from proxy.logger import warn_logger


async def timeout_reader(reader: StreamReader) -> Optional[bytes]:
    address = reader._transport.get_extra_info('peername')
    try:
        data = await asyncio.wait_for(reader.read(config.CHUNK_SIZE), config.READ_TIMEOUT)
        return data
    except asyncio.TimeoutError:
        warn_logger.warning(f'Read timeout error address - {address}')
    except ConnectionResetError:
        warn_logger.warning(f'Read connection reset error address - {address}')


async def timeout_writer(writer: StreamWriter) -> bool:
    address = writer.get_extra_info('peername')
    try:
        await asyncio.wait_for(writer.drain(), config.WRITE_TIMEOUT)
        return True
    except asyncio.TimeoutError:
        warn_logger.warning(f'Write timeout error address - {address}')
        return False
    except ConnectionResetError:
        warn_logger.warning(f'Write connection reset error address - {address}')
        return False
