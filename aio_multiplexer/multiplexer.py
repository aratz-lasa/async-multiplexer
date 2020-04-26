from typing import Callable
from contextlib import asynccontextmanager
import asyncio
from asyncio import StreamWriter, StreamReader
from hashlib import sha256

from aio_multiplexer.protocol import (
    MplexFlag,
    MplexProtocol,
    MplexMessage,
    StreamData,
    StreamID,
)

IP = str
Port = int
StreamName = str


async def open_tcp_multiplexer(ip: IP, port: Port):
    reader, writer = await asyncio.open_connection(ip, port)
    return Multiplexer(reader, writer)


@asynccontextmanager
async def open_tcp_multiplexer_context(ip: IP, port: Port):
    reader, writer = await asyncio.open_connection(ip, port)
    multiplexer = Multiplexer(reader, writer)
    try:
        yield multiplexer
    finally:
        await multiplexer.close()


class Stream:
    def __init__(
        self,
        stream_name: StreamName,
        protocol: MplexProtocol,
        read_queue: asyncio.Queue,
        cleanup_callback: Callable[[], None],
    ):
        self._protocol = protocol
        self._stream_name = stream_name
        self._stream_id = _get_stream_id_from_name(stream_name)
        self._buffer = bytearray()
        self._read_queue = read_queue
        self._cleanup_callbak = cleanup_callback
        self._closed = False
        self._buffer_fill_tasks = []

    async def close(self):
        if self._closed:
            raise RuntimeError("Stream closed")
        message = MplexMessage(
            stream_id=self._stream_id,
            flag=MplexFlag.CLOSE,
            data=self._stream_name.encode(),
        )
        await self._protocol.write_message(message)
        self._cleanup_callbak()
        for task in self._buffer_fill_tasks:
            task.cancel()
        self._closed = True

    async def write(self, data: StreamData):
        if self._closed:
            raise RuntimeError("Stream closed")
        message = MplexMessage(
            stream_id=self._stream_id, flag=MplexFlag.MESSAGE, data=data
        )
        await self._protocol.write_message(message)

    async def read(self, bytes_amount: int) -> bytes:
        if self._closed:
            raise RuntimeError("Stream closed")
        if bytes_amount == 0:
            return b""
        try:
            buffer_fill_task = asyncio.create_task(self._fill_buffer(bytes_amount))
            self._buffer_fill_tasks.append(buffer_fill_task)
            await buffer_fill_task
        except asyncio.CancelledError:
            raise RuntimeError("Stream closed")
        read_byte = self._buffer[:bytes_amount]
        self._buffer = self._buffer[bytes_amount:]
        return read_byte

    async def _fill_buffer(self, fill_limit: int):
        while len(self._buffer) < fill_limit:
            self._buffer.extend(await self._read_queue.get())


class Multiplexer:
    def __init__(self, reader: StreamReader, writer: StreamWriter):
        self._writer = writer
        self._protocol = MplexProtocol(reader, writer)
        self._stream_names = set()
        self._stream_queues = {}
        self._streams = {}
        self._read_messages_task = asyncio.create_task(self._read_messages_loop())

    async def close(self):
        await _stop_task(self._read_messages_task)
        self._writer.write_eof()
        await self._writer.wait_closed()

    async def multiplex(self, stream_name: StreamName) -> Stream:
        if stream_name == "":
            raise ValueError("Invalid empty stream name")
        if stream_name in self._stream_names:
            raise ValueError("Stream already oppened")

        stream_id = _get_stream_id_from_name(stream_name)
        message = MplexMessage(
            stream_id=stream_id, flag=MplexFlag.NEW_STREAM, data=stream_name.encode()
        )
        await self._protocol.write_message(message)
        self._stream_names.add(stream_name)
        read_queue = asyncio.Queue()
        self._stream_queues[stream_id] = read_queue

        def cleanup_callback():
            nonlocal self
            self._stream_names.remove(stream_name)
            del self._stream_queues[stream_id]
            del self._streams[stream_id]

        stream = Stream(
            stream_name=stream_name,
            protocol=self._protocol,
            read_queue=read_queue,
            cleanup_callback=cleanup_callback,
        )
        self._streams[stream_id] = stream
        return stream

    async def _read_messages_loop(self):
        while True:
            message = await self._protocol.read_message()
            if (
                message.flag == MplexFlag.MESSAGE
                and message.stream_id in self._stream_queues
            ):  # todo: else raise error?
                self._stream_queues[message.stream_id].put_nowait(message.data)
            elif (
                message.flag == MplexFlag.CLOSE
                and message.stream_id in self._stream_queues
            ):  # todo: else raise error?
                asyncio.create_task(self._streams[message.stream_id].close())
            await asyncio.sleep(0)  # just in case this loop becomes synchronous


def _get_stream_id_from_name(stream_name: StreamName) -> StreamID:
    hash_value = sha256()
    hash_value.update(stream_name.encode())
    stream_id = int.from_bytes(hash_value.digest(), "big")
    return stream_id


async def _stop_task(task: asyncio.Task):
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
