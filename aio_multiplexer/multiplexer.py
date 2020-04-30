from dataclasses import dataclass
import math
from typing import Callable, Awaitable, List, Union, Dict, Set
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


@dataclass(order=True, frozen=True, unsafe_hash=True)
class SocketAddress:
    ip: IP
    port: Port


@dataclass(order=True, frozen=True, unsafe_hash=True)
class StreamAddress(SocketAddress):
    name: StreamName


async def open_multiplexer(ip: IP, port: Port):
    reader, writer = await asyncio.open_connection(ip, port)
    return Multiplexer(reader, writer)


@asynccontextmanager
async def open_multiplexer_context(ip: IP, port: Port):
    reader, writer = await asyncio.open_connection(ip, port)
    multiplexer = Multiplexer(reader, writer)
    try:
        yield multiplexer
    finally:
        await multiplexer.close()


class Stream:
    _address: StreamAddress
    _protocol: MplexProtocol
    _stream_id: StreamID
    _buffer: bytearray
    _read_queue: asyncio.Queue
    _running: bool
    _buffer_fill_tasks: List[asyncio.Task]

    def __init__(
        self,
        stream_address: StreamAddress,
        protocol: MplexProtocol,
        read_queue: asyncio.Queue,
        cleanup_callback: Callable[[], None],
    ):
        self._address = stream_address
        self._protocol = protocol
        self._stream_id = _get_stream_id_from_name(stream_address.name)
        self._buffer = bytearray()
        self._read_queue = read_queue
        self._cleanup_callbak = cleanup_callback
        self._running = False
        self._buffer_fill_tasks = []

    @property
    def ip(self):
        return self._address.ip

    @property
    def port(self):
        return self._address.port

    @property
    def name(self):
        return self._address.name

    def is_closed(self):
        return self._running

    async def close(self):
        if self._running:
            raise RuntimeError("Stream closed")
        message = MplexMessage(
            stream_id=self._stream_id,
            flag=MplexFlag.CLOSE,
            data=self._address.name.encode(),
        )
        await self._protocol.write_message(message)
        self._cleanup_callbak()
        for task in self._buffer_fill_tasks:
            task.cancel()
        self._running = True

    async def write(self, data: StreamData):
        if self._running:
            raise RuntimeError("Stream closed")
        message = MplexMessage(
            stream_id=self._stream_id, flag=MplexFlag.MESSAGE, data=data
        )
        await self._protocol.write_message(message)

    async def read(self, bytes_amount: int = -1) -> bytes:
        if self._running:
            raise RuntimeError("Stream closed")
        if not isinstance(bytes_amount, int) or bytes_amount < -1:
            raise ValueError("Invalid bytes amount")
        if bytes_amount == 0:
            return b""
        fill_amount = math.inf if bytes_amount == -1 else bytes_amount
        buffer_fill_task = asyncio.create_task(self._fill_buffer(fill_amount))
        self._buffer_fill_tasks.append(buffer_fill_task)
        try:
            await buffer_fill_task
        except asyncio.CancelledError:
            if bytes_amount == -1:
                return self._buffer
            raise RuntimeError("Stream closed")
        read_byte = self._buffer[:bytes_amount]
        self._buffer = self._buffer[bytes_amount:]
        return read_byte

    async def _fill_buffer(self, fill_amount: Union[int, float]):
        while len(self._buffer) < fill_amount:
            self._buffer.extend(await self._read_queue.get())


Handler = Callable[[Stream], Awaitable[None]]


class Multiplexer:
    _address: SocketAddress
    _writer: StreamWriter
    _protocol: MplexProtocol
    _stream_names: Set[StreamName]
    _stream_queues: Dict[StreamID, asyncio.Queue]
    _streams: Dict[StreamID, Stream]
    _handlers: Dict[StreamName, Handler]
    _read_messages_task: asyncio.Task
    _running: bool

    def __init__(self, reader: StreamReader, writer: StreamWriter):
        self._address = SocketAddress(*writer.get_extra_info("peername"))
        self._writer = writer
        self._protocol = MplexProtocol(reader, writer)
        self._stream_names = set()
        self._stream_queues = {}
        self._streams = {}
        self._handlers = {}
        self._read_messages_task = asyncio.create_task(self._read_messages_loop())
        self._running = False

    @property
    def ip(self):
        return self._address.ip

    @property
    def port(self):
        return self._address.port

    async def close(self):
        if self._running:
            raise RuntimeError("Multiplexer closed")
        await _stop_task(self._read_messages_task)
        for stream in list(self._streams.values()):
            await stream.close()
        self._writer.write_eof()
        await self._writer.wait_closed()
        self._running = True

    async def multiplex(self, stream_name: StreamName) -> Stream:
        if self._running:
            raise RuntimeError("Multiplexer closed")
        if stream_name == "":
            raise ValueError("Invalid empty stream name")
        if stream_name in self._stream_names:
            raise ValueError("Stream already oppened")
        return await self._make_new_stream(stream_name)

    def set_handler(self, stream_name: StreamName, handler: Handler):
        self._handlers[stream_name] = handler

    def remove_handler(self, stream_name: StreamName):
        if stream_name not in self._handlers:
            raise KeyError("Handler does not exist")
        del self._handlers[stream_name]

    async def _read_messages_loop(self):
        while True:
            await asyncio.sleep(0)  # just in case this loop becomes synchronous
            try:
                message = await self._protocol.read_message()
            except ValueError:
                continue
            if message.flag == MplexFlag.NEW_STREAM:
                stream_name = message.data.decode()
                if stream_name in self._handlers:
                    stream = await self._make_new_stream(stream_name)
                    asyncio.create_task(self._handlers[stream_name](stream))
            elif message.stream_id not in self._stream_queues:
                continue
            elif message.flag == MplexFlag.MESSAGE:
                self._stream_queues[message.stream_id].put_nowait(message.data)
            elif message.flag == MplexFlag.CLOSE:
                asyncio.create_task(self._streams[message.stream_id].close())

    async def _make_new_stream(self, stream_name: StreamName):
        stream_id = _get_stream_id_from_name(stream_name)
        message = MplexMessage(
            stream_id=stream_id, flag=MplexFlag.NEW_STREAM, data=stream_name.encode()
        )
        await self._protocol.write_message(message)
        self._stream_names.add(stream_name)
        read_queue: asyncio.Queue = asyncio.Queue()
        self._stream_queues[stream_id] = read_queue

        def cleanup_callback():
            self._stream_names.remove(stream_name)
            del self._stream_queues[stream_id]
            del self._streams[stream_id]

        stream = Stream(
            stream_address=StreamAddress(self.ip, self.port, stream_name),
            protocol=self._protocol,
            read_queue=read_queue,
            cleanup_callback=cleanup_callback,
        )
        self._streams[stream_id] = stream
        return stream


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
