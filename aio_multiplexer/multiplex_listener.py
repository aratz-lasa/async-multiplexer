from asyncio import StreamReader, StreamWriter
from aio_multiplexer.multiplexer import Stream, StreamAddress, SocketAddress, IP, Port, Multiplexer
from contextlib import asynccontextmanager
import asyncio
from asyncio.events import AbstractServer


async def bind_multiplex_listener(ip: IP, port: Port):
    multiplex_listener = MultiplexListener()
    await multiplex_listener.bind(ip, port)
    return multiplex_listener


@asynccontextmanager
async def bind_multiplex_listener_context(ip: IP, port: Port):
    multiplex_listener = MultiplexListener()
    await multiplex_listener.bind(ip, port)
    try:
        yield multiplex_listener
    finally:
        await multiplex_listener.stop()


class MultiplexListener:
    def __init__(self):
        self._handlers = {}
        self._multiplexers = []

    async def bind(self, ip:IP, port: Port) -> AbstractServer:
        self._server = await asyncio.start_server(self._server_callback, ip, port)
        return self._server

    async def stop(self):
        for multiplexer in self._multiplexers:
            await multiplexer.close()
        self._server.close()
        await self._server.wait_closed()

    def set_handler(self, stream_name, handler):
        self._handlers[stream_name] = handler
        for multiplexer in self._multiplexers:
            multiplexer.set_handler(stream_name, handler)

    def remove_handler(self, stream_name):
        if stream_name not in self._handlers:
            raise KeyError("Handler does not exist")
        del self._handlers[stream_name]

    async def _server_callback(self, reader: StreamReader, writer: StreamWriter):
        ip, port = writer.get_extra_info('peername')
        multiplexer = Multiplexer(reader, writer)
        for stream_name, handler in self._handlers.items():
            multiplexer.set_handler(stream_name, handler)
        self._multiplexers.append(multiplexer)
