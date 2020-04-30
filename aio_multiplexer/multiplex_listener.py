from typing import List, Dict
from asyncio import StreamReader, StreamWriter
from aio_multiplexer.multiplexer import IP, Port, Multiplexer, StreamName, Handler
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
        await multiplex_listener.unbind()


class MultiplexListener:
    _handlers: Dict[StreamName, Handler]
    _multiplexers: List[Multiplexer]

    def __init__(self):
        self._handlers = {}
        self._multiplexers = []
        self._running = False

    async def bind(self, ip: IP, port: Port) -> AbstractServer:
        if self._running:
            raise RuntimeError("Multiplex Listener already bind")
        self._server = await asyncio.start_server(self._server_callback, ip, port)
        self._running = True
        return self._server

    async def unbind(self):
        if not self._running:
            raise RuntimeError("Multiplex Listener already unbind")
        for multiplexer in self._multiplexers:
            await multiplexer.close()
        self._server.close()
        await self._server.wait_closed()
        self._running = False

    def set_handler(self, stream_name: StreamName, handler: Handler):
        self._handlers[stream_name] = handler
        for multiplexer in self._multiplexers:
            multiplexer.set_handler(stream_name, handler)

    def remove_handler(self, stream_name: StreamName):
        if stream_name not in self._handlers:
            raise KeyError("Handler does not exist")
        del self._handlers[stream_name]

    async def _server_callback(self, reader: StreamReader, writer: StreamWriter):
        multiplexer = Multiplexer(reader, writer)
        for stream_name, handler in self._handlers.items():
            multiplexer.set_handler(stream_name, handler)
        self._multiplexers.append(multiplexer)
