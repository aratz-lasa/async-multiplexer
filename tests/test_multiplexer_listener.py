from functools import partial
from hypothesis import given
from hypothesis.strategies import ip_addresses, integers, text, lists, tuples
import asyncio
from asyncio.events import AbstractServer as AsyncioServer
from unittest.mock import patch, MagicMock, AsyncMock, ANY
import pytest
from async_multiplexer import (
    bind_multiplex_listener,
    bind_multiplex_listener_context,
)
from async_multiplexer.protocol import MplexFlag
from async_multiplexer.multiplexer import Stream, StreamName
from async_multiplexer.multiplex_listener import MultiplexListener
from tests.utils import get_connection_mock, get_encoded_message

# All test coroutines will be treated as marked.
pytestmark = pytest.mark.asyncio


def get_start_sever_mock():
    remote_conn_mock = RemoteTCPConnectionMock()
    server_mock = MagicMock(spec_set=AsyncioServer)

    async def new_start_server(
        client_connected_cb, host=None, port=None, *args, **kwargs
    ):
        remote_conn_mock.add_server_config(client_connected_cb, host, port)
        return server_mock

    start_server_mock = AsyncMock(wraps=new_start_server)
    return start_server_mock, server_mock, remote_conn_mock


class RemoteTCPConnectionMock:
    def add_server_config(self, client_connected_cb, host, port):
        self.callback = client_connected_cb
        self.host = host
        self.port = port

    def new_mock_connection(self, remote_ip, remote_port):
        reader_mock, writer_mock = get_connection_mock(remote_ip, remote_port)
        asyncio.create_task(self.callback(reader_mock, writer_mock))
        return reader_mock, writer_mock


async def test_bind_multiplex_listener():
    start_server_mock, server_mock, remote_conn_mock = get_start_sever_mock()
    ip, port = ("127.0.0.1", 7777)
    with patch("asyncio.start_server", start_server_mock):
        multiplex_listener = await bind_multiplex_listener(ip, port)
        assert isinstance(multiplex_listener, MultiplexListener)
        start_server_mock.assert_awaited_with(ANY, ip, port)


async def test_unbind_multiplex_listener():
    start_server_mock, server_mock, remote_conn_mock = get_start_sever_mock()
    with patch("asyncio.start_server", start_server_mock):
        multiplex_listener = await bind_multiplex_listener("127.0.0.1", 7777)

        await multiplex_listener.unbind()
        server_mock.close.assert_called()
        server_mock.wait_closed.assert_awaited()


async def test_bind_multiplexer_context():
    start_server_mock, server_mock, remote_conn_mock = get_start_sever_mock()
    ip, port = ("127.0.0.1", 7777)
    with patch("asyncio.start_server", start_server_mock):
        async with bind_multiplex_listener_context(ip, port) as multiplex_listener:
            assert isinstance(multiplex_listener, MultiplexListener)
            start_server_mock.assert_awaited_with(ANY, ip, port)
        server_mock.close.assert_called()
        server_mock.wait_closed.assert_awaited()


async def test_bind_twice():
    start_server_mock, server_mock, remote_conn_mock = get_start_sever_mock()
    ip, port = ("127.0.0.1", 7777)
    with patch("asyncio.start_server", start_server_mock):
        multiplex_listener = await bind_multiplex_listener(ip, port)
        with pytest.raises(RuntimeError):
            await multiplex_listener.bind(ip, port)


async def test_unbind_twice():
    start_server_mock, server_mock, remote_conn_mock = get_start_sever_mock()
    ip, port = ("127.0.0.1", 7777)
    with patch("asyncio.start_server", start_server_mock):
        multiplex_listener = await bind_multiplex_listener(ip, port)
        await multiplex_listener.unbind()
        with pytest.raises(RuntimeError):
            await multiplex_listener.unbind()


@given(
    remote_address=tuples(ip_addresses(), integers(min_value=0, max_value=65535)),
    stream_names=lists(text(min_size=1), unique=True),
)
async def test_handle_streams(remote_address, stream_names):
    remote_ip, remote_port = remote_address
    handled_events = {name: asyncio.Event() for name in stream_names}

    async def handler(stream_name: StreamName, stream: Stream):
        assert stream.name == stream_name
        assert stream.ip == remote_ip
        assert stream.port == remote_port
        handled_events[stream_name].set()

    start_server_mock, server_mock, remote_conn_mock = get_start_sever_mock()
    with patch("asyncio.start_server", start_server_mock):
        async with bind_multiplex_listener_context(
            "127.0.0.1", 7777
        ) as multiplex_listener:
            # First set handler, then open connection
            for stream_name in stream_names:
                multiplex_listener.set_handler(
                    stream_name, partial(handler, stream_name)
                )

            for stream_name in stream_names:
                reader_mock, writer_mock = remote_conn_mock.new_mock_connection(
                    remote_ip, remote_port
                )
                encoded_message = get_encoded_message(
                    stream_name, MplexFlag.NEW_STREAM, stream_name.encode()
                )
                reader_mock.feed_data(encoded_message)

                await asyncio.wait_for(handled_events[stream_name].wait(), timeout=0.01)


@given(
    remote_address=tuples(ip_addresses(), integers(min_value=0, max_value=65535)),
    stream_names=lists(text(min_size=1), unique=True),
)
async def test_handle_streams_after_connected(remote_address, stream_names):
    remote_ip, remote_port = remote_address
    handled_events = {name: asyncio.Event() for name in stream_names}

    async def handler(stream_name: StreamName, stream: Stream):
        assert stream.name == stream_name
        assert stream.ip == remote_ip
        assert stream.port == remote_port
        handled_events[stream_name].set()

    start_server_mock, server_mock, remote_conn_mock = get_start_sever_mock()
    with patch("asyncio.start_server", start_server_mock):
        async with bind_multiplex_listener_context(
            "127.0.0.1", 7777
        ) as multiplex_listener:
            # First open connection, then set handler
            for stream_name in stream_names:
                reader_mock, writer_mock = remote_conn_mock.new_mock_connection(
                    remote_ip, remote_port
                )
                await asyncio.sleep(0)
                for stream_name in stream_names:
                    multiplex_listener.set_handler(
                        stream_name, partial(handler, stream_name)
                    )
                encoded_message = get_encoded_message(
                    stream_name, MplexFlag.NEW_STREAM, stream_name.encode()
                )
                reader_mock.feed_data(encoded_message)

                await asyncio.wait_for(handled_events[stream_name].wait(), timeout=0.01)


async def test_remove_handler():
    remote_ip, remote_port = ("127.0.0.2", 7777)
    stream_name = "stream.1"
    handled_event = asyncio.Event()

    async def handler(stream: Stream):
        assert stream.name == stream_name
        assert stream.ip == remote_ip
        assert stream.port == remote_port
        handled_event.set()

    start_server_mock, server_mock, remote_conn_mock = get_start_sever_mock()
    with patch("asyncio.start_server", start_server_mock):
        async with bind_multiplex_listener_context(
            "127.0.0.1", 7777
        ) as multiplex_listener:
            multiplex_listener.set_handler(stream_name, handler)
            multiplex_listener.remove_handler(stream_name)

            reader_mock, writer_mock = remote_conn_mock.new_mock_connection(
                remote_ip, remote_port
            )
            encoded_message = get_encoded_message(
                stream_name, MplexFlag.NEW_STREAM, stream_name.encode()
            )
            reader_mock.feed_data(encoded_message)

            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(handled_event.wait(), timeout=0.01)
            with pytest.raises(KeyError):
                multiplex_listener.remove_handler(stream_name)
