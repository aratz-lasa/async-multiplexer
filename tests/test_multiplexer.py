import asyncio
import random
from functools import partial
from typing import List
from unittest.mock import patch, AsyncMock

import pytest
from hypothesis import given
from hypothesis.strategies import text, binary, lists

from async_multiplexer import open_multiplexer, open_multiplexer_context
from async_multiplexer.multiplexer import Multiplexer, Stream, StreamName, StreamData
from async_multiplexer.protocol import MplexFlag

# All test coroutines will be treated as marked.
from tests.utils import get_encoded_message, get_connection_mock

pytestmark = pytest.mark.asyncio


@patch("asyncio.open_connection")
async def test_open_multiplexer(mock: AsyncMock):
    ip, port = ("127.0.0.1", 7777)
    mock.return_value = get_connection_mock(ip, port)

    multiplexer = await open_multiplexer(ip, port)
    mock.assert_awaited_with(ip, port)
    assert isinstance(multiplexer, Multiplexer)
    assert multiplexer.ip == ip
    assert multiplexer.port == port


@patch("asyncio.open_connection")
async def test_close_multiplexer(mock: AsyncMock):
    ip, port = ("127.0.0.1", 7777)
    reader_mock, writer_mock = get_connection_mock(ip, port)
    mock.return_value = (reader_mock, writer_mock)

    multiplexer = await open_multiplexer(ip, port)
    await multiplexer.close()
    writer_mock.write_eof.assert_called()
    writer_mock.wait_closed.assert_awaited()


@patch("asyncio.open_connection")
async def test_multiplexer_contextmanager(mock: AsyncMock):
    ip, port = ("127.0.0.1", 7777)
    reader_mock, writer_mock = get_connection_mock(ip, port)
    mock.return_value = (reader_mock, writer_mock)

    async with open_multiplexer_context(ip, port) as multiplexer:
        mock.assert_awaited_with(ip, port)
        assert isinstance(multiplexer, Multiplexer)
    writer_mock.write_eof.assert_called()
    writer_mock.wait_closed.assert_awaited()


@given(stream_name=text(min_size=1))
async def test_multiplex_one(stream_name: StreamName):
    ip, port = ("127.0.0.1", 7777)
    reader_mock, writer_mock = get_connection_mock(ip, port)
    with patch("asyncio.open_connection", return_value=(reader_mock, writer_mock)):
        async with open_multiplexer_context(ip, port) as multiplexer:
            stream = await multiplexer.multiplex(stream_name)
            assert isinstance(stream, Stream)
            assert stream.ip == ip
            assert stream.port == port
            assert stream.name == stream_name
            assert not stream.is_closed()

            encoded_message = get_encoded_message(
                stream_name, MplexFlag.NEW_STREAM, stream_name.encode()
            )

            writer_mock.write.assert_called_with(encoded_message)


@given(stream_names=lists(text(min_size=1), unique=True, min_size=2))
async def test_multiplex_n(stream_names: List[StreamName]):
    ip, port = ("127.0.0.1", 7777)
    reader_mock, writer_mock = get_connection_mock(ip, port)
    with patch("asyncio.open_connection", return_value=(reader_mock, writer_mock)):
        async with open_multiplexer_context(ip, port) as multiplexer:
            for stream_name in stream_names:
                stream = await multiplexer.multiplex(stream_name)
                assert isinstance(stream, Stream)
                assert stream.ip == ip
                assert stream.port == port
                assert stream.name == stream_name
                assert not stream.is_closed()

                encoded_message = get_encoded_message(
                    stream_name, MplexFlag.NEW_STREAM, stream_name.encode()
                )

                writer_mock.write.assert_called_with(encoded_message)


@patch("asyncio.open_connection")
async def test_errors_after_close_multiplexer(mock: AsyncMock):
    ip, port = ("127.0.0.1", 7777)
    reader_mock, writer_mock = get_connection_mock(ip, port)
    mock.return_value = (reader_mock, writer_mock)

    multiplexer = await open_multiplexer(ip, port)
    await multiplexer.close()
    with pytest.raises(RuntimeError):
        await multiplexer.multiplex("stream.1")
    with pytest.raises(RuntimeError):
        await multiplexer.close()


async def test_multiplex_empty_name():
    ip, port = ("127.0.0.1", 7777)
    reader_mock, writer_mock = get_connection_mock(ip, port)
    with patch("asyncio.open_connection", return_value=(reader_mock, writer_mock)):
        async with open_multiplexer_context(ip, port) as multiplexer:
            with pytest.raises(ValueError):
                stream = await multiplexer.multiplex("")


@patch("asyncio.open_connection")
async def test_close_stream(mock: AsyncMock):
    ip, port = ("127.0.0.1", 7777)
    reader_mock, writer_mock = get_connection_mock(ip, port)
    mock.return_value = (reader_mock, writer_mock)

    async with open_multiplexer_context(ip, port) as multiplexer:
        stream_name = "stream.1"
        stream = await multiplexer.multiplex(stream_name)
        await stream.close()

        encoded_message = get_encoded_message(
            stream_name, MplexFlag.CLOSE, stream_name.encode()
        )
        writer_mock.write.assert_called_with(encoded_message)
        assert stream.is_closed()

        with pytest.raises(RuntimeError):
            await stream.close()
        with pytest.raises(RuntimeError):
            await asyncio.wait_for(stream.read(1), timeout=0.05)
        with pytest.raises(RuntimeError):
            await stream.write(b"data")


@patch("asyncio.open_connection")
async def test_open_multiplex_twice(mock: AsyncMock):
    ip, port = ("127.0.0.1", 7777)
    mock.return_value = get_connection_mock(ip, port)
    async with open_multiplexer_context(ip, port) as multiplexer:
        first_stream = await multiplexer.multiplex("stream.1")
        with pytest.raises(ValueError):
            second_stream = await multiplexer.multiplex("stream.1")

        await first_stream.close()
        second_stream = await multiplexer.multiplex("stream.1")


@given(data=binary())
async def test_write_to_stream(data: bytes):
    ip, port = ("127.0.0.1", 7777)
    reader_mock, writer_mock = get_connection_mock(ip, port)
    with patch("asyncio.open_connection", return_value=(reader_mock, writer_mock)):
        async with open_multiplexer_context(ip, port) as multiplexer:
            stream_name = "stream.1"
            stream = await multiplexer.multiplex(stream_name)
            await stream.write(data)

            encoded_message = get_encoded_message(stream_name, MplexFlag.MESSAGE, data)
            writer_mock.write.assert_called_with(encoded_message)


@patch("asyncio.open_connection")
async def test_read_zero_from_stream(mock: AsyncMock):
    ip, port = ("127.0.0.1", 7777)
    reader_mock, writer_mock = get_connection_mock(ip, port)
    mock.return_value = (reader_mock, writer_mock)
    async with open_multiplexer_context(ip, port) as multiplexer:
        stream_name = "stream.1"
        stream = await multiplexer.multiplex(stream_name)

        encoded_message = get_encoded_message(stream_name, MplexFlag.MESSAGE, b"data")
        reader_mock.feed_data(encoded_message)

        read_data = await asyncio.wait_for(stream.read(0), timeout=0.05)
        assert read_data == b""


@given(data=binary(min_size=1))
async def test_read_from_one_stream(data: StreamData):
    ip, port = ("127.0.0.1", 7777)
    reader_mock, writer_mock = get_connection_mock(ip, port)
    with patch("asyncio.open_connection", return_value=(reader_mock, writer_mock)):
        async with open_multiplexer_context(ip, port) as multiplexer:
            stream_name = "stream.1"
            stream = await multiplexer.multiplex(stream_name)

            encoded_message = get_encoded_message(stream_name, MplexFlag.MESSAGE, data)
            reader_mock.feed_data(encoded_message)

            read_amount = min(
                random.randint(1, 10), len(data)
            )  # read every 'read_amount' bytes
            for i in range(len(data) // read_amount):
                read_data = await asyncio.wait_for(stream.read(read_amount), timeout=1)
                assert (
                    read_data == data[i * read_amount : (i * read_amount) + read_amount]
                )


@given(stream_names=lists(text(min_size=1), unique=True, min_size=2), data=binary())
async def test_read_from_multiple_streams(
    stream_names: List[StreamName], data: StreamData
):
    ip, port = ("127.0.0.1", 7777)
    reader_mock, writer_mock = get_connection_mock(ip, port)
    with patch("asyncio.open_connection", return_value=(reader_mock, writer_mock)):
        async with open_multiplexer_context(ip, port) as multiplexer:
            streams = {
                stream_name: await multiplexer.multiplex(stream_name)
                for stream_name in stream_names
            }
            receiver, non_receiver = random.sample(stream_names, k=2)
            encoded_message = get_encoded_message(receiver, MplexFlag.MESSAGE, data)
            reader_mock.feed_data(encoded_message)

            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(streams[non_receiver].read(1), timeout=0.05)

            assert await streams[receiver].read(len(data)) == data


async def test_read_invalid_flag():
    ip, port = ("127.0.0.1", 7777)
    reader_mock, writer_mock = get_connection_mock(ip, port)
    with patch("asyncio.open_connection", return_value=(reader_mock, writer_mock)):
        async with open_multiplexer_context(ip, port) as multiplexer:
            stream_name = "stream.1"
            stream = await multiplexer.multiplex(stream_name)

            encoded_message = get_encoded_message(stream_name, 4, b"data")
            reader_mock.feed_data(encoded_message)

            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(stream.read(1), timeout=0.05)


async def test_read_close_flag():
    ip, port = ("127.0.0.1", 7777)
    reader_mock, writer_mock = get_connection_mock(ip, port)
    with patch("asyncio.open_connection", return_value=(reader_mock, writer_mock)):
        async with open_multiplexer_context(ip, port) as multiplexer:
            stream_name = "stream.1"
            stream = await multiplexer.multiplex(stream_name)

            encoded_message = get_encoded_message(stream_name, MplexFlag.CLOSE, b"")
            reader_mock.feed_data(encoded_message)

            await asyncio.sleep(0.05)
            # 'read' must be first so that it gives control back to event_loop
            with pytest.raises(RuntimeError):
                await stream.close()
            with pytest.raises(RuntimeError):
                await stream.write(b"data")
            with pytest.raises(RuntimeError):
                await asyncio.wait_for(stream.read(1), timeout=0.05)


async def test_read_until_close():
    ip, port = ("127.0.0.1", 7777)
    reader_mock, writer_mock = get_connection_mock(ip, port)
    with patch("asyncio.open_connection", return_value=(reader_mock, writer_mock)):
        async with open_multiplexer_context(ip, port) as multiplexer:
            stream_name = "stream.1"
            stream = await multiplexer.multiplex(stream_name)

            data = b"data"
            encoded_message = get_encoded_message(stream_name, MplexFlag.MESSAGE, data)
            reader_mock.feed_data(encoded_message)
            encoded_message = get_encoded_message(stream_name, MplexFlag.CLOSE, b"")
            reader_mock.feed_data(encoded_message)

            read_data = await asyncio.wait_for(stream.read(), timeout=0.05)
            assert read_data == data


async def test_close_from_multiplexer():
    ip, port = ("127.0.0.1", 7777)
    reader_mock, writer_mock = get_connection_mock(ip, port)
    with patch("asyncio.open_connection", return_value=(reader_mock, writer_mock)):
        multiplexer = await open_multiplexer(ip, port)
        stream_name = "stream.1"
        stream = await multiplexer.multiplex(stream_name)

        await multiplexer.close()
        with pytest.raises(RuntimeError):
            await stream.close()


@given(stream_names=lists(text(min_size=1), unique=True))
async def test_handle_new_stream(stream_names):
    ip, port = ("127.0.0.1", 7777)
    handled_events = {name: asyncio.Event() for name in stream_names}

    async def handler(stream_name: StreamName, stream: Stream):
        assert stream.name == stream_name
        assert stream.ip == ip
        assert stream.port == port
        handled_events[stream_name].set()

    reader_mock, writer_mock = get_connection_mock(ip, port)
    with patch("asyncio.open_connection", return_value=(reader_mock, writer_mock)):
        async with open_multiplexer_context(ip, port) as multiplexer:
            for stream_name in stream_names:
                multiplexer.set_handler(stream_name, partial(handler, stream_name))

            for stream_name in stream_names:
                encoded_message = get_encoded_message(
                    stream_name, MplexFlag.NEW_STREAM, stream_name.encode()
                )
                reader_mock.feed_data(encoded_message)

                await asyncio.wait_for(handled_events[stream_name].wait(), timeout=0.05)


async def test_remove_handler():
    ip, port = ("127.0.0.1", 7777)
    stream_name = "stream.1"
    handled_event = asyncio.Event()

    async def handler(stream: Stream):
        assert stream.name == stream_name
        assert stream.ip == ip
        assert stream.port == port
        handled_event.set()

    reader_mock, writer_mock = get_connection_mock(ip, port)
    with patch("asyncio.open_connection", return_value=(reader_mock, writer_mock)):
        async with open_multiplexer_context(ip, port) as multiplexer:
            multiplexer.set_handler(stream_name, handler)
            multiplexer.remove_handler(stream_name)

            encoded_message = get_encoded_message(
                stream_name, MplexFlag.NEW_STREAM, stream_name.encode()
            )
            reader_mock.feed_data(encoded_message)

            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(handled_event.wait(), timeout=0.05)
            with pytest.raises(KeyError):
                multiplexer.remove_handler(stream_name)


@given(data=binary())
async def test_readline(data: bytes):
    ip, port = ("127.0.0.1", 7777)
    reader_mock, writer_mock = get_connection_mock(ip, port)
    with patch("asyncio.open_connection", return_value=(reader_mock, writer_mock)):
        multiplexer = await open_multiplexer(ip, port)
        stream_name = "stream.1"
        stream = await multiplexer.multiplex(stream_name)

        data = data + b"\n" + data
        encoded_message = get_encoded_message(stream_name, MplexFlag.MESSAGE, data)
        reader_mock.feed_data(encoded_message)

        assert await stream.readline() == data.split(b"\n")[0] + b"\n"
