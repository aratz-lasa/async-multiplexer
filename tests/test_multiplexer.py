import random
import asyncio
from hashlib import sha256
from unittest.mock import patch, AsyncMock, MagicMock
from typing import List

import pytest
import uvarint
from hypothesis import given
from hypothesis.strategies import text, binary, lists

from aio_multiplexer import open_tcp_multiplexer, open_tcp_multiplexer_context
from aio_multiplexer.multiplexer import Multiplexer, Stream, StreamName, StreamData
from aio_multiplexer.protocol import MplexFlag, MplexMessage, StreamID, MplexProtocol
from tests.conftest import get_tcp_connection_mock, get_protocol_mock

# All test coroutines will be treated as marked.
pytestmark = pytest.mark.asyncio


def get_stream_id_from_name(stream_name: StreamName) -> StreamID:
    hash_value = sha256()
    hash_value.update(stream_name.encode())
    stream_id = int.from_bytes(hash_value.digest(), "big")
    return stream_id


def get_encoded_message(
    stream_name: StreamName, flag: MplexFlag, data: StreamData
) -> bytes:
    encoded_message = (
        uvarint.encode(get_stream_id_from_name(stream_name) << 3 | flag)
        + uvarint.encode(len(data))
        + data
    )
    return encoded_message


def get_message(
    stream_name: StreamName, flag: MplexFlag, data: StreamData
) -> MplexMessage:
    return MplexMessage(
        stream_id=get_stream_id_from_name(stream_name), flag=flag, data=data
    )


@patch("asyncio.open_connection")
async def test_open_tcp_multiplexer(tcp_mock: AsyncMock):
    tcp_mock.return_value = get_tcp_connection_mock()

    ip, port = ("127.0.0.1", 7777)
    multiplexer = await open_tcp_multiplexer(ip, port)
    tcp_mock.assert_awaited_with(ip, port)
    assert isinstance(multiplexer, Multiplexer)


@patch("asyncio.open_connection")
async def test_close_tcp_multiplexer(tcp_mock: AsyncMock):
    reader_mock, writer_mock = get_tcp_connection_mock()
    tcp_mock.return_value = (reader_mock, writer_mock)

    multiplexer = await open_tcp_multiplexer("127.0.0.1", 7777)
    await multiplexer.close()
    writer_mock.write_eof.assert_called()
    writer_mock.wait_closed.assert_awaited()


@patch("asyncio.open_connection")
async def test_tcp_multiplexer_contextmanager(tcp_mock: AsyncMock):
    reader_mock, writer_mock = get_tcp_connection_mock()
    tcp_mock.return_value = (reader_mock, writer_mock)

    ip, port = "127.0.0.1", 7777
    async with open_tcp_multiplexer_context(ip, port) as multiplexer:
        tcp_mock.assert_awaited_with(ip, port)
        assert isinstance(multiplexer, Multiplexer)
    writer_mock.write_eof.assert_called()
    writer_mock.wait_closed.assert_awaited()


@given(stream_name=text(min_size=1))
async def test_multiplex_one(stream_name: StreamName):
    reader_mock, writer_mock = get_tcp_connection_mock()
    with patch("asyncio.open_connection", return_value=(reader_mock, writer_mock)):
        async with open_tcp_multiplexer_context("127.0.0.1", 7777) as multiplexer:
            stream = await multiplexer.multiplex(stream_name)
            assert isinstance(stream, Stream)

            encoded_message = get_encoded_message(
                stream_name, MplexFlag.NEW_STREAM, stream_name.encode()
            )

            writer_mock.write.assert_called_with(encoded_message)


@given(stream_names=lists(text(min_size=1), unique=True, min_size=2))
async def test_multiplex_n(stream_names: List[StreamName]):
    reader_mock, writer_mock = get_tcp_connection_mock()
    with patch("asyncio.open_connection", return_value=(reader_mock, writer_mock)):
        async with open_tcp_multiplexer_context("127.0.0.1", 7777) as multiplexer:
            for stream_name in stream_names:
                stream = await multiplexer.multiplex(stream_name)
                assert isinstance(stream, Stream)

                encoded_message = get_encoded_message(
                    stream_name, MplexFlag.NEW_STREAM, stream_name.encode()
                )

                writer_mock.write.assert_called_with(encoded_message)


async def test_multiplex_empty_name():
    reader_mock, writer_mock = get_tcp_connection_mock()
    with patch("asyncio.open_connection", return_value=(reader_mock, writer_mock)):
        async with open_tcp_multiplexer_context("127.0.0.1", 7777) as multiplexer:
            with pytest.raises(ValueError):
                stream = await multiplexer.multiplex("")


@patch("asyncio.open_connection")
async def test_close_stream(tcp_mock: AsyncMock):
    reader_mock, writer_mock = get_tcp_connection_mock()
    tcp_mock.return_value = (reader_mock, writer_mock)

    async with open_tcp_multiplexer_context("127.0.0.1", 7777) as multiplexer:
        stream_name = "stream.1"
        stream = await multiplexer.multiplex(stream_name)
        await stream.close()
        encoded_message = get_encoded_message(
            stream_name, MplexFlag.CLOSE, stream_name.encode()
        )
        writer_mock.write.assert_called_with(encoded_message)

        with pytest.raises(RuntimeError):
            await stream.close()
        with pytest.raises(RuntimeError):
            await asyncio.wait_for(stream.read(1), timeout=0.01)
        with pytest.raises(RuntimeError):
            await stream.write(b"data")


@patch("asyncio.open_connection")
async def test_open_multiplex_twice(tcp_mock: AsyncMock):
    tcp_mock.return_value = get_tcp_connection_mock()
    async with open_tcp_multiplexer_context("127.0.0.1", 7777) as multiplexer:
        first_stream = await multiplexer.multiplex("stream.1")
        with pytest.raises(ValueError):
            second_stream = await multiplexer.multiplex("stream.1")

        await first_stream.close()
        second_stream = await multiplexer.multiplex("stream.1")


@given(data=binary())
async def test_write_to_stream(data: bytes):
    reader_mock, writer_mock = get_tcp_connection_mock()
    with patch("asyncio.open_connection", return_value=(reader_mock, writer_mock)):
        async with open_tcp_multiplexer_context("127.0.0.1", 7777) as multiplexer:
            stream_name = "stream.1"
            stream = await multiplexer.multiplex(stream_name)
            await stream.write(data)

            encoded_message = get_encoded_message(stream_name, MplexFlag.MESSAGE, data)
            writer_mock.write.assert_called_with(encoded_message)


@patch("asyncio.open_connection", return_value=get_tcp_connection_mock())
async def test_read_zero_from_stream(tcp_mock: AsyncMock):
    reader_mock, writer_mock = get_tcp_connection_mock()
    tcp_mock.return_value = (reader_mock, writer_mock)
    async with open_tcp_multiplexer_context("127.0.0.1", 7777) as multiplexer:
        stream_name = "stream.1"
        stream = await multiplexer.multiplex(stream_name)

        encoded_message = get_encoded_message(stream_name, MplexFlag.MESSAGE, b"data")
        reader_mock.feed_data(encoded_message)

        read_data = await asyncio.wait_for(stream.read(0), timeout=0.01)
        assert read_data == b""


@given(data=binary(min_size=1))
async def test_read_from_one_stream(data: StreamData):
    reader_mock, writer_mock = get_tcp_connection_mock()
    with patch("asyncio.open_connection", return_value=(reader_mock, writer_mock)):
        async with open_tcp_multiplexer_context("127.0.0.1", 7777) as multiplexer:
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


@given(data=binary(min_size=1))
async def test_read_too_much_from_stream(data: StreamData):
    reader_mock, writer_mock = get_tcp_connection_mock()
    with patch("asyncio.open_connection", return_value=(reader_mock, writer_mock)):
        async with open_tcp_multiplexer_context("127.0.0.1", 7777) as multiplexer:
            stream_name = "stream.1"
            stream = await multiplexer.multiplex(stream_name)

            encoded_message = get_encoded_message(stream_name, MplexFlag.MESSAGE, data)
            reader_mock.feed_data(encoded_message)

            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(stream.read(len(data) + 1), timeout=0.01)

            reader_mock.feed_data(encoded_message)
            read_data = await asyncio.wait_for(stream.read(len(data) + 1), timeout=0.01)
            assert read_data == data + data[:1]


@given(stream_names=lists(text(min_size=1), unique=True, min_size=2), data=binary())
async def test_read_from_multiple_streams(
    stream_names: List[StreamName], data: StreamData
):
    reader_mock, writer_mock = get_tcp_connection_mock()
    with patch("asyncio.open_connection", return_value=(reader_mock, writer_mock)):
        async with open_tcp_multiplexer_context("127.0.0.1", 7777) as multiplexer:
            streams = {
                stream_name: await multiplexer.multiplex(stream_name)
                for stream_name in stream_names
            }
            receiver, non_receiver = random.sample(stream_names, k=2)
            encoded_message = get_encoded_message(receiver, MplexFlag.MESSAGE, data)
            reader_mock.feed_data(encoded_message)

            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(streams[non_receiver].read(1), timeout=0.01)

            assert await streams[receiver].read(len(data)) == data


async def test_read_invalid_flag():
    reader_mock, writer_mock = get_tcp_connection_mock()
    with patch("asyncio.open_connection", return_value=(reader_mock, writer_mock)):
        async with open_tcp_multiplexer_context("127.0.0.1", 7777) as multiplexer:
            stream_name = "stream.1"
            stream = await multiplexer.multiplex(stream_name)

            encoded_message = get_encoded_message(
                stream_name, MplexFlag.NEW_STREAM, b"data"
            )
            reader_mock.feed_data(encoded_message)

            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(stream.read(1), timeout=0.01)


async def test_read_close_flag():
    reader_mock, writer_mock = get_tcp_connection_mock()
    with patch("asyncio.open_connection", return_value=(reader_mock, writer_mock)):
        async with open_tcp_multiplexer_context("127.0.0.1", 7777) as multiplexer:
            stream_name = "stream.1"
            stream = await multiplexer.multiplex(stream_name)

            encoded_message = get_encoded_message(stream_name, MplexFlag.CLOSE, b"")
            reader_mock.feed_data(encoded_message)

            # 'read' must be first so that it gives control back to event_loop
            with pytest.raises(RuntimeError):
                await asyncio.wait_for(stream.read(1), timeout=0.01)
            with pytest.raises(RuntimeError):
                await stream.close()
            with pytest.raises(RuntimeError):
                await stream.write(b"data")
