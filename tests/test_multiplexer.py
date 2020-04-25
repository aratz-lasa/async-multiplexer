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
    stream_id: StreamID, flag: MplexFlag, data: StreamData
) -> bytes:
    encoded_message = (
        uvarint.encode(stream_id << 3 | flag) + uvarint.encode(len(data)) + data
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


@given(stream_name=text())
async def test_multiplex_one(stream_name: StreamName):
    protocol_mock = get_protocol_mock()
    with patch(
        "aio_multiplexer.multiplexer.MplexProtocol", return_value=protocol_mock
    ), patch("asyncio.open_connection", return_value=get_tcp_connection_mock()):

        async with open_tcp_multiplexer_context("127.0.0.1", 7777) as multiplexer:
            stream = await multiplexer.multiplex(stream_name)
            assert isinstance(stream, Stream)

            message = get_message(
                stream_name, MplexFlag.NEW_STREAM, stream_name.encode()
            )

            protocol_mock.write_message.assert_awaited_with(message)


@given(stream_names=lists(text(), unique=True))
async def test_multiplex_n(stream_names: List[StreamName]):
    protocol_mock = get_protocol_mock()
    with patch(
        "aio_multiplexer.multiplexer.MplexProtocol", return_value=protocol_mock
    ), patch("asyncio.open_connection", return_value=get_tcp_connection_mock()):

        async with open_tcp_multiplexer_context("127.0.0.1", 7777) as multiplexer:
            for stream_name in stream_names:
                stream = await multiplexer.multiplex(stream_name)
                assert isinstance(stream, Stream)

                message = get_message(
                    stream_name, MplexFlag.NEW_STREAM, stream_name.encode()
                )

                protocol_mock.write_message.assert_awaited_with(message)


@patch("aio_multiplexer.multiplexer.MplexProtocol")
@patch("asyncio.open_connection", return_value=get_tcp_connection_mock())
async def test_close_stream(tcp_mock: AsyncMock, protocol_class_mock: MagicMock):
    protocol_mock = get_protocol_mock()
    protocol_class_mock.return_value = protocol_mock

    async with open_tcp_multiplexer_context("127.0.0.1", 7777) as multiplexer:
        stream_name = "stream.1"
        stream = await multiplexer.multiplex(stream_name)
        await stream.close()
        message = get_message(
            stream_name, MplexFlag.CLOSE_INITIATOR, stream_name.encode()
        )
        protocol_mock.write_message.assert_awaited_with(message)

        with pytest.raises(RuntimeError):
            await stream.close()


@patch("aio_multiplexer.multiplexer.MplexProtocol")
@patch("asyncio.open_connection", return_value=get_tcp_connection_mock())
async def test_close_stream_from_multiplexer(
    tcp_mock: AsyncMock, protocol_class_mock: MagicMock
):
    protocol_mock = get_protocol_mock()
    protocol_class_mock.return_value = protocol_mock

    async with open_tcp_multiplexer_context("127.0.0.1", 7777) as multiplexer:
        stream_name = "stream.1"
        await multiplexer.multiplex(stream_name)
        await multiplexer.close_stream(stream_name)
        message = get_message(
            stream_name, MplexFlag.CLOSE_INITIATOR, stream_name.encode()
        )
        protocol_mock.write_message.assert_awaited_with(message)

        with pytest.raises(RuntimeError):
            await multiplexer.close_stream(stream_name)


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
    protocol_mock = get_protocol_mock()
    with patch(
        "aio_multiplexer.multiplexer.MplexProtocol", return_value=protocol_mock
    ), patch("asyncio.open_connection", return_value=get_tcp_connection_mock()):
        async with open_tcp_multiplexer_context("127.0.0.1", 7777) as multiplexer:
            stream_name = "stream.1"
            stream = await multiplexer.multiplex(stream_name)
            await stream.write(data)

            message = get_message(stream_name, MplexFlag.MESSAGE_INITIATOR, data)
            protocol_mock.write_message.assert_awaited_with(message)


@patch("aio_multiplexer.multiplexer.MplexProtocol")
@patch("asyncio.open_connection", return_value=get_tcp_connection_mock())
async def test_read_zero_from_stream(
    tcp_mock: AsyncMock, protocol_class_mock: MagicMock
):
    protocol_mock = get_protocol_mock()
    protocol_mock.read_message.return_value = MplexMessage(
        0, MplexFlag.MESSAGE_RECEIVER, b""
    )
    protocol_class_mock.return_value = protocol_mock

    async with open_tcp_multiplexer_context("127.0.0.1", 7777) as multiplexer:
        stream_name = "stream.1"
        stream = await multiplexer.multiplex(stream_name)

        message = get_message(stream_name, MplexFlag.MESSAGE_RECEIVER, b"data")
        protocol_mock.read_message.return_value = message

        read_data = await asyncio.wait_for(stream.read(0), timeout=0.5)
        assert read_data == b""


@given(data=binary(min_size=1))
async def test_read_from_one_stream(data: StreamData):
    protocol_mock = get_protocol_mock()
    with patch(
        "asyncio.open_connection", return_value=get_tcp_connection_mock()
    ), patch("aio_multiplexer.multiplexer.MplexProtocol", return_value=protocol_mock):
        async with open_tcp_multiplexer_context("127.0.0.1", 7777) as multiplexer:
            stream_name = "stream.1"
            stream = await multiplexer.multiplex(stream_name)

            message = get_message(stream_name, MplexFlag.MESSAGE_RECEIVER, data)
            protocol_mock.read_message.return_value = message

            read_amount = min(
                random.randint(1, 10), len(data)
            )  # read every 'read_amount' bytes
            for i in range(len(data) // read_amount):
                read_data = await asyncio.wait_for(stream.read(read_amount), timeout=1)
                assert (
                    read_data == data[i * read_amount : (i * read_amount) + read_amount]
                )


async def test_read_from_multiple_streams():
    protocol_mock = get_protocol_mock()
    with patch(
        "asyncio.open_connection", return_value=get_tcp_connection_mock()
    ), patch("aio_multiplexer.multiplexer.MplexProtocol", return_value=protocol_mock):
        async with open_tcp_multiplexer_context("127.0.0.1", 7777) as multiplexer:
            stream_name_1 = "stream.1"
            stream_name_2 = "stream.2"
            stream_1 = await multiplexer.multiplex(stream_name_1)
            stream_2 = await multiplexer.multiplex(stream_name_2)

            data = b"data"
            message = get_message(stream_name_1, MplexFlag.MESSAGE_RECEIVER, data)
            protocol_mock.read_message.return_value = message

            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(stream_2.read(1), timeout=0.5)
