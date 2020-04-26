from typing import List, Tuple
import pytest
import uvarint
from hypothesis import given
from hypothesis.strategies import binary, sampled_from, integers, tuples, lists

from aio_multiplexer.protocol import MplexFlag, MplexMessage, StreamData, StreamID
from aio_multiplexer.protocol import MplexProtocol
from tests.conftest import get_tcp_connection_mock

# All test coroutines will be treated as marked.
pytestmark = pytest.mark.asyncio


def test_flags():
    assert MplexFlag.NEW_STREAM == 0
    assert MplexFlag.MESSAGE == 1
    assert MplexFlag.CLOSE == 2


def test_create_mplex_protocol():
    mplex_protocol = MplexProtocol(*get_tcp_connection_mock())
    assert isinstance(mplex_protocol, MplexProtocol)


@given(
    fragmented_message=tuples(integers(min_value=0), sampled_from(MplexFlag), binary())
)
async def test_read_message(fragmented_message: Tuple[StreamID, MplexFlag, StreamData]):
    reader_mock, writer_mock = get_tcp_connection_mock()
    stream_id, flag, data = fragmented_message

    mplex_protocol = MplexProtocol(reader_mock, writer_mock)
    encoded_message = (
        uvarint.encode(stream_id << 3 | flag) + uvarint.encode(len(data)) + data
    )
    reader_mock.feed_data(encoded_message)
    message = await mplex_protocol.read_message()
    assert isinstance(message, MplexMessage)
    assert message.stream_id == stream_id
    assert message.flag == flag
    assert message.data == data


@given(
    fragmented_messages=lists(
        tuples(integers(min_value=0), sampled_from(MplexFlag), binary()), min_size=2
    )
)
async def test_read_multiple_messages(
    fragmented_messages: List[Tuple[StreamID, MplexFlag, StreamData]]
):
    reader_mock, writer_mock = get_tcp_connection_mock()
    for stream_id, flag, data in fragmented_messages:
        encoded_message = (
            uvarint.encode(stream_id << 3 | flag) + uvarint.encode(len(data)) + data
        )
        reader_mock.feed_data(encoded_message)

    mplex_protocol = MplexProtocol(reader_mock, writer_mock)
    for stream_id, flag, data in fragmented_messages:
        message = await mplex_protocol.read_message()
        assert isinstance(message, MplexMessage)
        assert message.stream_id == stream_id
        assert message.flag == flag
        assert message.data == data


@given(
    fragmented_message=tuples(integers(min_value=0), sampled_from(MplexFlag), binary())
)
async def test_write_message(
    fragmented_message: Tuple[StreamID, MplexFlag, StreamData]
):
    reader_mock, writer_mock = get_tcp_connection_mock()
    stream_id, flag, data = fragmented_message

    mplex_protocol = MplexProtocol(reader_mock, writer_mock)
    await mplex_protocol.write_message(
        MplexMessage(stream_id=stream_id, flag=flag, data=data)
    )
    encoded_message = (
        uvarint.encode(stream_id << 3 | flag) + uvarint.encode(len(data)) + data
    )

    writer_mock.write.assert_called_with(encoded_message)
    writer_mock.drain.assert_awaited()
