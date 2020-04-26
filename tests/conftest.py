from typing import Tuple
from asyncio import StreamReader, StreamWriter
from unittest.mock import MagicMock

from aio_multiplexer.protocol import MplexProtocol, MplexMessage, MplexFlag
import pytest


@pytest.fixture
def tcp_connection_mock(event_loop):
    reader_mock, writer_mock = (
        MagicMock(spec_set=StreamReader, wraps=StreamReader(loop=event_loop)),
        MagicMock(spec_set=StreamWriter),
    )
    yield reader_mock, writer_mock


def get_tcp_connection_mock() -> Tuple[MagicMock, MagicMock]:
    reader_mock, writer_mock = (
        MagicMock(spec_set=StreamReader, wraps=StreamReader()),
        MagicMock(spec_set=StreamWriter),
    )
    return reader_mock, writer_mock


def get_protocol_mock() -> MagicMock:
    protocol_mock = MagicMock(spec_set=MplexProtocol)
    protocol_mock.read_message.return_value = MplexMessage(
        stream_id=0, flag=MplexFlag.MESSAGE, data=b""
    )  # generic data
    return protocol_mock
