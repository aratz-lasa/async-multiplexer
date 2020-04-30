from asyncio.streams import StreamReader, StreamWriter
from hashlib import sha256
from typing import Tuple
from unittest.mock import MagicMock

import uvarint

from aio_multiplexer.multiplexer import StreamName
from aio_multiplexer.protocol import (
    StreamID,
    MplexFlag,
    StreamData,
    MplexMessage,
    MplexProtocol,
)


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


def get_connection_mock(ip, port) -> Tuple[MagicMock, MagicMock]:
    reader_mock, writer_mock = (
        MagicMock(spec_set=StreamReader, wraps=StreamReader()),
        MagicMock(spec_set=StreamWriter),
    )
    writer_mock.get_extra_info.return_value = (ip, port)
    return reader_mock, writer_mock


def get_protocol_mock() -> MagicMock:
    protocol_mock = MagicMock(spec_set=MplexProtocol)
    protocol_mock.read_message.return_value = MplexMessage(
        stream_id=0, flag=MplexFlag.MESSAGE, data=b""
    )  # generic data
    return protocol_mock
