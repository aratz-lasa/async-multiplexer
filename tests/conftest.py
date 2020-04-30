from asyncio import StreamReader, StreamWriter
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def connection_mock(event_loop):
    reader_mock, writer_mock = (
        MagicMock(spec_set=StreamReader, wraps=StreamReader(loop=event_loop)),
        MagicMock(spec_set=StreamWriter),
    )
    yield reader_mock, writer_mock
