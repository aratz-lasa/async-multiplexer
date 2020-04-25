from asyncio import StreamReader, StreamWriter
from dataclasses import dataclass
from enum import IntEnum
from typing import Tuple

import uvarint

LIMIT = 1000

StreamID = int
StreamData = bytes


class MplexFlag(IntEnum):
    NEW_STREAM = 0
    MESSAGE_RECEIVER = 1
    MESSAGE_INITIATOR = 2
    CLOSE_RECEIVER = 3
    CLOSE_INITIATOR = 4
    RESET_RECEIVER = 5
    RESET_INITIATOR = 6


@dataclass
class MplexMessage:
    stream_id: StreamID
    flag: MplexFlag
    data: StreamData


class MplexProtocol:
    def __init__(self, reader: StreamReader, writer: StreamWriter):
        self._reader = reader
        self._writer = writer

    async def read_message(self) -> MplexMessage:
        header = await self._read_uvarint()
        stream_id, flag = self._decode_header(header)
        data_length = await self._read_uvarint()
        data = await self._reader.read(data_length)
        return MplexMessage(stream_id=stream_id, flag=flag, data=data)

    async def write_message(self, message: MplexMessage):
        encoded_message = self._encode_message(message)
        self._writer.write(encoded_message)
        await self._writer.drain()

    async def _read_uvarint(self, limit: int = LIMIT) -> int:
        integer = 0
        position = 0

        i = 0
        byte: int = int.from_bytes(await self._reader.read(1), byteorder="big")

        while 0b1000_0000 <= byte:
            if byte < 0b1000_0000:
                break

            integer |= (byte & 0b0111_1111) << position
            position += 7

            if position / 7 >= limit:
                raise OverflowError("integer > {} bytes".format(limit))

            byte = int.from_bytes(await self._reader.read(1), byteorder="big")
            i += 1

        return integer | (byte << position)

    def _decode_header(self, header: int) -> Tuple[StreamID, MplexFlag]:
        flag = MplexFlag(header & 0x07)
        stream_id = header >> 3
        return stream_id, flag

    def _encode_message(self, message: MplexMessage) -> bytes:
        return (
            uvarint.encode(message.stream_id << 3 | message.flag)
            + uvarint.encode(len(message.data))
            + message.data
        )
