# AioMultiplexer
[![Python 3.7](https://img.shields.io/badge/python-3.7-blue.svg)](https://www.python.org/downloads/release/python-370/)
[![PEP8](https://img.shields.io/badge/code%20style-pep8-orange.svg)](https://www.python.org/dev/peps/pep-0008/)
[![black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)


TCP Multiplexer based on [Mplex](https://github.com/libp2p/specs/tree/master/mplex) protocol, but simplified.
It is intended for creating mutiple streams in parallel
on top of a same TCP connection.

## Dependencies
The needed third-party libraries are:
- uvarint==1.2.0 

## Usage

### Client
```python
import asyncio
from aio_multiplexer import open_multiplexer_context

async def echo_client():
    async with open_multiplexer_context("127.0.0.1", 7777) as multiplexer:
        stream_echo_1 = await multiplexer.multiplex("echo.1")
        stream_echo_2 = await multiplexer.multiplex("echo.2")
        await stream_echo_1.write(b"echo.1")
        await stream_echo_2.write(b"echo.2")


if __name__ == "__main__":
    asyncio.run(echo_client())
```

### Server
```python
import asyncio
from aio_multiplexer import bind_multiplex_listener_context

async def handler(stream):
    data = await stream.read()
    print(data)

async def echo_server():
    async with bind_multiplex_listener_context("127.0.0.1", 7777) as listener:
        listener.set_handler("echo.1", handler)
        listener.set_handler("echo.2", handler)
        await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(echo_server())
```