import os
import asyncio
import aioconsole
import logging
import structlog

logger = structlog.get_logger()

from nats_transport.nats_transport import NATSClient

nats = NATSClient("nats://localhost", os.getenv("NAME", "a"))


async def main():
    await nats.connect()

    while True:
        msg = await aioconsole.ainput(">> ")
        parts = msg.split(" ")
        cmd = parts[0]
        if cmd == "pub":
            sub = parts[1]
            data = " ".join(parts[2:])
            await nats.publish(sub, data)
        if cmd == "sub":
            sub = parts[1]
            async def cb(data):
                logger.info("data recieved", data=data)
            await nats.subscribe(sub, handler=cb)


asyncio.run(main())
