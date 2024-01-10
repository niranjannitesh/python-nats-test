from nats.aio.client import Client as NATS
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig
import msgpack
import uuid
from typing import Any, Callable, Awaitable, List, Optional, TypedDict
import structlog

logger = structlog.get_logger()


class RequestData(TypedDict):
    data: Any
    trace_id: str


class ResponseData(TypedDict):
    data: Any
    error: Optional[Exception]
    trace_id: str


class NATSClient:
    """
    A NATS client that can be used to publish, subscribe and request messages."""

    def __init__(self, servers: str | List[str], name: str):
        self._name = name
        self._jsm: JetStreamContext = None
        self._nats_client = NATS()
        self._servers = servers

    async def connect(self):
        await self._nats_client.connect(servers=self._servers)
        self._jsm = self._nats_client.jetstream()
        logger.info("nats connected", servers=self._servers)

    async def disconnect(self):
        await self._nats_client.close()

    async def publish(self, subject: str, data: Any):
        trace_id = structlog.contextvars.get_contextvars().get("trace_id")
        if not trace_id:
            logger.info("no trace_id found, generating one")
            trace_id = str(uuid.uuid4())
            structlog.contextvars.bind_contextvars(trace_id=trace_id)
        logger.info("nats bus event published", subject=subject)
        await self._create_stream(subject)
        encoded_data = self._encode({"data": data, "trace_id": trace_id})
        await self._jsm.publish(subject, encoded_data)

    async def subscribe(
        self, subject: str, handler: Callable[[RequestData], Awaitable[None]]
    ):
        await self._create_stream(subject)
        extra_joined = "-".join(subject.split(".")[1:])
        durable_name_with_extra = self._name + "-" + extra_joined

        async def subscription_handler(msg):
            request_data: RequestData = self._decode(msg.data)
            structlog.contextvars.bind_contextvars(trace_id=request_data["trace_id"])
            logger.info("nats bus event received", subject=subject)
            await handler(request_data["data"])
            await msg.ack()

        await self._jsm.subscribe(
            subject,
            cb=subscription_handler,
            config=ConsumerConfig(
                name=durable_name_with_extra,
                durable_name=durable_name_with_extra,
                deliver_subject=subject,
                ack_policy="explicit",
            ),
            queue=durable_name_with_extra,
        )

    # async def request(self, subject: str, data: Any) -> ResponseData:
    #     trace_id = contextvars.ContextVar("trace_id", default=str(uuid.uuid4()))
    #     encoded_data = self._encode({
    #         "data": data, "trace_id": trace_id.get()
    #     })
    #     msg = await self._nats_client.request("x" + subject, encoded_data, 30)
    #     return self._decode(msg.data)

    # async def reply(
    #     self, subject: str, handler: Callable[[RequestData], Awaitable[ResponseData]]
    # ):
    #     async def reply_handler(msg):
    #         request_data: RequestData = self._decode(msg.data)
    #         response_data = await handler(request_data)
    #         encoded_response = self._encode(response_data)
    #         await msg.respond(encoded_response)

    #     await self._nats_client.subscribe("x" + subject, cb=reply_handler)

    async def _create_stream(self, subject: str):
        stream_name = subject.split(".")[0]
        streams = await self._jsm.streams_info()
        stream_exists = False
        for s in streams:
            if s.config.name == stream_name:
                stream_exists = True
                break
        if not stream_exists:
            logger.info(
                "creating stream",
                stream_name=stream_name,
                subjects=[stream_name + ".>"],
            )
            await self._jsm.add_stream(name=stream_name, subjects=[stream_name + ".>"])
        return stream_name

    def _encode(self, data: Any) -> bytes:
        return msgpack.packb(data, use_bin_type=True)

    def _decode(self, encoded_data: bytes) -> Any:
        return msgpack.unpackb(encoded_data, raw=False)
