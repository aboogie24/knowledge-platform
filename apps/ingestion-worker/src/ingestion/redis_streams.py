"""Redis Streams helpers for enqueueing and consuming events."""

import asyncio
import json
import uuid
from typing import Any, Awaitable, Callable

import structlog
from redis import asyncio as redis_async

logger = structlog.get_logger()


class RedisStreams:
    """Thin wrapper around Redis Streams for ingestion events."""

    def __init__(
        self,
        redis_url: str,
        stream: str,
        dlq_stream: str,
        group: str,
        consumer_name: str,
    ):
        self.redis_url = redis_url
        self.stream = stream
        self.dlq_stream = dlq_stream
        self.group = group
        self.consumer_name = consumer_name
        self.redis: redis_async.Redis | None = None
        self._stop = asyncio.Event()

    async def connect(self):
        """Connect and ensure consumer group exists."""
        self.redis = redis_async.from_url(self.redis_url, decode_responses=True)
        try:
            await self.redis.xgroup_create(self.stream, self.group, id="$", mkstream=True)
            logger.info("redis_group_created", stream=self.stream, group=self.group)
        except Exception as exc:
            # Group may already exist
            if "BUSYGROUP" in str(exc):
                logger.info("redis_group_exists", stream=self.stream, group=self.group)
            else:
                raise

    async def enqueue(self, event: dict[str, Any]) -> str:
        """Enqueue an event onto the stream."""
        if not self.redis:
            raise RuntimeError("Redis not connected")

        event_id = event.get("id") or str(uuid.uuid4())
        event["id"] = event_id

        redis_id = await self.redis.xadd(
            self.stream,
            {"data": json.dumps(event)},
            id="*",
        )
        logger.debug("redis_event_enqueued", event_id=event_id, redis_id=redis_id)
        return redis_id

    async def consume_forever(self, handler: Callable[[dict[str, Any]], Awaitable[None]], block_ms: int = 5000):
        """Consume events using XREADGROUP and dispatch to handler."""
        if not self.redis:
            raise RuntimeError("Redis not connected")

        while not self._stop.is_set():
            try:
                messages = await self.redis.xreadgroup(
                    groupname=self.group,
                    consumername=self.consumer_name,
                    streams={self.stream: ">"},
                    count=10,
                    block=block_ms,
                )
                if not messages:
                    continue

                for _, entries in messages:
                    for entry_id, fields in entries:
                        raw = fields.get("data")
                        try:
                            event = json.loads(raw)
                            await handler(event)
                            await self.redis.xack(self.stream, self.group, entry_id)
                            logger.debug("redis_event_ack", event_id=event.get("id"), entry_id=entry_id)
                        except Exception as exc:
                            logger.warning(
                                "redis_event_failed",
                                error=str(exc),
                                entry_id=entry_id,
                            )
                            await self._to_dlq(raw, error=str(exc))
                            await self.redis.xack(self.stream, self.group, entry_id)
            except Exception as exc:
                logger.error("redis_consumer_error", error=str(exc))
                await asyncio.sleep(1)

    async def _to_dlq(self, raw: str, error: str):
        """Send failed payload to DLQ."""
        if not self.redis:
            return
        await self.redis.xadd(
            self.dlq_stream,
            {"data": raw, "error": error},
            id="*",
        )
        logger.warning("redis_event_dlq", dlq=self.dlq_stream)

    async def stop(self):
        """Stop the consumer loop."""
        self._stop.set()
        if self.redis:
            await self.redis.close()
