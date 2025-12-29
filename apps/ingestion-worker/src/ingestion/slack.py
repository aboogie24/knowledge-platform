"""Slack webhook handling and event normalization."""

import hashlib
import hmac
import time
import uuid
from typing import Any, Optional

import structlog
from fastapi import HTTPException, Request

from ingestion.config import settings
from ingestion.redis_streams import RedisStreams

logger = structlog.get_logger()


def _verify_slack_signature(request_body: bytes, timestamp: str, signature: str) -> bool:
    """Verify Slack signing secret per https://api.slack.com/authentication/verifying-requests-from-slack."""
    if not settings.slack_signing_secret:
        return False
    if not timestamp or not signature:
        return False
    # Reject old requests (>5 minutes)
    if abs(time.time() - int(timestamp)) > 60 * 5:
        return False

    basestring = f"v0:{timestamp}:{request_body.decode('utf-8')}"
    digest = hmac.new(
        settings.slack_signing_secret.encode("utf-8"),
        basestring.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    computed = f"v0={digest}"
    return hmac.compare_digest(computed, signature)


async def handle_slack_webhook(
    request: Request,
    redis_client: RedisStreams,
    x_slack_signature: Optional[str],
    x_slack_request_timestamp: Optional[str],
):
    """Handle Slack Events API webhook: verify, handle challenge, enqueue event."""
    body = await request.body()
    if not _verify_slack_signature(body, x_slack_request_timestamp or "", x_slack_signature or ""):
        raise HTTPException(status_code=401, detail="Invalid Slack signature")

    payload: dict[str, Any] = await request.json()

    # URL verification handshake
    if payload.get("type") == "url_verification":
        return {"challenge": payload.get("challenge")}

    event = {
        "id": str(uuid.uuid4()),
        "source": "slack",
        "type": payload.get("type", "event_callback"),
        "timestamp": payload.get("event_time"),
        "payload": payload,
        "metadata": {
            "team_id": payload.get("team_id"),
            "api_app_id": payload.get("api_app_id"),
        },
    }

    await redis_client.enqueue(event)
    logger.info("slack_event_enqueued", event_id=event["id"])
    return {"status": "accepted"}
