"""FastAPI server for ingestion worker."""

import hashlib
import hmac
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Header, Request, BackgroundTasks
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
import structlog

from ingestion.config import settings
from ingestion.orchestrator import orchestrator

logger = structlog.get_logger()

# Prometheus metrics
WEBHOOK_REQUESTS = Counter(
    "ingestion_webhook_requests_total",
    "Total webhook requests",
    ["status"],
)
SYNC_DURATION = Histogram(
    "ingestion_sync_duration_seconds",
    "Time spent syncing documents",
    ["type"],
)
DOCUMENTS_INDEXED = Counter(
    "ingestion_documents_indexed_total",
    "Total documents indexed",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    # Startup
    logger.info("starting_ingestion_worker")
    await orchestrator.initialize()
    
    # If mode is poll, start background polling
    if settings.ingestion_mode == "poll":
        import asyncio
        asyncio.create_task(poll_loop())
    
    yield
    
    # Shutdown
    logger.info("shutting_down_ingestion_worker")


app = FastAPI(
    title="Knowledge Platform Ingestion Worker",
    description="Ingests Wiki.js docs from GitHub into Meilisearch",
    version="0.1.0",
    lifespan=lifespan,
)


async def poll_loop():
    """Background polling loop."""
    import asyncio
    
    logger.info("starting_poll_loop", interval=settings.poll_interval_seconds)
    
    while True:
        try:
            with SYNC_DURATION.labels(type="poll").time():
                result = await orchestrator.full_sync()
                DOCUMENTS_INDEXED.inc(result.get("documents_processed", 0))
        except Exception as e:
            logger.error("poll_sync_failed", error=str(e))
        
        await asyncio.sleep(settings.poll_interval_seconds)


def verify_github_signature(payload: bytes, signature: str) -> bool:
    """Verify GitHub webhook signature."""
    if not settings.github_webhook_secret:
        return True  # Skip verification if no secret configured
    
    if not signature:
        return False
    
    expected = "sha256=" + hmac.new(
        settings.github_webhook_secret.encode(),
        payload,
        hashlib.sha256,
    ).hexdigest()
    
    return hmac.compare_digest(expected, signature)


# Health endpoints
@app.get("/health")
async def health():
    """Liveness probe."""
    return {"status": "healthy"}


@app.get("/ready")
async def ready():
    """Readiness probe."""
    try:
        status = await orchestrator.get_status()
        return {"status": "ready", "last_sync": status["last_sync"]}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint."""
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )


# Status endpoint
@app.get("/status")
async def status():
    """Get ingestion status."""
    return await orchestrator.get_status()


# Webhook endpoint
@app.post("/webhook/github")
async def github_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    x_hub_signature_256: str | None = Header(None),
    x_github_event: str | None = Header(None),
):
    """Handle GitHub webhook events."""
    # Get raw body for signature verification
    body = await request.body()
    
    # Verify signature
    if not verify_github_signature(body, x_hub_signature_256 or ""):
        WEBHOOK_REQUESTS.labels(status="unauthorized").inc()
        raise HTTPException(status_code=401, detail="Invalid signature")
    
    # Parse payload
    payload = await request.json()
    
    # Handle different event types
    if x_github_event == "ping":
        WEBHOOK_REQUESTS.labels(status="ping").inc()
        return {"status": "pong", "zen": payload.get("zen")}
    
    if x_github_event != "push":
        WEBHOOK_REQUESTS.labels(status="ignored").inc()
        return {"status": "ignored", "event": x_github_event}
    
    # Process push event in background
    WEBHOOK_REQUESTS.labels(status="accepted").inc()
    
    async def process_push():
        with SYNC_DURATION.labels(type="webhook").time():
            result = await orchestrator.process_webhook(payload)
            DOCUMENTS_INDEXED.inc(result.get("documents_processed", 0))
    
    background_tasks.add_task(process_push)
    
    return {"status": "accepted", "processing": "background"}


# Manual sync endpoints
@app.post("/sync/full")
async def sync_full(background_tasks: BackgroundTasks):
    """Trigger a full sync."""
    async def do_sync():
        with SYNC_DURATION.labels(type="full").time():
            result = await orchestrator.full_sync()
            DOCUMENTS_INDEXED.inc(result.get("documents_processed", 0))
    
    background_tasks.add_task(do_sync)
    return {"status": "started", "type": "full"}


@app.post("/sync/path/{path:path}")
async def sync_path(path: str):
    """Sync a specific path."""
    full_path = f"{settings.github_docs_path}/{path}"
    result = await orchestrator.sync_single(full_path)
    return result


@app.post("/rebuild")
async def rebuild(background_tasks: BackgroundTasks):
    """Clear and rebuild all indexes."""
    async def do_rebuild():
        with SYNC_DURATION.labels(type="rebuild").time():
            result = await orchestrator.clear_and_rebuild()
            DOCUMENTS_INDEXED.inc(result.get("documents_processed", 0))
    
    background_tasks.add_task(do_rebuild)
    return {"status": "started", "type": "rebuild"}


# Search endpoints (for testing)
@app.get("/search")
async def search(q: str, limit: int = 10):
    """Search documents."""
    result = await orchestrator.indexer.search(q, limit=limit)
    return result


@app.get("/search/chunks")
async def search_chunks(q: str, limit: int = 10):
    """Search document chunks."""
    result = await orchestrator.indexer.search_chunks(q, limit=limit)
    return result


# Add missing import
from starlette.responses import Response