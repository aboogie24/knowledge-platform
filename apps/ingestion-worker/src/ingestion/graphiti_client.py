"""Graphiti/Neo4j client factory and helpers."""

import asyncio
from datetime import datetime

import structlog

from ingestion.config import settings

# Optional imports to keep startup resilient if Graphiti isn't configured
try:
    from graphiti_core import Graphiti
    from graphiti_core.nodes import EpisodeType
    from graphiti_core.llm_client import LLMConfig, OpenAIClient
    from graphiti_core.llm_client.anthropic_client import AnthropicClient
    from graphiti_core.embedder.openai import OpenAIEmbedder, OpenAIEmbedderConfig
except Exception:  # pragma: no cover - only hit when graphiti not installed
    Graphiti = None  # type: ignore
    EpisodeType = None  # type: ignore
    LLMConfig = None  # type: ignore
    OpenAIClient = None  # type: ignore
    AnthropicClient = None  # type: ignore
    OpenAIEmbedder = None  # type: ignore
    OpenAIEmbedderConfig = None  # type: ignore

logger = structlog.get_logger()


class GraphitiNotConfigured(Exception):
    """Raised when Graphiti integration is not available."""


def create_graphiti_client() -> Graphiti:
    """Create a Graphiti client, preferring Anthropic to avoid reasoning.effort bugs."""
    if not Graphiti:
        raise GraphitiNotConfigured("graphiti-core is not installed")

    password = settings.neo4j_password_value
    if not password:
        raise GraphitiNotConfigured("Graphiti requires NEO4J_PASSWORD or NEO4J_AUTH to be set")

    if settings.use_anthropic and settings.anthropic_api_key:
        llm_client = AnthropicClient(
            config=LLMConfig(
                api_key=settings.anthropic_api_key,
                model=settings.anthropic_model,
            )
        )

        embedder = None
        if settings.openai_api_key and OpenAIEmbedder:
            embedder = OpenAIEmbedder(
                config=OpenAIEmbedderConfig(api_key=settings.openai_api_key)
            )

        return Graphiti(
            uri=settings.neo4j_uri,
            user=settings.neo4j_user,
            password=password,
            llm_client=llm_client,
            embedder=embedder,
        )

    if not settings.openai_api_key:
        raise GraphitiNotConfigured(
            "Graphiti requires ANTHROPIC_API_KEY or OPENAI_API_KEY to be set"
        )

    llm_client = OpenAIClient(
        config=LLMConfig(
            api_key=settings.openai_api_key,
            model=settings.openai_model,
        )
    )

    return Graphiti(
        uri=settings.neo4j_uri,
        user=settings.neo4j_user,
        password=password,
        llm_client=llm_client,
    )


class GraphitiIndexer:
    """Thin wrapper to ingest docs into Graphiti if configured."""

    def __init__(self):
        self.enabled = settings.use_graphiti
        self.client: Graphiti | None = None
        self.throttle_seconds = settings.graphiti_throttle_seconds

    async def initialize(self):
        """Connect and build indices/constraints."""
        if not self.enabled:
            logger.info("graphiti_disabled")
            return
        try:
            self.client = create_graphiti_client()
            await self.client.build_indices_and_constraints()
            logger.info("graphiti_initialized")
        except Exception as exc:
            logger.warning("graphiti_init_failed", error=str(exc))
            self.enabled = False

    async def index_document(self, doc):
        """Index a single document as an episode."""
        if not (self.enabled and self.client and EpisodeType):
            return

        if self.throttle_seconds:
            await asyncio.sleep(self.throttle_seconds)

        reference_time = doc.updated_at or datetime.utcnow()
        try:
            await self.client.add_episode(
                name=doc.title,
                episode_body=doc.body_raw or doc.content,
                source=EpisodeType.text,
                source_description=doc.source_url or doc.path,
                reference_time=reference_time,
            )
            logger.debug("graphiti_indexed_doc", doc_id=doc.id, title=doc.title)
        except Exception as exc:
            logger.warning("graphiti_index_failed", doc_id=doc.id, error=str(exc))

    async def index_documents(self, docs: list):
        """Index multiple documents."""
        if not (self.enabled and self.client):
            return
        for doc in docs:
            await self.index_document(doc)
