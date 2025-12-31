"""Graphiti client wrapper for the GraphQL API."""

from datetime import datetime

import structlog

from graph_api.config import settings

try:
    from graphiti_core import Graphiti
    from graphiti_core.llm_client import LLMConfig, OpenAIClient
    from graphiti_core.llm_client.anthropic_client import AnthropicClient
    from graphiti_core.embedder.openai import OpenAIEmbedder, OpenAIEmbedderConfig
    from graphiti_core.search.search_config import SearchConfig
except Exception:  # pragma: no cover - only hit when graphiti not installed
    Graphiti = None  # type: ignore
    LLMConfig = None  # type: ignore
    OpenAIClient = None  # type: ignore
    AnthropicClient = None  # type: ignore
    OpenAIEmbedder = None  # type: ignore
    OpenAIEmbedderConfig = None  # type: ignore
    SearchConfig = None  # type: ignore

logger = structlog.get_logger()


class GraphitiNotConfigured(Exception):
    """Raised when Graphiti integration is not available."""


def create_graphiti_client() -> Graphiti:
    """Create a Graphiti client for querying the graph."""
    if not Graphiti:
        raise GraphitiNotConfigured("graphiti-core is not installed")

    password = settings.neo4j_password_value
    if not password:
        raise GraphitiNotConfigured("Graphiti requires NEO4J_PASSWORD or NEO4J_AUTH to be set")

    if settings.use_anthropic and settings.anthropic_api_key and AnthropicClient:
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


def get_default_search_config(limit: int) -> SearchConfig:
    """Build a SearchConfig with the desired limit."""
    return SearchConfig(limit=limit)


async def get_recent_episodes(client: Graphiti, limit: int):
    """Retrieve recent episodes."""
    return await client.retrieve_episodes(reference_time=datetime.utcnow(), last_n=limit)
