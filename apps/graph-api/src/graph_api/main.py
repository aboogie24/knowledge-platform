"""Entry point for the GraphQL graph API."""

import asyncio

import structlog
import uvicorn
from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter

from graph_api.config import settings
from graph_api.graphiti_client import create_graphiti_client, GraphitiNotConfigured
from graph_api.schema import schema

logger = structlog.get_logger()


def build_app() -> FastAPI:
    """Create the FastAPI app with GraphQL mounted."""
    app = FastAPI(
        title="Knowledge Graph GraphQL API",
        description="GraphQL endpoint backed by Graphiti/Neo4j",
        version="0.1.0",
    )

    graphiti = None
    try:
        graphiti = create_graphiti_client()
        logger.info("graphiti_client_initialized", neo4j_uri=settings.neo4j_uri)
    except GraphitiNotConfigured as exc:
        logger.warning("graphiti_not_configured", error=str(exc))

    graphql_app = GraphQLRouter(
        schema,
        context_getter=lambda request: {"request": request, "graphiti": graphiti},
    )
    app.include_router(graphql_app, prefix="/graphql")

    @app.get("/health")
    async def health():
        return {"status": "healthy"}

    return app


def run():
    """Run the server via uvicorn."""
    uvicorn.run(
        "graph_api.main:build_app",
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level.lower(),
        factory=True,
    )


# Uvicorn factory target
app = build_app

if __name__ == "__main__":
    run()
