"""GraphQL schema and resolvers."""

from datetime import datetime
from typing import Optional

import strawberry

from graph_api.graphiti_client import create_graphiti_client, get_default_search_config, get_recent_episodes
from graph_api.config import settings

from graphiti_core.edges import EntityEdge
from graphiti_core.nodes import EntityNode, EpisodicNode


def to_entity(node: EntityNode) -> "Entity":
    """Convert Graphiti EntityNode to GraphQL Entity."""
    return Entity(
        uuid=node.uuid,
        name=node.name,
        group_id=node.group_id,
        created_at=node.created_at.isoformat() if node.created_at else None,
        summary=getattr(node, "summary", None),
    )


def to_relationship(edge: EntityEdge) -> "Relationship":
    """Convert Graphiti EntityEdge to GraphQL Relationship."""
    return Relationship(
        uuid=edge.uuid,
        name=edge.name,
        fact=edge.fact,
        source_uuid=edge.source_node_uuid,
        target_uuid=edge.target_node_uuid,
        created_at=edge.created_at.isoformat() if edge.created_at else None,
        valid_at=edge.valid_at.isoformat() if edge.valid_at else None,
        expired_at=edge.expired_at.isoformat() if edge.expired_at else None,
    )


def to_episode(ep: EpisodicNode) -> "Episode":
    """Convert Graphiti EpisodicNode to GraphQL Episode."""
    return Episode(
        uuid=ep.uuid,
        name=ep.name,
        content=ep.content,
        created_at=ep.created_at.isoformat() if ep.created_at else None,
        valid_at=ep.valid_at.isoformat() if ep.valid_at else None,
    )


@strawberry.type
class Entity:
    uuid: strawberry.ID
    name: str
    group_id: Optional[str] = None
    created_at: Optional[str] = None
    summary: Optional[str] = None


@strawberry.type
class Relationship:
    uuid: strawberry.ID
    name: str
    fact: Optional[str]
    source_uuid: strawberry.ID
    target_uuid: strawberry.ID
    created_at: Optional[str] = None
    valid_at: Optional[str] = None
    expired_at: Optional[str] = None


@strawberry.type
class Episode:
    uuid: strawberry.ID
    name: Optional[str]
    content: Optional[str]
    created_at: Optional[str]
    valid_at: Optional[str]


@strawberry.type
class GraphStats:
    entity_count: int
    relationship_count: int
    episode_count: int


@strawberry.type
class Query:
    def _client(self, info):
        client = info.context.get("graphiti")
        if not client:
            raise RuntimeError("Graphiti client not configured")
        return client

    @strawberry.field
    async def entities(self, info, limit: int = 50, search: Optional[str] = None) -> list[Entity]:
        """Search entities by text query (uses Graphiti search_)."""
        query = search or "*"
        client = self._client(info)
        results = await client.search_(query=query, config=get_default_search_config(limit))
        nodes = results.nodes or []
        return [to_entity(node) for node in nodes][:limit]

    @strawberry.field
    async def entity(
        self, info, uuid: Optional[str] = None, name: Optional[str] = None
    ) -> Optional[Entity]:
        """Lookup entity by UUID (preferred) or by name via search."""
        client = self._client(info)
        if uuid:
            nodes = await client.search_(
                query=uuid, config=get_default_search_config(5), search_filter=None
            )
            for node in nodes.nodes or []:
                if node.uuid == uuid:
                    return to_entity(node)
            return None
        if name:
            nodes = await client.search_(query=name, config=get_default_search_config(5))
            if nodes.nodes:
                return to_entity(nodes.nodes[0])
        return None

    @strawberry.field
    async def relationships(
        self, info, limit: int = 50, entity_name: Optional[str] = None
    ) -> list[Relationship]:
        """Return relationships via search (optionally centered on entity name)."""
        query = entity_name or "*"
        client = self._client(info)
        results = await client.search_(query=query, config=get_default_search_config(limit))
        edges = results.edges or []
        return [to_relationship(edge) for edge in edges][:limit]

    @strawberry.field
    async def search_entities(self, info, pattern: str, limit: int = 20) -> list[Entity]:
        """Search entities by pattern."""
        client = self._client(info)
        results = await client.search_(query=pattern, config=get_default_search_config(limit))
        nodes = results.nodes or []
        return [to_entity(node) for node in nodes][:limit]

    @strawberry.field
    async def entity_graph(
        self, info, center_name: str, depth: int = 2, limit: int = 50
    ) -> list[Relationship]:
        """Fetch a small graph centered around a name (uses search_ BFS)."""
        client = self._client(info)
        config = get_default_search_config(limit)
        config.edge_config.bfs_max_depth = depth
        results = await client.search_(query=center_name, config=config)
        return [to_relationship(edge) for edge in results.edges or []][:limit]

    @strawberry.field
    async def episodes(self, info, limit: int = 20) -> list[Episode]:
        """Return most recent episodes."""
        client = self._client(info)
        eps = await get_recent_episodes(client, limit=limit)
        return [to_episode(ep) for ep in eps]

    @strawberry.field
    async def stats(self, info) -> GraphStats:
        """Approximate graph stats from a broad search."""
        client = self._client(info)
        results = await client.search_(query="*", config=get_default_search_config(50))
        return GraphStats(
            entity_count=len(results.nodes or []),
            relationship_count=len(results.edges or []),
            episode_count=len(results.episodes or []),
        )


schema = strawberry.Schema(Query)
