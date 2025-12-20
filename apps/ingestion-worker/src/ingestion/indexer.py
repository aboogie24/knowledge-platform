"""Index documents in Meilisearch."""

from meilisearch import Client
from meilisearch.errors import MeilisearchApiError
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion.config import settings
from ingestion.parser import ParsedDocument, DocumentChunk

logger = structlog.get_logger()


class MeilisearchIndexer:
    """Index documents in Meilisearch."""

    def __init__(self):
        api_key = settings.meilisearch_api_key or None
        self.client = Client(settings.meilisearch_url, api_key)
        self.index_name = settings.meili_index_name
        self.chunks_index_name = f"{settings.meili_index_name}_chunks"

    async def initialize(self):
        """Initialize indexes with proper settings."""
        await self._setup_documents_index()
        await self._setup_chunks_index()
        logger.info("meilisearch_initialized", index=self.index_name)

    async def _setup_documents_index(self):
        """Configure the main documents index."""
        index = self.client.index(self.index_name)

        # Create index if it doesn't exist
        try:
            self.client.get_index(self.index_name)
        except MeilisearchApiError:
            self.client.create_index(self.index_name, {"primaryKey": "id"})
            logger.info("created_index", index=self.index_name)

        # Configure searchable attributes
        index.update_searchable_attributes([
            "title",
            "content",
            "description",
            "tags",
            "path",
        ])

        # Configure filterable attributes
        index.update_filterable_attributes([
            "tags",
            "author",
            "path",
            "updated_at",
        ])

        # Configure sortable attributes
        index.update_sortable_attributes([
            "title",
            "updated_at",
            "word_count",
        ])

        # Configure ranking rules
        index.update_ranking_rules([
            "words",
            "typo",
            "proximity",
            "attribute",
            "sort",
            "exactness",
            "updated_at:desc",
        ])

        # Configure displayed attributes
        index.update_displayed_attributes([
            "id",
            "title",
            "path",
            "description",
            "tags",
            "author",
            "source_url",
            "updated_at",
            "word_count",
            "reading_time_minutes",
        ])

    async def _setup_chunks_index(self):
        """Configure the chunks index for granular search."""
        index = self.client.index(self.chunks_index_name)

        try:
            self.client.get_index(self.chunks_index_name)
        except MeilisearchApiError:
            self.client.create_index(self.chunks_index_name, {"primaryKey": "id"})
            logger.info("created_index", index=self.chunks_index_name)

        index.update_searchable_attributes([
            "content",
            "title",
            "tags",
        ])

        index.update_filterable_attributes([
            "document_id",
            "tags",
            "path",
        ])

        index.update_sortable_attributes([
            "chunk_index",
            "updated_at",
        ])

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def index_document(self, doc: ParsedDocument):
        """Index a single document."""
        index = self.client.index(self.index_name)
        task = index.add_documents([doc.to_meili_doc()])
        
        logger.debug(
            "indexed_document",
            doc_id=doc.id,
            title=doc.title,
            task_uid=task.task_uid,
        )
        return task

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def index_chunks(self, chunks: list[DocumentChunk]):
        """Index document chunks."""
        if not chunks:
            return None

        index = self.client.index(self.chunks_index_name)
        docs = [chunk.to_meili_doc() for chunk in chunks]
        task = index.add_documents(docs)

        logger.debug(
            "indexed_chunks",
            doc_id=chunks[0].document_id,
            chunk_count=len(chunks),
            task_uid=task.task_uid,
        )
        return task

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def index_batch(self, docs: list[ParsedDocument], chunks: list[DocumentChunk]):
        """Index a batch of documents and their chunks."""
        # Index documents
        if docs:
            doc_index = self.client.index(self.index_name)
            doc_task = doc_index.add_documents([d.to_meili_doc() for d in docs])
            logger.info("batch_indexed_documents", count=len(docs), task_uid=doc_task.task_uid)

        # Index chunks
        if chunks:
            chunk_index = self.client.index(self.chunks_index_name)
            chunk_task = chunk_index.add_documents([c.to_meili_doc() for c in chunks])
            logger.info("batch_indexed_chunks", count=len(chunks), task_uid=chunk_task.task_uid)

    async def delete_document(self, doc_id: str):
        """Delete a document and its chunks."""
        # Delete main document
        doc_index = self.client.index(self.index_name)
        doc_index.delete_document(doc_id)

        # Delete chunks (by filter)
        chunk_index = self.client.index(self.chunks_index_name)
        chunk_index.delete_documents({"filter": f"document_id = {doc_id}"})

        logger.info("deleted_document", doc_id=doc_id)

    async def delete_by_path(self, path: str):
        """Delete documents matching a path prefix."""
        # Delete from main index
        doc_index = self.client.index(self.index_name)
        doc_index.delete_documents({"filter": f"path CONTAINS {path}"})

        # Delete from chunks index
        chunk_index = self.client.index(self.chunks_index_name)
        chunk_index.delete_documents({"filter": f"path CONTAINS {path}"})

        logger.info("deleted_by_path", path=path)

    async def search(self, query: str, limit: int = 10, filters: str | None = None) -> dict:
        """Search documents."""
        index = self.client.index(self.index_name)
        
        search_params = {
            "limit": limit,
            "attributesToHighlight": ["content", "title"],
            "highlightPreTag": "<mark>",
            "highlightPostTag": "</mark>",
        }
        
        if filters:
            search_params["filter"] = filters

        return index.search(query, search_params)

    async def search_chunks(
        self, query: str, limit: int = 10, filters: str | None = None
    ) -> dict:
        """Search document chunks (more granular)."""
        index = self.client.index(self.chunks_index_name)

        search_params = {
            "limit": limit,
            "attributesToHighlight": ["content"],
            "highlightPreTag": "<mark>",
            "highlightPostTag": "</mark>",
        }

        if filters:
            search_params["filter"] = filters

        return index.search(query, search_params)

    async def get_stats(self) -> dict:
        """Get index statistics."""
        doc_index = self.client.index(self.index_name)
        chunk_index = self.client.index(self.chunks_index_name)

        doc_stats = doc_index.get_stats()
        chunk_stats = chunk_index.get_stats()

        return {
            "documents": {
                "count": doc_stats.number_of_documents,
                "indexing": doc_stats.is_indexing,
            },
            "chunks": {
                "count": chunk_stats.number_of_documents,
                "indexing": chunk_stats.is_indexing,
            },
        }

    async def clear_all(self):
        """Clear all documents from indexes."""
        self.client.index(self.index_name).delete_all_documents()
        self.client.index(self.chunks_index_name).delete_all_documents()
        logger.warning("cleared_all_indexes")