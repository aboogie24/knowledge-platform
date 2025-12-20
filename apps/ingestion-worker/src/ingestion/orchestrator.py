"""Main ingestion orchestrator."""

import asyncio
from datetime import datetime

import structlog

from ingestion.config import settings
from ingestion.github_client import GitHubClient, GitHubFile
from ingestion.parser import DocumentParser, ParsedDocument, DocumentChunk
from ingestion.indexer import MeilisearchIndexer

logger = structlog.get_logger()


class IngestionOrchestrator:
    """Orchestrates the document ingestion pipeline."""

    def __init__(self):
        self.github = GitHubClient()
        self.parser = DocumentParser()
        self.indexer = MeilisearchIndexer()
        
        # Track state
        self.last_sync: datetime | None = None
        self.last_sha: str | None = None
        self.stats = {
            "total_processed": 0,
            "total_indexed": 0,
            "total_chunks": 0,
            "errors": 0,
        }

    async def initialize(self):
        """Initialize all components."""
        await self.indexer.initialize()
        logger.info("ingestion_orchestrator_initialized")

    async def full_sync(self) -> dict:
        """Perform a full synchronization of all documents."""
        logger.info("starting_full_sync")
        start_time = datetime.now()

        # Fetch all docs from GitHub
        github_files = await self.github.get_all_docs()
        
        # Process and index each document
        all_docs: list[ParsedDocument] = []
        all_chunks: list[DocumentChunk] = []
        errors: list[dict] = []

        for gf in github_files:
            try:
                doc = self.parser.parse(gf)
                chunks = self.parser.chunk(doc)
                
                all_docs.append(doc)
                all_chunks.extend(chunks)
                
                self.stats["total_processed"] += 1
            except Exception as e:
                logger.error("failed_to_process_doc", path=gf.path, error=str(e))
                errors.append({"path": gf.path, "error": str(e)})
                self.stats["errors"] += 1

        # Batch index
        if all_docs:
            await self.indexer.index_batch(all_docs, all_chunks)
            self.stats["total_indexed"] += len(all_docs)
            self.stats["total_chunks"] += len(all_chunks)

        # Update state
        self.last_sync = datetime.now()
        duration = (self.last_sync - start_time).total_seconds()

        result = {
            "status": "completed",
            "duration_seconds": duration,
            "documents_processed": len(all_docs),
            "chunks_created": len(all_chunks),
            "errors": errors,
        }

        logger.info("full_sync_completed", **result)
        return result

    async def incremental_sync(self, changed_paths: list[str]) -> dict:
        """Sync only changed documents."""
        logger.info("starting_incremental_sync", changed_count=len(changed_paths))
        start_time = datetime.now()

        processed = []
        errors = []

        for path in changed_paths:
            try:
                # Fetch and process the changed file
                github_file = await self.github.get_file(path)
                doc = self.parser.parse(github_file)
                chunks = self.parser.chunk(doc)

                # Index document and chunks
                await self.indexer.index_document(doc)
                await self.indexer.index_chunks(chunks)

                processed.append(path)
                self.stats["total_processed"] += 1
                self.stats["total_indexed"] += 1
                self.stats["total_chunks"] += len(chunks)

            except Exception as e:
                logger.error("failed_to_sync_doc", path=path, error=str(e))
                errors.append({"path": path, "error": str(e)})
                self.stats["errors"] += 1

        self.last_sync = datetime.now()
        duration = (self.last_sync - start_time).total_seconds()

        result = {
            "status": "completed",
            "duration_seconds": duration,
            "documents_processed": len(processed),
            "errors": errors,
        }

        logger.info("incremental_sync_completed", **result)
        return result

    async def process_webhook(self, payload: dict) -> dict:
        """Process a GitHub webhook payload."""
        # Extract relevant information
        ref = payload.get("ref", "")
        before = payload.get("before", "")
        after = payload.get("after", "")

        # Only process pushes to the configured branch
        expected_ref = f"refs/heads/{settings.github_branch}"
        if ref != expected_ref:
            logger.debug("skipping_webhook", ref=ref, expected=expected_ref)
            return {"status": "skipped", "reason": f"Not target branch: {ref}"}

        # Get changed files from commits
        changed_paths = set()
        for commit in payload.get("commits", []):
            for path in commit.get("added", []) + commit.get("modified", []):
                if path.startswith(settings.github_docs_path):
                    if path.endswith((".md", ".markdown", ".html")):
                        changed_paths.add(path)

            # Handle removed files
            for path in commit.get("removed", []):
                if path.startswith(settings.github_docs_path):
                    doc_id = self.parser._path_to_id(path)
                    await self.indexer.delete_document(doc_id)
                    logger.info("deleted_removed_doc", path=path)

        if not changed_paths:
            return {"status": "skipped", "reason": "No docs changed"}

        # Process changed files
        return await self.incremental_sync(list(changed_paths))

    async def sync_single(self, path: str) -> dict:
        """Sync a single document by path."""
        logger.info("syncing_single_doc", path=path)

        try:
            github_file = await self.github.get_file(path)
            doc = self.parser.parse(github_file)
            chunks = self.parser.chunk(doc)

            await self.indexer.index_document(doc)
            await self.indexer.index_chunks(chunks)

            return {
                "status": "completed",
                "document_id": doc.id,
                "title": doc.title,
                "chunks": len(chunks),
            }
        except Exception as e:
            logger.error("failed_to_sync_single", path=path, error=str(e))
            return {"status": "error", "path": path, "error": str(e)}

    async def delete_document(self, doc_id: str) -> dict:
        """Delete a document by ID."""
        await self.indexer.delete_document(doc_id)
        return {"status": "deleted", "document_id": doc_id}

    async def clear_and_rebuild(self) -> dict:
        """Clear all indexes and rebuild from scratch."""
        logger.warning("clearing_and_rebuilding")
        
        await self.indexer.clear_all()
        
        # Reset stats
        self.stats = {
            "total_processed": 0,
            "total_indexed": 0,
            "total_chunks": 0,
            "errors": 0,
        }
        
        return await self.full_sync()

    async def get_status(self) -> dict:
        """Get current ingestion status."""
        index_stats = await self.indexer.get_stats()
        
        return {
            "last_sync": self.last_sync.isoformat() if self.last_sync else None,
            "stats": self.stats,
            "indexes": index_stats,
            "config": {
                "repo": settings.github_repo,
                "branch": settings.github_branch,
                "docs_path": settings.github_docs_path,
                "mode": settings.ingestion_mode,
            },
        }


# Singleton instance
orchestrator = IngestionOrchestrator()