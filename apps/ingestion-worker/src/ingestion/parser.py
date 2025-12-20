"""Parse Wiki.js markdown documents."""

import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import frontmatter
import markdown
from bs4 import BeautifulSoup
import structlog

from ingestion.config import settings
from ingestion.github_client import GitHubFile

logger = structlog.get_logger()


@dataclass
class ParsedDocument:
    """A parsed document ready for indexing."""

    id: str
    title: str
    path: str
    content: str  # Plain text content
    body_html: str  # HTML content
    body_raw: str  # Raw markdown
    
    # Metadata from frontmatter
    description: str = ""
    tags: list[str] = field(default_factory=list)
    author: str = ""
    
    # Timestamps
    created_at: datetime | None = None
    updated_at: datetime | None = None
    
    # Source info
    source_url: str = ""
    source_sha: str = ""
    
    # Wiki.js specific
    wiki_id: str = ""
    wiki_path: str = ""
    
    # Computed
    word_count: int = 0
    reading_time_minutes: int = 0

    def to_meili_doc(self) -> dict:
        """Convert to Meilisearch document format."""
        return {
            "id": self.id,
            "title": self.title,
            "path": self.path,
            "content": self.content,
            "description": self.description,
            "tags": self.tags,
            "author": self.author,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "source_url": self.source_url,
            "word_count": self.word_count,
            "reading_time_minutes": self.reading_time_minutes,
        }


@dataclass
class DocumentChunk:
    """A chunk of a document for granular indexing."""

    id: str
    document_id: str
    title: str
    path: str
    content: str
    chunk_index: int
    total_chunks: int
    
    # Inherited from parent
    tags: list[str] = field(default_factory=list)
    source_url: str = ""
    updated_at: datetime | None = None

    def to_meili_doc(self) -> dict:
        """Convert to Meilisearch document format."""
        return {
            "id": self.id,
            "document_id": self.document_id,
            "title": self.title,
            "path": self.path,
            "content": self.content,
            "chunk_index": self.chunk_index,
            "total_chunks": self.total_chunks,
            "tags": self.tags,
            "source_url": self.source_url,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class DocumentParser:
    """Parse Wiki.js markdown documents."""

    def __init__(self):
        self.md = markdown.Markdown(
            extensions=[
                "meta",
                "tables",
                "fenced_code",
                "codehilite",
                "toc",
            ]
        )

    def parse(self, github_file: GitHubFile) -> ParsedDocument:
        """Parse a GitHub file into a structured document."""
        # Parse frontmatter
        post = frontmatter.loads(github_file.content)
        metadata = post.metadata
        body = post.content

        # Convert markdown to HTML
        self.md.reset()
        body_html = self.md.convert(body)

        # Extract plain text from HTML
        soup = BeautifulSoup(body_html, "html.parser")
        content = soup.get_text(separator=" ", strip=True)

        # Calculate reading time (average 200 words per minute)
        word_count = len(content.split())
        reading_time = max(1, word_count // 200)

        # Generate document ID from path
        doc_id = self._path_to_id(github_file.path)

        # Extract title from frontmatter or first heading or filename
        title = self._extract_title(metadata, body, github_file.name)

        # Parse tags - handle both string and list formats
        tags = self._parse_tags(metadata.get("tags", []))

        # Parse dates
        created_at = self._parse_date(metadata.get("date") or metadata.get("created"))
        updated_at = github_file.last_modified or self._parse_date(metadata.get("updated"))

        return ParsedDocument(
            id=doc_id,
            title=title,
            path=github_file.path,
            content=content,
            body_html=body_html,
            body_raw=body,
            description=metadata.get("description", ""),
            tags=tags,
            author=metadata.get("author", ""),
            created_at=created_at,
            updated_at=updated_at,
            source_url=github_file.url,
            source_sha=github_file.sha,
            wiki_id=metadata.get("id", ""),
            wiki_path=metadata.get("path", github_file.path),
            word_count=word_count,
            reading_time_minutes=reading_time,
        )

    def chunk(self, doc: ParsedDocument) -> list[DocumentChunk]:
        """Split document into chunks for granular indexing."""
        chunks = []
        content = doc.content
        chunk_size = settings.chunk_size
        overlap = settings.chunk_overlap

        # Simple chunking by character count with overlap
        # TODO: Improve with sentence-aware chunking
        if len(content) <= chunk_size:
            # Document fits in one chunk
            chunks.append(
                DocumentChunk(
                    id=f"{doc.id}_0",
                    document_id=doc.id,
                    title=doc.title,
                    path=doc.path,
                    content=content,
                    chunk_index=0,
                    total_chunks=1,
                    tags=doc.tags,
                    source_url=doc.source_url,
                    updated_at=doc.updated_at,
                )
            )
        else:
            # Split into overlapping chunks
            start = 0
            chunk_index = 0

            while start < len(content):
                end = start + chunk_size

                # Try to break at sentence boundary
                if end < len(content):
                    # Look for sentence end within last 20% of chunk
                    search_start = end - int(chunk_size * 0.2)
                    search_text = content[search_start:end]
                    
                    # Find last sentence boundary
                    for delim in [". ", ".\n", "! ", "? ", "\n\n"]:
                        last_pos = search_text.rfind(delim)
                        if last_pos != -1:
                            end = search_start + last_pos + len(delim)
                            break

                chunk_content = content[start:end].strip()

                if chunk_content:  # Don't add empty chunks
                    chunks.append(
                        DocumentChunk(
                            id=f"{doc.id}_{chunk_index}",
                            document_id=doc.id,
                            title=doc.title,
                            path=doc.path,
                            content=chunk_content,
                            chunk_index=chunk_index,
                            total_chunks=0,  # Will update after
                            tags=doc.tags,
                            source_url=doc.source_url,
                            updated_at=doc.updated_at,
                        )
                    )
                    chunk_index += 1

                start = end - overlap

            # Update total_chunks
            for chunk in chunks:
                chunk.total_chunks = len(chunks)

        logger.debug(
            "chunked_document",
            doc_id=doc.id,
            original_length=len(content),
            chunks=len(chunks),
        )

        return chunks

    def _path_to_id(self, path: str) -> str:
        """Convert file path to document ID."""
        # Remove docs prefix and extension
        doc_path = Path(path)
        stem = doc_path.stem
        
        # Create ID from path segments
        parts = list(doc_path.parent.parts)
        if parts and parts[0] == settings.github_docs_path.split("/")[0]:
            parts = parts[1:]  # Remove docs prefix
        
        parts.append(stem)
        return "-".join(parts).lower().replace(" ", "-")

    def _extract_title(self, metadata: dict, body: str, filename: str) -> str:
        """Extract title from metadata, body, or filename."""
        # Try frontmatter title
        if metadata.get("title"):
            return metadata["title"]

        # Try first heading
        heading_match = re.search(r"^#\s+(.+)$", body, re.MULTILINE)
        if heading_match:
            return heading_match.group(1).strip()

        # Fall back to filename
        return filename.replace("-", " ").replace("_", " ").title()

    def _parse_tags(self, tags) -> list[str]:
        """Parse tags from various formats."""
        if isinstance(tags, list):
            return [str(t).strip() for t in tags if t]
        if isinstance(tags, str):
            # Handle comma or space separated
            return [t.strip() for t in re.split(r"[,\s]+", tags) if t.strip()]
        return []

    def _parse_date(self, date_value) -> datetime | None:
        """Parse date from various formats."""
        if not date_value:
            return None

        if isinstance(date_value, datetime):
            return date_value

        if isinstance(date_value, str):
            # Try common formats
            for fmt in [
                "%Y-%m-%d",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%d %H:%M:%S",
            ]:
                try:
                    return datetime.strptime(date_value, fmt)
                except ValueError:
                    continue

        return None