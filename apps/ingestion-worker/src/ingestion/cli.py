"""CLI for manual ingestion operations."""

import asyncio
import sys

import click
import structlog

from ingestion.config import settings
from ingestion.orchestrator import orchestrator

# Configure logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(settings.log_level),
)

logger = structlog.get_logger()


def run_async(coro):
    """Helper to run async functions."""
    return asyncio.run(coro)


@click.group()
@click.option("--debug", is_flag=True, help="Enable debug logging")
def cli(debug: bool):
    """Knowledge Platform Ingestion CLI.
    
    Ingests Wiki.js docs from GitHub into Meilisearch for AI-powered search.
    """
    if debug:
        import logging
        structlog.configure(
            wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
        )


@cli.command()
def sync():
    """Full sync from GitHub.
    
    Fetches all documents from the configured GitHub repository
    and indexes them in Meilisearch.
    """
    async def _sync():
        await orchestrator.initialize()
        return await orchestrator.full_sync()
    
    click.echo("🔄 Starting full sync...")
    result = run_async(_sync())
    
    click.echo(f"\n✅ Sync completed: {result['documents_processed']} documents indexed")
    click.echo(f"   Chunks created: {result['chunks_created']}")
    click.echo(f"   Duration: {result['duration_seconds']:.2f}s")
    
    if result['errors']:
        click.echo(click.style(f"\n⚠️  Errors: {len(result['errors'])}", fg="yellow"))
        for err in result['errors']:
            click.echo(f"   - {err['path']}: {err['error']}")


@cli.command()
@click.argument("path")
def index(path: str):
    """Index a specific file or directory.
    
    PATH is the path to the document relative to the docs root.
    """
    async def _index():
        await orchestrator.initialize()
        full_path = path
        if not path.startswith(settings.github_docs_path):
            full_path = f"{settings.github_docs_path}/{path}"
        return await orchestrator.sync_single(full_path)
    
    click.echo(f"📄 Indexing: {path}")
    result = run_async(_index())
    
    if result['status'] == 'completed':
        click.echo(click.style(f"✅ Indexed: {result['title']}", fg="green"))
        click.echo(f"   Document ID: {result['document_id']}")
        click.echo(f"   Chunks: {result['chunks']}")
    else:
        click.echo(click.style(f"❌ Error: {result.get('error')}", fg="red"))
        sys.exit(1)


@cli.command()
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompt")
def rebuild(yes: bool):
    """Clear all indexes and rebuild from scratch.
    
    This will delete all existing indexed data and perform a fresh
    full sync from GitHub.
    """
    if not yes:
        click.confirm(
            click.style("⚠️  This will delete all indexed data. Continue?", fg="yellow"),
            abort=True,
        )
    
    async def _rebuild():
        await orchestrator.initialize()
        return await orchestrator.clear_and_rebuild()
    
    click.echo("🗑️  Clearing indexes and rebuilding...")
    result = run_async(_rebuild())
    
    click.echo(click.style(f"\n✅ Rebuild completed: {result['documents_processed']} documents indexed", fg="green"))


@cli.command()
def status():
    """Show ingestion status and statistics."""
    async def _status():
        await orchestrator.initialize()
        return await orchestrator.get_status()
    
    status = run_async(_status())
    
    click.echo("\n📊 " + click.style("Ingestion Status", bold=True))
    click.echo("=" * 45)
    
    # Config
    click.echo(f"\n{'Repository:':<20} {status['config']['repo']}")
    click.echo(f"{'Branch:':<20} {status['config']['branch']}")
    click.echo(f"{'Docs path:':<20} {status['config']['docs_path']}")
    click.echo(f"{'Mode:':<20} {status['config']['mode']}")
    click.echo(f"{'Last sync:':<20} {status['last_sync'] or 'Never'}")
    
    # Index stats
    click.echo("\n📈 " + click.style("Index Statistics", bold=True))
    click.echo(f"{'Documents indexed:':<20} {status['indexes']['documents']['count']}")
    click.echo(f"{'Chunks indexed:':<20} {status['indexes']['chunks']['count']}")
    
    # Processing stats
    click.echo("\n📋 " + click.style("Processing Stats", bold=True))
    click.echo(f"{'Total processed:':<20} {status['stats']['total_processed']}")
    click.echo(f"{'Total indexed:':<20} {status['stats']['total_indexed']}")
    click.echo(f"{'Errors:':<20} {status['stats']['errors']}")


@cli.command()
@click.argument("query")
@click.option("--limit", "-n", default=5, help="Maximum number of results")
@click.option("--chunks", "-c", is_flag=True, help="Search chunks instead of documents")
def search(query: str, limit: int, chunks: bool):
    """Search indexed documents.
    
    QUERY is the search term to look for.
    """
    async def _search():
        await orchestrator.initialize()
        if chunks:
            return await orchestrator.indexer.search_chunks(query, limit=limit)
        return await orchestrator.indexer.search(query, limit=limit)
    
    result = run_async(_search())
    
    click.echo(f"\n🔍 Search: '{query}'")
    click.echo(f"Found {result['estimatedTotalHits']} results\n")
    
    if not result['hits']:
        click.echo(click.style("No results found.", fg="yellow"))
        return
    
    for i, hit in enumerate(result['hits'], 1):
        title = hit.get('title', 'Untitled')
        click.echo(f"{i}. " + click.style(title, bold=True))
        click.echo(f"   Path: {hit.get('path', 'N/A')}")
        if hit.get('tags'):
            click.echo(f"   Tags: {', '.join(hit['tags'])}")
        if hit.get('description'):
            desc = hit['description'][:80] + "..." if len(hit['description']) > 80 else hit['description']
            click.echo(f"   {desc}")
        click.echo()


@cli.command()
@click.option("--host", default="0.0.0.0", help="Host to bind to")
@click.option("--port", "-p", default=None, type=int, help="Port to listen on")
@click.option("--reload", is_flag=True, help="Enable auto-reload for development")
def serve(host: str, port: int | None, reload: bool):
    """Start the webhook server.
    
    Runs a FastAPI server that listens for GitHub webhooks
    and processes document changes in real-time.
    """
    import uvicorn
    
    port = port or settings.webhook_port
    
    click.echo(f"🚀 Starting ingestion server on {host}:{port}")
    click.echo(f"   Webhook endpoint: POST /webhook/github")
    click.echo(f"   Health check: GET /health")
    click.echo(f"   Metrics: GET /metrics")
    click.echo()
    
    uvicorn.run(
        "ingestion.server:app",
        host=host,
        port=port,
        reload=reload,
        log_level=settings.log_level.lower(),
    )


@cli.command()
def list_docs():
    """List all documents in the GitHub repository."""
    async def _list():
        await orchestrator.initialize()
        return await orchestrator.github.get_tree()
    
    click.echo(f"📂 Docs in {settings.github_repo}/{settings.github_docs_path}\n")
    
    files = run_async(_list())
    
    if not files:
        click.echo(click.style("No documents found.", fg="yellow"))
        return
    
    for f in files[:50]:
        size = f.get('size', 0)
        click.echo(f"  {f['path']:<60} {size:>8,} bytes")
    
    if len(files) > 50:
        click.echo(f"\n  ... and {len(files) - 50} more files")
    
    click.echo(f"\nTotal: {len(files)} files")


@cli.command()
@click.argument("path")
def preview(path: str):
    """Preview how a document will be parsed.
    
    PATH is the path to the document to preview.
    """
    async def _preview():
        await orchestrator.initialize()
        full_path = path
        if not path.startswith(settings.github_docs_path):
            full_path = f"{settings.github_docs_path}/{path}"
        
        github_file = await orchestrator.github.get_file(full_path)
        doc = orchestrator.parser.parse(github_file)
        chunks = orchestrator.parser.chunk(doc)
        return doc, chunks
    
    click.echo(f"🔍 Previewing: {path}\n")
    
    try:
        doc, chunks = run_async(_preview())
    except Exception as e:
        click.echo(click.style(f"❌ Error: {e}", fg="red"))
        sys.exit(1)
    
    click.echo(click.style("Document:", bold=True))
    click.echo(f"  ID: {doc.id}")
    click.echo(f"  Title: {doc.title}")
    click.echo(f"  Path: {doc.path}")
    click.echo(f"  Description: {doc.description[:80]}..." if doc.description else "  Description: None")
    click.echo(f"  Tags: {', '.join(doc.tags) if doc.tags else 'None'}")
    click.echo(f"  Author: {doc.author or 'Unknown'}")
    click.echo(f"  Word count: {doc.word_count}")
    click.echo(f"  Reading time: {doc.reading_time_minutes} min")
    click.echo(f"  Chunks: {len(chunks)}")
    
    if chunks:
        click.echo(f"\n" + click.style("First Chunk Preview:", bold=True))
        preview_text = chunks[0].content[:300]
        if len(chunks[0].content) > 300:
            preview_text += "..."
        click.echo(f"  {preview_text}")


@cli.command()
@click.argument("doc_id")
def delete(doc_id: str):
    """Delete a document from the index.
    
    DOC_ID is the document ID to delete.
    """
    async def _delete():
        await orchestrator.initialize()
        return await orchestrator.delete_document(doc_id)
    
    click.confirm(f"Delete document '{doc_id}'?", abort=True)
    
    result = run_async(_delete())
    click.echo(click.style(f"✅ Deleted: {doc_id}", fg="green"))


def main():
    """Entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()