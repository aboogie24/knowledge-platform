"""Configuration for ingestion worker."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # GitHub configuration
    github_repo: str = Field(description="GitHub repo in format 'owner/repo'")
    github_branch: str = Field(default="main", description="Branch to watch")
    github_docs_path: str = Field(default="docs", description="Path to docs in repo")
    github_token: str = Field(description="GitHub PAT for API access")
    github_webhook_secret: str = Field(default="", description="Webhook secret for verification")

    # Meilisearch configuration
    meilisearch_url: str = Field(default="http://localhost:7700")
    meilisearch_api_key: str = Field(default="")
    meili_index_name: str = Field(default="documents")

    # Neo4j configuration (optional, for Graphiti)
    neo4j_uri: str = Field(default="bolt://localhost:7687")
    neo4j_user: str = Field(default="neo4j")
    neo4j_password: str = Field(default="")

    # LLM configuration (for Graphiti semantic extraction)
    openai_api_key: str = Field(default="")
    anthropic_api_key: str = Field(default="")

    # Ingestion settings
    ingestion_mode: str = Field(default="webhook", description="webhook, poll, or manual")
    poll_interval_seconds: int = Field(default=300)
    chunk_size: int = Field(default=1000, description="Characters per chunk")
    chunk_overlap: int = Field(default=200, description="Overlap between chunks")

    # Server settings
    webhook_port: int = Field(default=8080)
    log_level: str = Field(default="INFO")

    @property
    def github_api_url(self) -> str:
        """GitHub API base URL."""
        return "https://api.github.com"

    @property
    def github_raw_url(self) -> str:
        """GitHub raw content URL."""
        owner, repo = self.github_repo.split("/")
        return f"https://raw.githubusercontent.com/{owner}/{repo}/{self.github_branch}"

    @property
    def use_graphiti(self) -> bool:
        """Whether Graphiti (Neo4j) is configured."""
        return bool(self.neo4j_password and (self.openai_api_key or self.anthropic_api_key))


settings = Settings()