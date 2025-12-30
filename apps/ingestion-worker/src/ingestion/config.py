"""Configuration for ingestion worker."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # GitHub configuration
    github_repo: str = Field(description="GitHub repo in format 'owner/repo'")
    github_branch: str = Field(default="main", description="Branch to watch")
    github_docs_path: str = Field(
        default="docs",
        description="Path to docs in repo",
        alias="GITHUB_DOCS_PATH",
    )
    github_token: str = Field(description="GitHub PAT for API access")
    github_webhook_secret: str = Field(default="", description="Webhook secret for verification")
    slack_signing_secret: str = Field(default="", description="Slack signing secret for Events API")

    # Meilisearch configuration
    meilisearch_url: str = Field(default="http://localhost:7700")
    meilisearch_api_key: str = Field(default="")
    meili_index_name: str = Field(default="documents")

    # Neo4j configuration (optional, for Graphiti)
    neo4j_uri: str = Field(default="bolt://localhost:7687")
    neo4j_user: str = Field(default="neo4j")
    neo4j_password: str = Field(default="")
    neo4j_auth: str = Field(
        default="",
        description="Optional NEO4J_AUTH value (format neo4j/<password>) from Neo4j secret",
    )
    neo4j_enabled: bool = Field(default=True, description="Toggle Graphiti/Neo4j integration")

    # LLM configuration (for Graphiti semantic extraction)
    openai_api_key: str = Field(default="")
    openai_model: str = Field(default="gpt-4o-mini")
    anthropic_api_key: str = Field(default="")
    anthropic_model: str = Field(default="claude-sonnet-4-20250514")
    use_anthropic: bool = Field(default=True)

    # Ingestion settings
    ingestion_mode: str = Field(default="webhook", description="webhook, poll, or manual")
    poll_interval_seconds: int = Field(default=300)
    chunk_size: int = Field(default=1000, description="Characters per chunk")
    chunk_overlap: int = Field(default=200, description="Overlap between chunks")

    # Redis / queue settings
    redis_url: str = Field(default="redis://localhost:6379/0")
    redis_stream: str = Field(default="ingestion:events")
    redis_dlq_stream: str = Field(default="ingestion:dlq")
    redis_consumer_group: str = Field(default="ingestion-workers")
    redis_consumer_name: str = Field(default="ingestion-worker")

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
        if not self.neo4j_enabled:
            return False
        return bool(
            self.neo4j_password_value and (self.openai_api_key or self.anthropic_api_key)
        )

    @property
    def neo4j_password_value(self) -> str:
        """Resolve Neo4j password, preferring explicit password then NEO4J_AUTH."""
        if self.neo4j_password:
            return self.neo4j_password
        if self.neo4j_auth:
            # NEO4J_AUTH is typically "neo4j/<password>"
            if "/" in self.neo4j_auth:
                return self.neo4j_auth.split("/", 1)[1]
            return self.neo4j_auth
        return ""


settings = Settings()
