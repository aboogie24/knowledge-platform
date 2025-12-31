"""Configuration for the GraphQL graph API."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # Server
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8081)
    log_level: str = Field(default="INFO")

    # Neo4j / Graphiti
    neo4j_uri: str = Field(default="bolt://localhost:7687")
    neo4j_user: str = Field(default="neo4j")
    neo4j_password: str = Field(default="")
    neo4j_auth: str = Field(
        default="",
        description="Optional NEO4J_AUTH value (format neo4j/<password>) from Neo4j secret",
    )

    # LLM configuration (Graphiti search may rely on rerankers/embedders)
    openai_api_key: str = Field(default="")
    openai_model: str = Field(default="gpt-4o-mini")
    anthropic_api_key: str = Field(default="")
    anthropic_model: str = Field(default="claude-3-5-sonnet-20240620")
    use_anthropic: bool = Field(default=True)

    graphiti_group_ids: list[str] = Field(default_factory=list)

    @property
    def neo4j_password_value(self) -> str:
        """Resolve Neo4j password, preferring explicit password then NEO4J_AUTH."""
        if self.neo4j_password:
            return self.neo4j_password
        if self.neo4j_auth:
            if "/" in self.neo4j_auth:
                return self.neo4j_auth.split("/", 1)[1]
            return self.neo4j_auth
        return ""


settings = Settings()
