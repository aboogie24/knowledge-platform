# Graph API environment template
#
# Copy to .env and fill in required secrets.

# Server
HOST=0.0.0.0
PORT=8081
LOG_LEVEL=INFO

# Neo4j / Graphiti
NEO4J_URI=bolt://neo4j:7687
NEO4J_USER=neo4j
# Provide either NEO4J_PASSWORD or NEO4J_AUTH (neo4j/<password>)
NEO4J_PASSWORD=
NEO4J_AUTH=

# LLM configuration (Anthropic preferred; falls back to OpenAI)
ANTHROPIC_API_KEY=
ANTHROPIC_MODEL=claude-3-5-sonnet-20240620
USE_ANTHROPIC=true

OPENAI_API_KEY=
OPENAI_MODEL=gpt-4o-mini

# Optional: restrict searches to group ids (comma separated)
GRAPHITI_GROUP_IDS=
