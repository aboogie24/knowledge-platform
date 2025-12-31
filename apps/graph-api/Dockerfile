FROM python:3.11-slim AS builder

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY pyproject.toml .
COPY src ./src

# Install dependencies into a wheelhouse to reduce final image size
RUN pip install --no-cache-dir --upgrade pip && \
    pip wheel --no-cache-dir --wheel-dir /wheels .

FROM python:3.11-slim AS runtime

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Copy wheels and install
COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir /wheels/*

# Copy source for runtime (for app code)
COPY src ./src

# Env defaults (override in runtime)
ENV HOST=0.0.0.0 \
    PORT=8081 \
    LOG_LEVEL=INFO

EXPOSE 8081

CMD ["uvicorn", "graph_api.main:app", "--factory", "--host", "0.0.0.0", "--port", "8081"]
