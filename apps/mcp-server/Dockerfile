# Build stage
FROM node:20-slim AS builder

WORKDIR /app

# Copy package files
COPY package.json package-lock.json* ./

# Install dependencies
RUN npm ci

# Copy source code
COPY tsconfig.json ./
COPY src/ ./src/

# Build TypeScript
RUN npm run build

# Production stage
FROM node:20-slim

WORKDIR /app

# Create non-root user
RUN useradd -m -u 1000 appuser

# Copy package files and install production dependencies only
COPY package.json package-lock.json* ./
RUN npm ci --only=production && npm cache clean --force

# Copy built application
COPY --from=builder /app/dist ./dist

# Set ownership
RUN chown -R appuser:appuser /app

USER appuser

# Environment variables
ENV NODE_ENV=production \
    LOG_LEVEL=info

# The MCP server uses stdio transport, so no port to expose
# For SSE transport, uncomment:
# EXPOSE 3000

CMD ["node", "dist/index.js"]