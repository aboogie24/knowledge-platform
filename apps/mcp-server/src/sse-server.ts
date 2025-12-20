/**
 * Knowledge Platform MCP Server - SSE Transport
 * 
 * HTTP server for MCP over Server-Sent Events.
 * Used for deployment in Kubernetes where stdio isn't available.
 */

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { SSEServerTransport } from "@modelcontextprotocol/sdk/server/sse.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
  Tool,
} from "@modelcontextprotocol/sdk/types.js";
import { MeiliSearch } from "meilisearch";
import { createServer } from "http";
import { parse } from "url";
import { pino, type LevelWithSilent } from "pino";

// Configuration
const config = {
  meilisearchUrl: process.env.MEILISEARCH_URL || "http://localhost:7700",
  meilisearchApiKey: process.env.MEILISEARCH_API_KEY || "",
  indexName: process.env.MEILI_INDEX_NAME || "documents",
  chunksIndexName: process.env.MEILI_INDEX_NAME 
    ? `${process.env.MEILI_INDEX_NAME}_chunks` 
    : "documents_chunks",
  port: parseInt(process.env.PORT || "3000"),
  logLevel: (process.env.LOG_LEVEL || "info") as LevelWithSilent,
};

// Logger
const logger = pino({
  level: config.logLevel,
});

// Meilisearch client
const meili = new MeiliSearch({
  host: config.meilisearchUrl,
  apiKey: config.meilisearchApiKey,
});

// Tool definitions (same as stdio version)
const tools: Tool[] = [
  {
    name: "search_docs",
    description: "Search the knowledge base for documents matching a query.",
    inputSchema: {
      type: "object",
      properties: {
        query: { type: "string", description: "Search query" },
        limit: { type: "number", description: "Max results (default: 10)" },
        tags: { type: "array", items: { type: "string" }, description: "Filter by tags" },
      },
      required: ["query"],
    },
  },
  {
    name: "search_chunks",
    description: "Search document chunks for granular results.",
    inputSchema: {
      type: "object",
      properties: {
        query: { type: "string", description: "Search query" },
        limit: { type: "number", description: "Max chunks (default: 10)" },
        document_id: { type: "string", description: "Filter to specific document" },
      },
      required: ["query"],
    },
  },
  {
    name: "get_document",
    description: "Retrieve a specific document by ID.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "Document ID or path" },
      },
      required: ["id"],
    },
  },
  {
    name: "lookup_decision",
    description: "Look up why a technology/pattern is or isn't used.",
    inputSchema: {
      type: "object",
      properties: {
        question: { type: "string", description: "Question about a decision" },
      },
      required: ["question"],
    },
  },
];

// Tool handlers (simplified)
async function handleSearchDocs(args: { query: string; limit?: number; tags?: string[] }) {
  const { query, limit = 10, tags } = args;
  const index = meili.index(config.indexName);
  
  const searchParams: any = {
    limit: Math.min(limit, 50),
    attributesToHighlight: ["content", "title"],
    highlightPreTag: "**",
    highlightPostTag: "**",
  };
  
  if (tags?.length) {
    searchParams.filter = tags.map((t) => `tags = "${t}"`).join(" OR ");
  }
  
  const results = await index.search(query, searchParams);
  
  return {
    total: results.estimatedTotalHits,
    results: results.hits.map((hit: any) => ({
      id: hit.id,
      title: hit.title,
      path: hit.path,
      tags: hit.tags || [],
      snippet: hit._formatted?.content?.slice(0, 300) || "",
    })),
  };
}

async function handleSearchChunks(args: { query: string; limit?: number; document_id?: string }) {
  const { query, limit = 10, document_id } = args;
  const index = meili.index(config.chunksIndexName);
  
  const searchParams: any = { limit };
  if (document_id) searchParams.filter = `document_id = "${document_id}"`;
  
  const results = await index.search(query, searchParams);
  
  return {
    total: results.estimatedTotalHits,
    chunks: results.hits,
  };
}

async function handleGetDocument(args: { id: string }) {
  const index = meili.index(config.indexName);
  try {
    return await index.getDocument(args.id);
  } catch {
    const results = await index.search("", { filter: `path = "${args.id}"`, limit: 1 });
    if (results.hits.length) return results.hits[0];
    throw new Error(`Document not found: ${args.id}`);
  }
}

async function handleLookupDecision(args: { question: string }) {
  const results = await handleSearchDocs({ query: args.question, limit: 5 });
  
  if (!results.results.length) {
    return { found: false, message: "No relevant decisions found." };
  }
  
  return {
    found: true,
    question: args.question,
    relevant_documents: results.results,
  };
}

// Create MCP server
function createMCPServer() {
  const server = new Server(
    { name: "knowledge-platform", version: "0.1.0" },
    { capabilities: { tools: {} } }
  );

  server.setRequestHandler(ListToolsRequestSchema, async () => ({ tools }));

  server.setRequestHandler(CallToolRequestSchema, async (request) => {
    const { name, arguments: args } = request.params;
    
    try {
      let result: any;
      switch (name) {
        case "search_docs":
          result = await handleSearchDocs(args as any);
          break;
        case "search_chunks":
          result = await handleSearchChunks(args as any);
          break;
        case "get_document":
          result = await handleGetDocument(args as any);
          break;
        case "lookup_decision":
          result = await handleLookupDecision(args as any);
          break;
        default:
          throw new Error(`Unknown tool: ${name}`);
      }
      
      return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
    } catch (error: any) {
      return { content: [{ type: "text", text: JSON.stringify({ error: error.message }) }], isError: true };
    }
  });

  return server;
}

// HTTP server with SSE transport
const httpServer = createServer(async (req, res) => {
  const url = parse(req.url || "", true);
  
  // Health check
  if (url.pathname === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "healthy" }));
    return;
  }
  
  // MCP SSE endpoint
  if (url.pathname === "/mcp" || url.pathname === "/sse") {
    const server = createMCPServer();
    const transport = new SSEServerTransport("/mcp", res);
    
    res.on("close", () => {
      logger.info("SSE connection closed");
    });
    
    await server.connect(transport);
    logger.info("SSE connection established");
    return;
  }
  
  // 404 for other paths
  res.writeHead(404);
  res.end("Not found");
});

httpServer.listen(config.port, () => {
  logger.info({ port: config.port, meilisearch: config.meilisearchUrl }, "MCP SSE server started");
});
