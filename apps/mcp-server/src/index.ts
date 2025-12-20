/**
 * Knowledge Platform MCP Server
 * 
 * Exposes search tools for AI clients via the Model Context Protocol.
 */

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
  Tool,
} from "@modelcontextprotocol/sdk/types.js";
import { MeiliSearch } from "meilisearch";
import { z } from "zod";
import { pino, type LevelWithSilent } from "pino";

// Configuration
const config = {
  meilisearchUrl: process.env.MEILISEARCH_URL || "http://localhost:7700",
  meilisearchApiKey: process.env.MEILISEARCH_API_KEY || "",
  indexName: process.env.MEILI_INDEX_NAME || "documents",
  chunksIndexName: process.env.MEILI_INDEX_NAME ? `${process.env.MEILI_INDEX_NAME}_chunks` : "documents_chunks",
  logLevel: (process.env.LOG_LEVEL || "info") as LevelWithSilent,
};

// Logger
const logger = pino({
  level: config.logLevel,
  transport: {
    target: "pino-pretty",
    options: { colorize: true },
  },
});

// Meilisearch client
const meili = new MeiliSearch({
  host: config.meilisearchUrl,
  apiKey: config.meilisearchApiKey,
});

// Tool definitions
const tools: Tool[] = [
  {
    name: "search_docs",
    description: `Search the knowledge base for documents matching a query. Returns titles, paths, and relevant snippets.
    
Use this tool to find:
- Architecture decisions (ADRs)
- Technical documentation
- Best practices and guidelines
- Historical context about decisions

Returns up to 10 results by default.`,
    inputSchema: {
      type: "object",
      properties: {
        query: {
          type: "string",
          description: "Search query - can be keywords, questions, or natural language",
        },
        limit: {
          type: "number",
          description: "Maximum number of results (default: 10, max: 50)",
          default: 10,
        },
        tags: {
          type: "array",
          items: { type: "string" },
          description: "Filter by tags (optional)",
        },
      },
      required: ["query"],
    },
  },
  {
    name: "search_chunks",
    description: `Search document chunks for more granular results. Use this when you need specific passages or when search_docs returns documents that are too long.
    
Returns smaller text chunks (~1000 chars) with surrounding context.`,
    inputSchema: {
      type: "object",
      properties: {
        query: {
          type: "string",
          description: "Search query",
        },
        limit: {
          type: "number",
          description: "Maximum number of chunks (default: 10)",
          default: 10,
        },
        document_id: {
          type: "string",
          description: "Filter to chunks from a specific document (optional)",
        },
      },
      required: ["query"],
    },
  },
  {
    name: "get_document",
    description: `Retrieve a specific document by its ID or path. Use this after search_docs to get the full content of a document.`,
    inputSchema: {
      type: "object",
      properties: {
        id: {
          type: "string",
          description: "Document ID (from search results) or path",
        },
      },
      required: ["id"],
    },
  },
  {
    name: "list_tags",
    description: `List all available tags in the knowledge base. Useful for discovering what topics are documented.`,
    inputSchema: {
      type: "object",
      properties: {},
    },
  },
  {
    name: "lookup_decision",
    description: `Look up why a specific technology or pattern is or isn't used. This is a convenience tool that combines search with answer synthesis.
    
Example queries:
- "Why don't we use Redis for sessions?"
- "What's our GitOps strategy?"
- "Why was vpc-foo deprecated?"
- "What's the decision on container registry?"

Returns a structured answer with:
- Summary of the decision
- Status (approved, deprecated, rejected, etc.)
- Alternatives (if any)
- Source documents`,
    inputSchema: {
      type: "object",
      properties: {
        question: {
          type: "string",
          description: "Question about a technology decision or pattern",
        },
      },
      required: ["question"],
    },
  },
];

// Tool handlers
async function handleSearchDocs(args: {
  query: string;
  limit?: number;
  tags?: string[];
}) {
  const { query, limit = 10, tags } = args;
  
  const index = meili.index(config.indexName);
  
  const searchParams: any = {
    limit: Math.min(limit, 50),
    attributesToHighlight: ["content", "title", "description"],
    highlightPreTag: "**",
    highlightPostTag: "**",
    attributesToRetrieve: [
      "id",
      "title",
      "path",
      "description",
      "tags",
      "source_url",
      "updated_at",
    ],
  };
  
  if (tags && tags.length > 0) {
    searchParams.filter = tags.map((t) => `tags = "${t}"`).join(" OR ");
  }
  
  const results = await index.search(query, searchParams);
  
  logger.info({ query, hits: results.hits.length }, "search_docs");
  
  return {
    total: results.estimatedTotalHits,
    results: results.hits.map((hit: any) => ({
      id: hit.id,
      title: hit.title,
      path: hit.path,
      description: hit.description || "",
      tags: hit.tags || [],
      source_url: hit.source_url,
      updated_at: hit.updated_at,
      snippet: hit._formatted?.description || hit._formatted?.content?.slice(0, 300) || "",
    })),
  };
}

async function handleSearchChunks(args: {
  query: string;
  limit?: number;
  document_id?: string;
}) {
  const { query, limit = 10, document_id } = args;
  
  const index = meili.index(config.chunksIndexName);
  
  const searchParams: any = {
    limit: Math.min(limit, 50),
    attributesToHighlight: ["content"],
    highlightPreTag: "**",
    highlightPostTag: "**",
  };
  
  if (document_id) {
    searchParams.filter = `document_id = "${document_id}"`;
  }
  
  const results = await index.search(query, searchParams);
  
  logger.info({ query, hits: results.hits.length }, "search_chunks");
  
  return {
    total: results.estimatedTotalHits,
    chunks: results.hits.map((hit: any) => ({
      id: hit.id,
      document_id: hit.document_id,
      title: hit.title,
      path: hit.path,
      content: hit._formatted?.content || hit.content,
      chunk_index: hit.chunk_index,
      total_chunks: hit.total_chunks,
    })),
  };
}

async function handleGetDocument(args: { id: string }) {
  const { id } = args;
  
  const index = meili.index(config.indexName);
  
  try {
    const doc = await index.getDocument(id);
    logger.info({ id }, "get_document");
    return doc;
  } catch (error) {
    // Try searching by path
    const results = await index.search("", {
      filter: `path = "${id}"`,
      limit: 1,
    });
    
    if (results.hits.length > 0) {
      return results.hits[0];
    }
    
    throw new Error(`Document not found: ${id}`);
  }
}

async function handleListTags() {
  const index = meili.index(config.indexName);
  
  // Get facet distribution for tags
  const results = await index.search("", {
    facets: ["tags"],
    limit: 0,
  });
  
  const tagCounts = results.facetDistribution?.tags || {};
  
  logger.info({ tagCount: Object.keys(tagCounts).length }, "list_tags");
  
  return {
    tags: Object.entries(tagCounts)
      .map(([tag, count]) => ({ tag, count }))
      .sort((a, b) => (b.count as number) - (a.count as number)),
  };
}

async function handleLookupDecision(args: { question: string }) {
  const { question } = args;
  
  // Search for relevant documents
  const results = await handleSearchDocs({ query: question, limit: 5 });
  
  logger.info({ question, hits: results.results.length }, "lookup_decision");
  
  if (results.results.length === 0) {
    return {
      found: false,
      message: "No relevant decisions found for this question.",
      suggestion: "Try rephrasing your question or use search_docs with different keywords.",
    };
  }
  
  // Return structured results for the AI to synthesize
  return {
    found: true,
    question,
    relevant_documents: results.results.map((doc) => ({
      title: doc.title,
      path: doc.path,
      description: doc.description,
      tags: doc.tags,
      source_url: doc.source_url,
      snippet: doc.snippet,
    })),
    note: "Synthesize an answer from these documents. If the answer is unclear, say so.",
  };
}

// Main server setup
async function main() {
  const server = new Server(
    {
      name: "knowledge-platform",
      version: "0.1.0",
    },
    {
      capabilities: {
        tools: {},
      },
    }
  );

  // Handle tool listing
  server.setRequestHandler(ListToolsRequestSchema, async () => {
    return { tools };
  });

  // Handle tool calls
  server.setRequestHandler(CallToolRequestSchema, async (request) => {
    const { name, arguments: args } = request.params;

    logger.debug({ tool: name, args }, "tool_call");

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
        case "list_tags":
          result = await handleListTags();
          break;
        case "lookup_decision":
          result = await handleLookupDecision(args as any);
          break;
        default:
          throw new Error(`Unknown tool: ${name}`);
      }

      return {
        content: [
          {
            type: "text",
            text: JSON.stringify(result, null, 2),
          },
        ],
      };
    } catch (error: any) {
      logger.error({ tool: name, error: error.message }, "tool_error");
      
      return {
        content: [
          {
            type: "text",
            text: JSON.stringify({ error: error.message }),
          },
        ],
        isError: true,
      };
    }
  });

  // Start server
  const transport = new StdioServerTransport();
  await server.connect(transport);
  
  logger.info({ meilisearch: config.meilisearchUrl }, "MCP server started");
}

main().catch((error) => {
  logger.error({ error: error.message }, "Failed to start server");
  process.exit(1);
});
