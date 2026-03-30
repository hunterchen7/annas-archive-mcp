import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { search, getByMd5, getStats } from "./db.js";
import { download } from "./download.js";

export function createServer(): McpServer {
  const server = new McpServer({
    name: "annas-archive",
    version: "1.0.0",
  });

  server.tool(
    "search",
    "Search the local Anna's Archive metadata index for books, papers, and other documents by title, author, DOI, ISBN, or keywords. Returns metadata and MD5 hashes for downloading.",
    {
      query: z.string().describe("Search query — title, author name, keywords, DOI, or ISBN"),
      language: z.string().optional().describe("Filter by language (e.g. 'english', 'chinese', 'french')"),
      format: z.string().optional().describe("Filter by file format (e.g. 'pdf', 'epub', 'djvu')"),
      limit: z.number().min(1).max(50).optional().describe("Max results to return (default 10, max 50)"),
    },
    async ({ query, language, format, limit }) => {
      const results = await search({ query, language, format, limit });
      if (results.length === 0) {
        return { content: [{ type: "text", text: "No results found." }] };
      }
      const formatted = results.map((doc, i) => {
        const parts = [`${i + 1}. **${doc.title || "Untitled"}**`];
        if (doc.author) parts.push(`   Author: ${doc.author}`);
        if (doc.year) parts.push(`   Year: ${doc.year}`);
        if (doc.language) parts.push(`   Language: ${doc.language}`);
        if (doc.extension) parts.push(`   Format: ${doc.extension}`);
        if (doc.filesize) parts.push(`   Size: ${(doc.filesize / 1024 / 1024).toFixed(1)} MB`);
        if (doc.doi) parts.push(`   DOI: ${doc.doi}`);
        if (doc.isbn) parts.push(`   ISBN: ${doc.isbn}`);
        parts.push(`   Source: ${doc.source}`);
        parts.push(`   MD5: ${doc.md5}`);
        return parts.join("\n");
      });
      return {
        content: [{ type: "text", text: `Found ${results.length} results:\n\n${formatted.join("\n\n")}` }],
      };
    }
  );

  server.tool(
    "download",
    "Download a document by its MD5 hash using the Anna's Archive fast download API. Requires member API key.",
    {
      md5: z.string().length(32).describe("MD5 hash of the document (from search results)"),
      filename: z.string().optional().describe("Desired filename for the download"),
    },
    async ({ md5, filename }) => {
      // Look up metadata first
      const doc = await getByMd5(md5);
      const result = await download(md5, filename || (doc ? `${md5}.${doc.extension || "bin"}` : undefined));

      if (result.error) {
        return { content: [{ type: "text", text: `Download failed: ${result.error}` }], isError: true };
      }

      let text = `Downloaded to: ${result.filePath}`;
      if (doc) {
        text += `\nTitle: ${doc.title || "Unknown"}`;
        if (doc.author) text += `\nAuthor: ${doc.author}`;
      }

      return { content: [{ type: "text", text }] };
    }
  );

  server.tool(
    "stats",
    "Get statistics about the local Anna's Archive metadata index — total records, records per source.",
    {},
    async () => {
      const stats = await getStats();
      const lines = [`Total documents: ${stats.total.toLocaleString()}\n\nBy source:`];
      for (const [source, count] of Object.entries(stats.by_source)) {
        lines.push(`  ${source}: ${count.toLocaleString()}`);
      }
      return { content: [{ type: "text", text: lines.join("\n") }] };
    }
  );

  return server;
}
