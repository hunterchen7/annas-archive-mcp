import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { search, getByMd5, getStats } from "./db.js";
import { getDownloadUrl } from "./download.js";

export function createServer(secretKey?: string): McpServer {
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
    "Get a direct download URL for a document by its MD5 hash. Returns a temporary download link. To download the file locally, use the returned URL with curl or wget (e.g. `curl -L -o filename.epub '<url>'`). Always present the URL as a clickable markdown link to the user.",
    {
      md5: z.string().length(32).describe("MD5 hash of the document (from search results)"),
    },
    async ({ md5 }) => {
      const doc = await getByMd5(md5);
      const result = await getDownloadUrl(md5, secretKey || "");

      if (result.error) {
        return { content: [{ type: "text", text: `Download failed: ${result.error}` }], isError: true };
      }

      let text = `Download URL: ${result.downloadUrl}`;
      if (doc) {
        text += `\nTitle: ${doc.title || "Unknown"}`;
        if (doc.author) text += `\nAuthor: ${doc.author}`;
        if (doc.extension) text += `\nFormat: ${doc.extension}`;
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
