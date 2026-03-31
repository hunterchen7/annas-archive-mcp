import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { search, getByMd5, getStats } from "./db.js";
import { getDownloadUrl } from "./download.js";

export function createServer(secretKey?: string): McpServer {
  const server = new McpServer({
    name: "annas-archive",
    version: "1.0.0",
    description: "Search and download books, papers, and documents from a local Anna's Archive metadata index. Use search to find documents, then download to get a direct URL. Download the file locally with curl: curl -L -o <filename> '<url>'",
  });

  server.tool(
    "search",
    `Search the local Anna's Archive metadata index for books, papers, and other documents. Returns metadata and MD5 hashes for downloading.

SEARCH BEHAVIOR:
- Uses AND matching — all query terms must appear across title, author, or publisher fields. More terms = fewer results.
- Use 2-3 specific terms for best results. Avoid long natural language queries.
- Diacritic-insensitive — "Zizek" matches "Žižek", "Simulacre" matches "Simulacré".
- Stopwords like "and", "the", "of" are ignored by the search engine.
- If AND matching returns no results, automatically falls back to OR matching (ranked by relevance).

QUERY STRATEGIES:
- For a specific book: use key title words + author surname. e.g. "Parallax View Zizek" not "The Parallax View by Slavoj Žižek"
- For an author's works: just use the author name. e.g. "Baudrillard"
- For DOI lookup: pass the DOI directly e.g. "10.1038/nature12345"
- For ISBN lookup: pass the ISBN directly (10 or 13 digits)
- For non-English titles: search in the original language. e.g. "三國演義" not "Romance of the Three Kingdoms"
- If a query returns no results, try fewer terms or broader keywords.

RESULTS include: title, author, year, language, format, file size, MD5 hash, ISBN/DOI if available. Use the MD5 hash with the download tool to get the file.`,
    {
      query: z.string().describe("Search query — 2-3 key terms from title/author, a DOI, or an ISBN. Fewer terms = more results."),
      language: z.string().optional().describe("Filter by language (e.g. 'english', 'chinese', 'french')"),
      format: z.string().optional().describe("Filter by file format (e.g. 'pdf', 'epub', 'djvu')"),
      limit: z.number().min(1).max(50).optional().describe("Max results to return (default 10, max 50)"),
    },
    async ({ query, language, format, limit }) => {
      const results = await search({ query, language, format, limit });
      if (results.length === 0) {
        return { content: [{ type: "text", text: "No results found. Try fewer search terms, or search in the original language for non-English titles." }] };
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
    "Get a direct download URL for a document by its MD5 hash (from search results). Returns a temporary download link — use it promptly. Present the URL as a clickable markdown link to the user. To save locally, use: curl -L -o filename.epub '<url>'",
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
    "Get statistics about the local Anna's Archive metadata index — total records and breakdown by source collection.",
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
