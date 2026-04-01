import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { search, getByMd5, getStats } from "./db.js";
import { getDownloadUrl } from "./download.js";
import { readDocument } from "./reader.js";

export function createServer(secretKey?: string): McpServer {
  const server = new McpServer({
    name: "annas-archive",
    version: "1.0.0",
    description: `Search ~48M books, papers, and documents from a local Anna's Archive metadata index.

Tools: search → download or read.

search: Find documents using any combination of title, author, year_from/year_to, publisher, isbn, doi, language, format. Example: search(title="Simulacra", author="Baudrillard", format="pdf") or search(query="machine learning", year_from=2023, language="english").

download: Get a direct download URL by MD5 hash from search results. Requires an Anna's Archive membership API key (in X-Annas-Secret-Key header).

read: Extract and return full text from a document by MD5 hash. Supports PDF, EPUB, DJVU, MOBI, and more. Use start_page/end_page to paginate.`,
  });

  server.tool(
    "search",
    `Search the local Anna's Archive metadata index (~48M books, papers, and documents). Returns metadata and MD5 hashes for downloading.

PARAMETERS — use any combination:
- "query": General full-text search across title + author + publisher. Best for broad searches.
- "title": Search within titles only. Use this when you know the book/paper name.
- "author": Search within authors only. Use this to find works by a specific person.
- "publisher": Search within publishers only.
- "year_from" / "year_to": Filter by publication year range (e.g. year_from=2020, year_to=2024).
- "isbn": Exact ISBN lookup (10 or 13 digits, hyphens OK).
- "doi": Exact DOI lookup (e.g. "10.1038/nature12345").
- "language": Filter by language — lowercase English name (e.g. "english", "chinese", "french", "german", "spanish", "russian", "japanese", "arabic").
- "format": Filter by file format (e.g. "pdf", "epub", "djvu", "mobi", "fb2", "azw3").
- "limit": Max results (default 10, max 50).

SEARCH BEHAVIOR:
- All text params use AND matching — all terms must appear. More terms = fewer, more precise results.
- Diacritic-insensitive: "Zizek" matches "Žižek".
- Stopwords ("the", "of", "and") are ignored.
- If "query" AND matching returns nothing, automatically falls back to OR matching.
- You can combine params freely: title="Pedagogy" + author="Freire" + format="pdf" + language="english".

QUERY STRATEGIES:
- Specific book: use "title" + "author". e.g. title="Parallax View", author="Zizek"
- Author's works: use "author" alone. e.g. author="Baudrillard"
- Broad topic: use "query". e.g. query="machine learning neural networks"
- Recent papers: use "query" or "title" + year_from/year_to. e.g. query="transformer attention", year_from=2023
- Non-English: search in original language. e.g. title="三國演義"
- If no results, try fewer terms or use "query" instead of specific fields.

RESULTS include: title, author, year, language, format, file size, MD5 hash, ISBN/DOI if available. Use the MD5 with the download or read tools.`,
    {
      query: z.string().optional().describe("General full-text search across title, author, and publisher. Use 2-3 key terms, e.g. 'machine learning transformers'. Avoid full sentences."),
      title: z.string().optional().describe("Full-text search within titles only. e.g. 'Parallax View'. Partial matches work — 'Simulacra' matches 'Simulacra and Simulation'."),
      author: z.string().optional().describe("Full-text search within authors only. Use surname or full name, e.g. 'Baudrillard' or 'Jean Baudrillard'."),
      year_from: z.number().optional().describe("Minimum publication year (inclusive). 4-digit year, e.g. 2020."),
      year_to: z.number().optional().describe("Maximum publication year (inclusive). 4-digit year, e.g. 2024."),
      publisher: z.string().optional().describe("Full-text search within publishers only. e.g. 'Oxford University Press'."),
      isbn: z.string().optional().describe("Exact ISBN lookup. 10 or 13 digits, hyphens are stripped automatically. e.g. '978-0-14-044793-4' or '9780140447934'."),
      doi: z.string().optional().describe("Exact DOI lookup. e.g. '10.1038/nature12345'."),
      language: z.string().optional().describe("Filter by language. Lowercase English name: 'english', 'chinese', 'french', 'german', 'spanish', 'russian', 'japanese', 'arabic', 'italian', 'portuguese', 'korean'."),
      format: z.string().optional().describe("Filter by file format. Lowercase extension: 'pdf', 'epub', 'djvu', 'mobi', 'fb2', 'azw3', 'txt', 'docx', 'lit', 'rtf'."),
      limit: z.number().min(1).max(50).optional().describe("Max results to return. Default 10, max 50. Use higher values for broad searches."),
    },
    async ({ query, title, author, year_from, year_to, publisher, isbn, doi, language, format, limit }) => {
      if (!query && !title && !author && !isbn && !doi) {
        return { content: [{ type: "text", text: "Please provide at least one search parameter: query, title, author, isbn, or doi." }], isError: true };
      }
      const results = await search({
        query, title, author,
        yearFrom: year_from, yearTo: year_to,
        publisher, isbn, doi, language, format, limit,
      });
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
    `Get a direct download URL for a document by its MD5 hash (from search results). Returns a temporary download link — use it promptly.

Requires an Anna's Archive membership API key configured in client headers (X-Annas-Secret-Key).

Present the URL as a clickable markdown link. To save locally: curl -L -o filename.ext '<url>'`,
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

  server.tool(
    "read",
    `Read the text content of a document by its MD5 hash. Downloads the file, extracts text, and returns it page by page. Supports PDF, EPUB, DJVU, MOBI, AZW3, FB2, DOCX, RTF, and plain text. Results are cached — subsequent reads are instant.

Requires an Anna's Archive membership API key (configured in client headers) to download files not already cached.

BEHAVIOR:
- No page range → returns page count + first page preview. Use this first to understand the document.
- start_page only → returns 20 pages starting from that page.
- start_page + end_page → returns that exact range.
- Output capped at 50k characters. Request smaller ranges for large documents.

TYPICAL WORKFLOW:
1. search(title="Pedagogy", author="Freire") → find document, get MD5
2. read(md5) → get page count and preview
3. read(md5, start_page=1, end_page=10) → read first 10 pages
4. read(md5, start_page=11, end_page=20) → continue reading`,
    {
      md5: z.string().length(32).describe("MD5 hash of the document (from search results)"),
      start_page: z.number().min(1).optional().describe("First page to return (1-indexed). Omit to get document overview."),
      end_page: z.number().min(1).optional().describe("Last page to return (inclusive). Omit to read from start_page to the cap."),
    },
    async ({ md5, start_page, end_page }) => {
      const doc = await getByMd5(md5);
      const ext = doc?.extension || "pdf";

      let pageRange: string | undefined;
      if (start_page != null) {
        if (end_page != null) {
          pageRange = `${start_page}-${end_page}`;
        } else {
          // Default: start_page to start_page + 20 (reasonable chunk)
          pageRange = `${start_page}-${start_page + 19}`;
        }
      }

      const result = await readDocument(md5, ext, secretKey || "", pageRange);

      if (result.error) {
        return { content: [{ type: "text", text: `Read failed: ${result.error}` }], isError: true };
      }

      let header = "";
      if (doc) {
        header += `**${doc.title || "Untitled"}**`;
        if (doc.author) header += ` by ${doc.author}`;
        header += "\n\n";
      }

      return { content: [{ type: "text", text: header + (result.text || "") }] };
    }
  );

  return server;
}
