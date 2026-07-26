import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { search, getByMd5, getStats } from "./db.js";
import { getDownloadUrl } from "./download.js";
import { readDocument } from "./reader.js";
import { keyValidationError } from "./auth.js";
import { MD5_PATTERN } from "./identifiers.js";
import {
  HeaderCredential,
  type MembershipCredential,
} from "./credential.js";

const oauthSecurity = {
  securitySchemes: [{ type: "oauth2", scopes: ["annas:use"] }],
};

export function createServer(
  credentialOrKey: MembershipCredential | string = "",
): McpServer {
  const credential = typeof credentialOrKey === "string"
    ? new HeaderCredential(credentialOrKey)
    : credentialOrKey;
  const server = new McpServer({
    name: "annas-archive",
    version: "1.0.0",
    description: `Search ~48M books, papers, and documents from a local Anna's Archive metadata index.

Tools: search → download or read.

search: Find documents using any combination of title, author, year_from/year_to, publisher, isbn, doi, language, format. Example: search(title="Simulacra", author="Baudrillard", format="pdf") or search(query="machine learning", year_from=2023, language="english").

download: Get a fast download URL by MD5 hash from search results. Requires a linked Anna's Archive membership key through OAuth or X-Annas-Secret-Key.

read: Extract and return full text from a document by MD5 hash. Also requires membership secret key. Supports PDF, EPUB, DJVU, MOBI, and more. Use start_page/end_page to paginate, or list_chapters=true / chapter=N to navigate by chapter.`,
  });

  server.registerTool(
    "search",
    {
      description: `Search the local Anna's Archive metadata index (~48M books, papers, and documents). Returns metadata and MD5 hashes for downloading.

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

FORMAT TIPS (for "read" workflows):
- When the same work is available as both PDF and EPUB, EPUB is surfaced first automatically — EPUBs have native chapter structure, so chapter-based reading is accurate.
- For reading books (not papers), prefer EPUB when available. The read tool's chapter/list_chapters features work best on EPUB.
- For papers and articles, PDF is usually the only option and works fine with page-based reading.
- Only set format="pdf" explicitly if the user specifically needs the PDF version.

RESULTS include: title, author, year, language, format, file size, MD5 hash, ISBN/DOI if available. Use the MD5 with the download or read tools.`,
      inputSchema: {
        query: z.string().trim().min(1).max(256).optional().describe("General full-text search across title, author, and publisher. Use 2-3 key terms, e.g. 'machine learning transformers'. Avoid full sentences."),
        title: z.string().trim().min(1).max(256).optional().describe("Full-text search within titles only. e.g. 'Parallax View'. Partial matches work — 'Simulacra' matches 'Simulacra and Simulation'."),
        author: z.string().trim().min(1).max(256).optional().describe("Full-text search within authors only. Use surname or full name, e.g. 'Baudrillard' or 'Jean Baudrillard'."),
        year_from: z.number().int().min(0).max(3000).optional().describe("Minimum publication year (inclusive). 4-digit year, e.g. 2020."),
        year_to: z.number().int().min(0).max(3000).optional().describe("Maximum publication year (inclusive). 4-digit year, e.g. 2024."),
        publisher: z.string().trim().min(1).max(256).optional().describe("Full-text search within publishers only. e.g. 'Oxford University Press'."),
        isbn: z.string().trim().min(1).max(32).optional().describe("Exact ISBN lookup. 10 or 13 digits, hyphens are stripped automatically. e.g. '978-0-14-044793-4' or '9780140447934'."),
        doi: z.string().trim().min(1).max(256).optional().describe("Exact DOI lookup. e.g. '10.1038/nature12345'."),
        language: z.string().trim().min(1).max(64).optional().describe("Filter by language. Lowercase English name: 'english', 'chinese', 'french', 'german', 'spanish', 'russian', 'japanese', 'arabic', 'italian', 'portuguese', 'korean'."),
        format: z.string().trim().min(1).max(64).optional().describe("Filter by file format. Lowercase extension: 'pdf', 'epub', 'djvu', 'mobi', 'fb2', 'azw3', 'txt', 'docx', 'lit', 'rtf'."),
        limit: z.number().int().min(1).max(50).optional().describe("Max results to return. Default 10, max 50. Use higher values for broad searches."),
      },
      _meta: oauthSecurity,
    },
    async ({ query, title, author, year_from, year_to, publisher, isbn, doi, language, format, limit }) => {
      const authError = keyValidationError(await credential.validateMembership());
      if (authError) {
        return { content: [{ type: "text", text: authError.message }], isError: true };
      }
      if (!query && !title && !author && !isbn && !doi) {
        return { content: [{ type: "text", text: "Please provide at least one search parameter: query, title, author, isbn, or doi." }], isError: true };
      }
      if (year_from !== undefined && year_to !== undefined && year_from > year_to) {
        return { content: [{ type: "text", text: "year_from must not be greater than year_to." }], isError: true };
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

  server.registerTool(
    "download",
    {
      description: `Get a fast download URL for a document by its MD5 hash (from search results). Returns a temporary download link — use it promptly.

Requires a linked Anna's Archive membership key through OAuth or the X-Annas-Secret-Key header. Get one at https://annas-archive.gl/account .

Present the URL as a clickable markdown link. To save locally: curl -L -o filename.ext '<url>'`,
      inputSchema: {
        md5: z.string().regex(MD5_PATTERN).describe("32-character hexadecimal MD5 hash from search results"),
      },
      _meta: oauthSecurity,
    },
    async ({ md5 }) => {
      const authError = keyValidationError(await credential.validateMembership());
      if (authError) {
        return { content: [{ type: "text", text: authError.message }], isError: true };
      }
      const secretKey = await credential.getPlaintextKey();
      const doc = await getByMd5(md5);
      const result = await getDownloadUrl(md5, secretKey);

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

  server.registerTool(
    "stats",
    {
      description: "Get statistics about the local Anna's Archive metadata index — total records and breakdown by source collection.",
      _meta: oauthSecurity,
    },
    async () => {
      const stats = await getStats();
      const lines = [`Total documents: ${stats.total.toLocaleString()}\n\nBy source:`];
      for (const [source, count] of Object.entries(stats.by_source)) {
        lines.push(`  ${source}: ${count.toLocaleString()}`);
      }
      return { content: [{ type: "text", text: lines.join("\n") }] };
    }
  );

  server.registerTool(
    "read",
    {
      description: `Read the text content of a document by its MD5 hash. Downloads the file via fast download, extracts text, and returns it page by page OR chapter by chapter. Supports PDF, EPUB, DJVU, MOBI, AZW3, FB2, DOCX, RTF, and plain text. Results are cached — subsequent reads are instant.

Requires a currently valid Anna's Archive membership key linked through OAuth or configured in the X-Annas-Secret-Key header for every read, including cached documents.

BEHAVIOR:
- No arguments → returns page count, chapter count (if detected), and first page preview. Use this first to understand the document.
- list_chapters=true → returns the detected table of contents (chapter number, title, page range) without content.
- chapter=N → returns the full text of chapter N (1-indexed, from the detected TOC).
- start_page only → returns 20 pages starting from that page.
- start_page + end_page → returns that exact range.
- Output capped at 50k characters. Request a smaller range or narrower chapter if truncated.

Chapter detection is heuristic (matches "Chapter N", "Part N", roman numerals, "Prologue", "Introduction", "Appendix", etc.). Books without clearly-marked headings won't have chapters detected — fall back to page ranges.

TYPICAL WORKFLOW:
1. search(title="Pedagogy", author="Freire") → find document, get MD5
2. read(md5) → get page count, chapter count, and preview
3. read(md5, list_chapters=true) → see the table of contents
4. read(md5, chapter=3) → read chapter 3
5. read(md5, start_page=11, end_page=20) → or fall back to page ranges`,
      inputSchema: {
        md5: z.string().regex(MD5_PATTERN).describe("32-character hexadecimal MD5 hash from search results"),
        start_page: z.number().int().min(1).max(1_000_000).optional().describe("First page to return (1-indexed). Omit to get document overview. Mutually exclusive with chapter."),
        end_page: z.number().int().min(1).max(1_000_000).optional().describe("Last page to return (inclusive). Omit to read 20 pages from start_page."),
        chapter: z.number().int().min(1).max(1_000_000).optional().describe("Read a specific chapter by its index (1-based, from the detected TOC). Use list_chapters first to see what's available. Mutually exclusive with start_page/end_page."),
        list_chapters: z.boolean().optional().describe("If true, returns the detected chapter list (titles + page ranges) instead of text."),
      },
      _meta: oauthSecurity,
    },
    async ({ md5, start_page, end_page, chapter, list_chapters }) => {
      const authError = keyValidationError(await credential.validateMembership());
      if (authError) {
        return { content: [{ type: "text", text: authError.message }], isError: true };
      }
      const secretKey = await credential.getPlaintextKey();
      if (end_page !== undefined && start_page === undefined) {
        return { content: [{ type: "text", text: "end_page requires start_page." }], isError: true };
      }
      if (chapter !== undefined && (start_page !== undefined || end_page !== undefined || list_chapters)) {
        return { content: [{ type: "text", text: "chapter cannot be combined with page ranges or list_chapters." }], isError: true };
      }
      if (list_chapters && (start_page !== undefined || end_page !== undefined)) {
        return { content: [{ type: "text", text: "list_chapters cannot be combined with a page range." }], isError: true };
      }
      if (start_page !== undefined && end_page !== undefined) {
        if (end_page < start_page) {
          return { content: [{ type: "text", text: "end_page must not be less than start_page." }], isError: true };
        }
        if (end_page - start_page + 1 > 100) {
          return { content: [{ type: "text", text: "A read request can include at most 100 pages." }], isError: true };
        }
      }
      const doc = await getByMd5(md5);
      const ext = doc?.extension || "pdf";

      const opts: { pageRange?: string; chapter?: number; listChapters?: boolean } = {};
      if (list_chapters) {
        opts.listChapters = true;
      } else if (chapter != null) {
        opts.chapter = chapter;
      } else if (start_page != null) {
        opts.pageRange = end_page != null
          ? `${start_page}-${end_page}`
          : `${start_page}-${Math.min(start_page + 19, 1_000_000)}`;
      }

      const result = await readDocument(md5, ext, secretKey, opts);

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
