import { OpenAPIHono, createRoute, z } from "@hono/zod-openapi";
import { search, getByMd5, getStats } from "./db.js";
import { getDownloadUrl } from "./download.js";
import { readDocument } from "./reader.js";
import { scrapeSearch } from "./scrape.js";

const DocumentSchema = z.object({
  md5: z.string(),
  title: z.string().nullable(),
  author: z.string().nullable(),
  publisher: z.string().nullable(),
  language: z.string().nullable(),
  year: z.number().nullable(),
  extension: z.string().nullable(),
  filesize: z.number().nullable(),
  source: z.string(),
  doi: z.string().nullable(),
  isbn: z.string().nullable(),
  pages: z.string().nullable(),
  series: z.string().nullable(),
  description: z.string().nullable(),
}).openapi("Document");

const ErrorSchema = z.object({
  error: z.string(),
}).openapi("Error");

const app = new OpenAPIHono();

// --- Search ---

const searchRoute = createRoute({
  method: "get",
  path: "/search",
  operationId: "search",
  summary: "Search documents",
  description: "Full-text search across ~72M books, papers, and documents. Supports granular field-level search, year ranges, and format/language filters.",
  request: {
    query: z.object({
      query: z.string().optional().openapi({ description: "General full-text search across title, author, and publisher.", example: "machine learning" }),
      title: z.string().optional().openapi({ description: "Search within titles only.", example: "Parallax View" }),
      author: z.string().optional().openapi({ description: "Search within authors only.", example: "Zizek" }),
      year_from: z.coerce.number().int().optional().openapi({ description: "Minimum publication year (inclusive).", example: 2020 }),
      year_to: z.coerce.number().int().optional().openapi({ description: "Maximum publication year (inclusive).", example: 2024 }),
      publisher: z.string().optional().openapi({ description: "Search within publishers only." }),
      isbn: z.string().optional().openapi({ description: "Exact ISBN lookup (10 or 13 digits).", example: "9780140447934" }),
      doi: z.string().optional().openapi({ description: "Exact DOI lookup.", example: "10.1038/nature12345" }),
      language: z.string().optional().openapi({ description: "Filter by language.", example: "english" }),
      format: z.string().optional().openapi({ description: "Filter by file format.", example: "pdf" }),
      limit: z.coerce.number().int().min(1).max(50).optional().default(10).openapi({ description: "Max results to return.", example: 10 }),
    }),
  },
  responses: {
    200: {
      description: "Search results",
      content: { "application/json": { schema: z.object({ count: z.number(), results: z.array(DocumentSchema) }) } },
    },
    400: {
      description: "Missing required search parameter",
      content: { "application/json": { schema: ErrorSchema } },
    },
  },
});

app.openapi(searchRoute, async (c) => {
  const { query, title, author, year_from, year_to, publisher, isbn, doi, language, format, limit } = c.req.valid("query");
  if (!query && !title && !author && !isbn && !doi) {
    return c.json({ error: "Provide at least one of: query, title, author, isbn, doi" }, 400);
  }
  const results = await search({
    query, title, author,
    yearFrom: year_from, yearTo: year_to,
    publisher, isbn, doi, language, format, limit,
  });
  return c.json({ count: results.length, results });
});

// --- Download ---

const downloadRoute = createRoute({
  method: "get",
  path: "/download/{md5}",
  operationId: "download",
  summary: "Get download URL",
  description: "Get a temporary fast download URL for a document. Requires an Anna's Archive membership secret key via X-Annas-Secret-Key header or aa_key query param.",
  request: {
    params: z.object({ md5: z.string().length(32).openapi({ description: "MD5 hash of the document.", example: "9d47090bf23ccee3d68dee8e9a1329bb" }) }),
    query: z.object({ aa_key: z.string().optional().openapi({ description: "Secret key (alternative to header)." }) }),
  },
  responses: {
    200: {
      description: "Download URL",
      content: { "application/json": { schema: z.object({ download_url: z.string() }) } },
    },
    401: { description: "Missing or invalid secret key", content: { "application/json": { schema: ErrorSchema } } },
    502: { description: "Upstream error", content: { "application/json": { schema: ErrorSchema } } },
  },
});

app.openapi(downloadRoute, async (c) => {
  const { md5 } = c.req.valid("param");
  const secretKey = c.req.header("x-annas-secret-key") || c.req.valid("query").aa_key || "";
  const result = await getDownloadUrl(md5, secretKey);
  if (result.error) {
    const status = result.error.includes("secret key") ? 401 : 502;
    return c.json({ error: result.error }, status);
  }
  return c.json({ download_url: result.downloadUrl! });
});

// --- Read ---

const readRoute = createRoute({
  method: "get",
  path: "/read/{md5}",
  operationId: "read",
  summary: "Extract text from document",
  description: "Download a document and extract its text content. Supports PDF, EPUB, DJVU, MOBI, and more. Results are cached. Without page params, returns page count and first page preview.",
  request: {
    params: z.object({ md5: z.string().length(32).openapi({ description: "MD5 hash of the document." }) }),
    query: z.object({
      start_page: z.coerce.number().int().min(1).optional().openapi({ description: "First page to return (1-indexed)." }),
      end_page: z.coerce.number().int().min(1).optional().openapi({ description: "Last page to return (inclusive)." }),
      aa_key: z.string().optional().openapi({ description: "Secret key (alternative to header)." }),
    }),
  },
  responses: {
    200: {
      description: "Extracted text",
      content: {
        "application/json": {
          schema: z.object({
            document: z.object({ title: z.string().nullable(), author: z.string().nullable(), format: z.string().nullable() }).nullable(),
            text: z.string(),
          }),
        },
      },
    },
    401: { description: "Missing or invalid secret key", content: { "application/json": { schema: ErrorSchema } } },
    502: { description: "Upstream error", content: { "application/json": { schema: ErrorSchema } } },
  },
});

app.openapi(readRoute, async (c) => {
  const { md5 } = c.req.valid("param");
  const { start_page, end_page, aa_key } = c.req.valid("query");
  const secretKey = c.req.header("x-annas-secret-key") || aa_key || "";
  const doc = await getByMd5(md5);
  const ext = doc?.extension || "pdf";

  let pageRange: string | undefined;
  if (start_page) {
    const ep = end_page ?? start_page + 19;
    pageRange = `${start_page}-${ep}`;
  }

  const result = await readDocument(md5, ext, secretKey, pageRange);
  if (result.error) {
    const status = result.error.includes("secret key") ? 401 : 502;
    return c.json({ error: result.error }, status);
  }
  return c.json({
    document: doc ? { title: doc.title, author: doc.author, format: doc.extension } : null,
    text: result.text || "",
  });
});

// --- Web Search (scrape) ---

const ScrapeResultSchema = z.object({
  md5: z.string(),
  title: z.string(),
  author: z.string().nullable(),
  publisher: z.string().nullable(),
  year: z.number().nullable(),
  language: z.string().nullable(),
  extension: z.string().nullable(),
  filesize: z.string().nullable(),
  description: z.string().nullable(),
  category: z.string().nullable(),
  sources: z.string().nullable(),
}).openapi("ScrapeResult");

const webSearchRoute = createRoute({
  method: "get",
  path: "/web-search",
  operationId: "webSearch",
  summary: "Search Anna's Archive website",
  description: "Search the live Anna's Archive website via scraping. May find documents not in the local index. Requires an Anna's Archive membership secret key via X-Annas-Secret-Key header or aa_key query param.",
  request: {
    query: z.object({
      query: z.string().openapi({ description: "Search query.", example: "machine learning transformers" }),
      ext: z.string().optional().openapi({ description: "Filter by file format.", example: "pdf" }),
      lang: z.string().optional().openapi({ description: "Filter by language code.", example: "en" }),
      sort: z.string().optional().openapi({ description: "Sort: most_relevant, newest, oldest, largest, smallest." }),
      aa_key: z.string().optional().openapi({ description: "Secret key (alternative to header)." }),
    }),
  },
  responses: {
    200: {
      description: "Search results from Anna's Archive",
      content: { "application/json": { schema: z.object({ count: z.number(), results: z.array(ScrapeResultSchema) }) } },
    },
    401: { description: "Missing secret key", content: { "application/json": { schema: ErrorSchema } } },
    502: { description: "Scraping error", content: { "application/json": { schema: ErrorSchema } } },
  },
});

app.openapi(webSearchRoute, async (c) => {
  const { query, ext, lang, sort, aa_key } = c.req.valid("query");
  const secretKey = c.req.header("x-annas-secret-key") || aa_key || "";
  if (!secretKey) {
    return c.json({ error: "Secret key required for web search. Provide via X-Annas-Secret-Key header or aa_key query param." }, 401);
  }
  const { results, error } = await scrapeSearch({ query, extension: ext, language: lang, sort });
  if (error) {
    return c.json({ error }, 502);
  }
  return c.json({ count: results.length, results });
});

// --- Stats ---

const statsRoute = createRoute({
  method: "get",
  path: "/stats",
  operationId: "getStats",
  summary: "Index statistics",
  description: "Get total record count and breakdown by source collection.",
  responses: {
    200: {
      description: "Statistics",
      content: {
        "application/json": {
          schema: z.object({
            total: z.number(),
            by_source: z.record(z.string(), z.number()),
          }),
        },
      },
    },
  },
});

app.openapi(statsRoute, async (c) => {
  const stats = await getStats();
  return c.json(stats);
});

// --- OpenAPI doc ---

app.doc("/openapi.json", {
  openapi: "3.1.0",
  info: {
    title: "Anna's Archive Search API",
    version: "1.0.0",
    description: "Search ~72M books, papers, and documents from a local Anna's Archive metadata index. Download files and extract text with an Anna's Archive membership secret key.",
    license: { name: "MIT" },
  },
  servers: [
    { url: "https://aa-mcp.hunterchen.ca/api", description: "Production" },
    { url: "http://localhost:3001/api", description: "Local" },
  ],
});

import { getRequestListener } from "@hono/node-server";

export function createNodeHandler() {
  return getRequestListener(app.fetch);
}

export { app as apiApp };
