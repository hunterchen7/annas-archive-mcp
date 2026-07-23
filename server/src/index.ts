import { createServer, type SecretLease } from "./server.js";
import { search, getByMd5, getStats } from "./db.js";
import { getDownloadUrl } from "./download.js";
import { readDocument } from "./reader.js";
import { keyValidationError, validateKey } from "./auth.js";
import { takeSecretKey } from "./requestKey.js";
import { isMd5 } from "./identifiers.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import express from "express";
import type { Request, Response, NextFunction } from "express";

const transport = process.env.TRANSPORT || "http";

if (transport === "stdio") {
  const server = createServer(process.env.ANNAS_SECRET_KEY || "");
  const stdioTransport = new StdioServerTransport();
  await server.connect(stdioTransport);
  console.error("MCP server running on stdio");
} else {
  const app = express();
  app.set("trust proxy", true);
  app.use(express.json());

  app.use((req, res, next) => {
    if (Object.prototype.hasOwnProperty.call(req.query, "aa_key")) {
      res.status(400).json({
        error: "Secret keys in URLs are not accepted. Use the X-Annas-Secret-Key header.",
      });
      return;
    }
    next();
  });

  // Rate limiting — per IP, in memory
  const RATE_WINDOW_MS = 60_000; // 1 minute
  const RATE_MAX = parseInt(process.env.RATE_LIMIT || "60", 10); // requests per window
  const hits = new Map<string, { count: number; resetAt: number }>();

  // Clean up stale entries every 5 minutes
  setInterval(() => {
    const now = Date.now();
    for (const [ip, entry] of hits) {
      if (now > entry.resetAt) hits.delete(ip);
    }
  }, 300_000);

  function getClientIp(req: Request): string {
    // CF-Connecting-IP is set by Cloudflare to the real client IP
    return (req.headers["cf-connecting-ip"] as string) ||
      req.ip ||
      req.socket.remoteAddress ||
      "unknown";
  }

  function rateLimit(req: Request, res: Response, next: NextFunction) {
    const ip = getClientIp(req);
    const now = Date.now();
    let entry = hits.get(ip);

    if (!entry || now > entry.resetAt) {
      entry = { count: 0, resetAt: now + RATE_WINDOW_MS };
      hits.set(ip, entry);
    }

    entry.count++;

    res.setHeader("X-RateLimit-Limit", RATE_MAX);
    res.setHeader("X-RateLimit-Remaining", Math.max(0, RATE_MAX - entry.count));
    res.setHeader("X-RateLimit-Reset", Math.ceil(entry.resetAt / 1000));

    if (entry.count > RATE_MAX) {
      res.status(429).json({ error: "Rate limit exceeded. Try again in a minute." });
      return;
    }

    next();
  }

  app.use("/mcp", rateLimit);

  // Streamable HTTP transport — fresh server per request (stateless)
  app.post("/mcp", async (req, res) => {
    if (Array.isArray(req.body)) {
      res.status(400).json({ error: "MCP JSON-RPC batches are not accepted." });
      return;
    }
    const secretLease: SecretLease = { value: takeSecretKey(req) };
    const server = createServer(secretLease);
    const httpTransport = new StreamableHTTPServerTransport({
      sessionIdGenerator: undefined,
    });
    let closed = false;
    const closeRequestResources = () => {
      if (closed) return;
      closed = true;
      secretLease.value = "";
      void httpTransport.close().catch(() => {});
      void server.close().catch(() => {});
    };
    res.once("close", closeRequestResources);
    try {
      await server.connect(httpTransport);
      await httpTransport.handleRequest(req, res, req.body);
    } catch (error) {
      closeRequestResources();
      throw error;
    }
  });

  // GET /mcp — required for client discovery/verification
  app.get("/mcp", (_req, res) => {
    res.json({ name: "annas-archive", version: "1.0.0", status: "ok" });
  });

  // Health check
  app.get("/health", (_req, res) => {
    res.json({ status: "ok", transport: "http" });
  });

  // --- REST API ---

  app.use("/api", rateLimit);

  // GET /api/search
  app.get("/api/search", async (req, res) => {
    const authError = keyValidationError(await validateKey(takeSecretKey(req)));
    if (authError) {
      res.status(authError.status).json({ error: authError.message });
      return;
    }
    const { query, title, author, year_from, year_to, publisher, isbn, doi, language, format, limit } = req.query;
    if (!query && !title && !author && !isbn && !doi) {
      res.status(400).json({ error: "Provide at least one of: query, title, author, isbn, doi" });
      return;
    }
    const results = await search({
      query: query as string,
      title: title as string,
      author: author as string,
      yearFrom: year_from ? parseInt(year_from as string) : undefined,
      yearTo: year_to ? parseInt(year_to as string) : undefined,
      publisher: publisher as string,
      isbn: isbn as string,
      doi: doi as string,
      language: language as string,
      format: format as string,
      limit: limit ? Math.min(parseInt(limit as string), 50) : 10,
    });
    res.json({ count: results.length, results });
  });

  // GET /api/download/:md5
  app.get("/api/download/:md5", async (req, res) => {
    if (!isMd5(req.params.md5)) {
      res.status(400).json({ error: "md5 must be exactly 32 hexadecimal characters." });
      return;
    }
    const secretKey = takeSecretKey(req);
    const authError = keyValidationError(await validateKey(secretKey));
    if (authError) {
      res.status(authError.status).json({ error: authError.message });
      return;
    }
    const result = await getDownloadUrl(req.params.md5, secretKey);
    if (result.error) {
      res.status(result.error.includes("secret key") ? 401 : 502).json({ error: result.error });
      return;
    }
    res.json({ download_url: result.downloadUrl });
  });

  // GET /api/read/:md5
  app.get("/api/read/:md5", async (req, res) => {
    if (!isMd5(req.params.md5)) {
      res.status(400).json({ error: "md5 must be exactly 32 hexadecimal characters." });
      return;
    }
    const secretKey = takeSecretKey(req);
    const authError = keyValidationError(await validateKey(secretKey));
    if (authError) {
      res.status(authError.status).json({ error: authError.message });
      return;
    }
    const doc = await getByMd5(req.params.md5);
    const ext = doc?.extension || "pdf";
    const { start_page, end_page, chapter, list_chapters } = req.query;

    const opts: { pageRange?: string; chapter?: number; listChapters?: boolean } = {};
    if (list_chapters === "true" || list_chapters === "1") {
      opts.listChapters = true;
    } else if (chapter) {
      const c = parseInt(chapter as string);
      if (Number.isFinite(c) && c >= 1) opts.chapter = c;
    } else if (start_page) {
      const sp = parseInt(start_page as string);
      const ep = end_page ? parseInt(end_page as string) : sp + 19;
      opts.pageRange = `${sp}-${ep}`;
    }

    const result = await readDocument(req.params.md5, ext, secretKey, opts);
    if (result.error) {
      res.status(result.error.includes("secret key") ? 401 : 502).json({ error: result.error });
      return;
    }
    res.json({
      document: doc ? { title: doc.title, author: doc.author, format: doc.extension } : null,
      text: result.text,
      page_count: result.pageCount,
      format: result.format,
      chapters: result.chapters,
    });
  });

  // GET /api/stats
  app.get("/api/stats", async (_req, res) => {
    const stats = await getStats();
    res.json(stats);
  });

  // GET /api/book/:md5 — metadata lookup
  app.get("/api/book/:md5", async (req, res) => {
    if (!isMd5(req.params.md5)) {
      res.status(400).json({ error: "md5 must be exactly 32 hexadecimal characters." });
      return;
    }
    const authError = keyValidationError(await validateKey(takeSecretKey(req)));
    if (authError) {
      res.status(authError.status).json({ error: authError.message });
      return;
    }
    const doc = await getByMd5(req.params.md5);
    if (!doc) {
      res.status(404).json({ error: "Not found" });
      return;
    }
    res.json(doc);
  });

  const port = parseInt(process.env.PORT || "3001", 10);
  app.listen(port, "0.0.0.0", () => {
    console.log(`MCP server listening on http://0.0.0.0:${port}/mcp`);
  });
}
