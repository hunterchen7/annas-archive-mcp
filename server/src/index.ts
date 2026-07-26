import { createServer } from "./server.js";
import { search, getByMd5, getStats, pool } from "./db.js";
import { getDownloadUrl } from "./download.js";
import { readDocument } from "./reader.js";
import { keyValidationError, validateKeyLive } from "./auth.js";
import { isMd5 } from "./identifiers.js";
import { parseReadQuery, parseSearchQuery } from "./httpInput.js";
import { boundedInteger } from "./config.js";
import { resolveCredential } from "./credential.js";
import { oauthConfigFromEnv } from "./oauthConfig.js";
import { KeyProtector } from "./oauthCrypto.js";
import { PostgresOAuthProvider, oauthScope } from "./oauthProvider.js";
import { oauthPortalRouter } from "./oauthPortal.js";
import { publicClientOAuthMetadata } from "./oauthMetadata.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { mcpAuthRouter } from "@modelcontextprotocol/sdk/server/auth/router.js";
import express from "express";
import type { Request, Response, NextFunction, RequestHandler } from "express";

const transport = process.env.TRANSPORT || "http";

if (transport === "stdio") {
  const server = createServer(process.env.ANNAS_SECRET_KEY || "");
  const stdioTransport = new StdioServerTransport();
  await server.connect(stdioTransport);
  console.error("MCP server running on stdio");
} else {
  const app = express();
  const oauthConfig = oauthConfigFromEnv();
  const oauthProvider = oauthConfig
    ? new PostgresOAuthProvider({
      pool,
      protector: new KeyProtector(oauthConfig.encryptionKey),
      issuerUrl: oauthConfig.issuerUrl,
      resourceUrl: oauthConfig.resourceUrl,
      validateKey: validateKeyLive,
    })
    : undefined;
  const resourceMetadataUrl = oauthConfig
    ? new URL("/.well-known/oauth-protected-resource/mcp", oauthConfig.issuerUrl).href
    : undefined;
  app.disable("x-powered-by");
  app.disable("etag");
  app.set(
    "trust proxy",
    process.env.TRUST_PROXY || "loopback, linklocal, uniquelocal",
  );
  app.use((_req, res, next) => {
    res.setHeader("Cache-Control", "no-store");
    res.setHeader("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'");
    res.setHeader("Permissions-Policy", "camera=(), microphone=(), geolocation=()");
    res.setHeader("Referrer-Policy", "no-referrer");
    res.setHeader("Strict-Transport-Security", "max-age=31536000");
    res.setHeader("X-Content-Type-Options", "nosniff");
    res.setHeader("X-Frame-Options", "DENY");
    next();
  });
  // Rate limiting — per IP, in memory
  const RATE_WINDOW_MS = 60_000; // 1 minute
  const RATE_MAX = boundedInteger(process.env.RATE_LIMIT, 60, 1, 10_000);
  const MAX_RATE_KEYS = 100_000;
  const hits = new Map<string, { count: number; resetAt: number }>();

  // Clean up stale entries every 5 minutes
  const rateCleanupTimer = setInterval(() => {
    const now = Date.now();
    for (const [ip, entry] of hits) {
      if (now > entry.resetAt) hits.delete(ip);
    }
  }, 300_000);
  rateCleanupTimer.unref();

  function getClientIp(req: Request): string {
    return req.ip || req.socket.remoteAddress || "unknown";
  }

  function rateLimit(req: Request, res: Response, next: NextFunction) {
    const ip = getClientIp(req);
    const now = Date.now();
    let entry = hits.get(ip);

    if (!entry || now > entry.resetAt) {
      if (!entry && hits.size >= MAX_RATE_KEYS) {
        const oldest = hits.keys().next().value;
        if (oldest !== undefined) hits.delete(oldest);
      }
      entry = { count: 0, resetAt: now + RATE_WINDOW_MS };
      hits.set(ip, entry);
    }

    entry.count++;

    res.setHeader("X-RateLimit-Limit", RATE_MAX);
    res.setHeader("X-RateLimit-Remaining", Math.max(0, RATE_MAX - entry.count));
    res.setHeader("X-RateLimit-Reset", Math.ceil(entry.resetAt / 1000));

    if (entry.count > RATE_MAX) {
      res.setHeader("Retry-After", Math.ceil((entry.resetAt - now) / 1000));
      res.status(429).json({ error: "Rate limit exceeded. Try again in a minute." });
      return;
    }

    next();
  }

  type AsyncRoute = (req: Request, res: Response, next: NextFunction) => Promise<void>;
  const asyncRoute = (handler: AsyncRoute): RequestHandler =>
    (req, res, next) => {
      void handler(req, res, next).catch(next);
  };

  app.use("/mcp", rateLimit);
  app.use("/api", rateLimit);
  app.use("/health", rateLimit);
  app.use("/oauth", rateLimit);
  app.use((req, res, next) => {
    if (Object.keys(req.query).some((key) => key.toLowerCase() === "aa_key")) {
      res.status(400).json({
        error: "Secret keys in URLs are not accepted. Use the X-Annas-Secret-Key header.",
      });
      return;
    }
    next();
  });
  if (oauthProvider && oauthConfig) {
    const authRouterOptions = {
      provider: oauthProvider,
      issuerUrl: oauthConfig.issuerUrl,
      resourceServerUrl: oauthConfig.resourceUrl,
      serviceDocumentationUrl: new URL("/oauth", oauthConfig.issuerUrl),
      scopesSupported: [oauthScope],
      resourceName: "Anna's Archive MCP",
    };
    const publicMetadata = publicClientOAuthMetadata(authRouterOptions);
    app.options("/.well-known/oauth-authorization-server", (_req, res) => {
      res.setHeader("Access-Control-Allow-Origin", "*");
      res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
      res.status(204).end();
    });
    app.get("/.well-known/oauth-authorization-server", (_req, res) => {
      res.setHeader("Access-Control-Allow-Origin", "*");
      res.json(publicMetadata);
    });
    app.use(mcpAuthRouter(authRouterOptions));
    app.use("/oauth", oauthPortalRouter(oauthProvider));
    void oauthProvider.cleanupExpired().catch((error) => {
      console.error(`OAuth cleanup failed during startup: ${error}`);
    });
    const oauthCleanupTimer = setInterval(() => {
      void oauthProvider.cleanupExpired().catch((error) => {
        console.error(`OAuth cleanup failed: ${error}`);
      });
    }, 60 * 60 * 1000);
    oauthCleanupTimer.unref();
  }
  app.use("/mcp", express.json({ limit: "1mb", strict: true }));
  app.use("/mcp", (_req, res, next) => {
    const setHeader = res.setHeader;
    res.setHeader = function (name, value) {
      return setHeader.call(
        this,
        name,
        name.toLowerCase() === "cache-control" ? "no-store" : value,
      );
    };
    next();
  });

  // Streamable HTTP transport — fresh server per request (stateless)
  app.post("/mcp", asyncRoute(async (req, res) => {
    if (Array.isArray(req.body)) {
      res.status(400).json({ error: "MCP JSON-RPC batches are not accepted." });
      return;
    }
    const resolved = await resolveCredential(req, oauthProvider);
    if (oauthProvider && !resolved.present) {
      if (resourceMetadataUrl) {
        res.setHeader("WWW-Authenticate", `Bearer resource_metadata="${resourceMetadataUrl}"`);
      }
      res.status(401).json({ error: "Authentication is required." });
      return;
    }
    const server = createServer(resolved.credential);
    const httpTransport = new StreamableHTTPServerTransport({
      sessionIdGenerator: undefined,
    });
    let closed = false;
    const closeRequestResources = () => {
      if (closed) return;
      closed = true;
      resolved.credential.clear();
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
  }));

  // GET /mcp — required for client discovery/verification
  app.get("/mcp", asyncRoute(async (req, res) => {
    const resolved = await resolveCredential(req, oauthProvider);
    try {
      if (oauthProvider && !resolved.present) {
        if (resourceMetadataUrl) {
          res.setHeader("WWW-Authenticate", `Bearer resource_metadata="${resourceMetadataUrl}"`);
        }
        res.status(401).json({ error: "Authentication is required." });
        return;
      }
      res.json({ name: "annas-archive", version: "1.0.0", status: "ok" });
    } finally {
      resolved.credential.clear();
    }
  }));

  // Health check
  let databaseHealth: { ok: boolean; expiresAt: number } | undefined;
  let pendingDatabaseHealth: Promise<boolean> | undefined;
  async function isDatabaseReady(): Promise<boolean> {
    const now = Date.now();
    if (databaseHealth && now < databaseHealth.expiresAt) return databaseHealth.ok;
    if (pendingDatabaseHealth) return pendingDatabaseHealth;

    const healthQuery = oauthProvider
      ? `SELECT
           to_regclass('public.oauth_clients') IS NOT NULL
           AND to_regclass('public.oauth_authorization_requests') IS NOT NULL
           AND to_regclass('public.oauth_connections') IS NOT NULL
           AND to_regclass('public.oauth_authorization_codes') IS NOT NULL
           AND to_regclass('public.oauth_access_tokens') IS NOT NULL
           AND to_regclass('public.oauth_refresh_tokens') IS NOT NULL
           AND to_regclass('public.idx_oauth_connections_client_key') IS NOT NULL
           AND EXISTS (
             SELECT 1
             FROM oauth_schema_metadata
             WHERE singleton = TRUE AND version >= 2
           )
           AS oauth_schema_ready`
      : "SELECT true AS oauth_schema_ready";
    const check = pool.query(healthQuery)
      .then((result) => result.rows[0]?.oauth_schema_ready === true)
      .catch(() => false)
      .then((ok) => {
        databaseHealth = {
          ok,
          expiresAt: Date.now() + (ok ? 5_000 : 2_000),
        };
        return ok;
      })
      .finally(() => {
        pendingDatabaseHealth = undefined;
      });
    pendingDatabaseHealth = check;
    return check;
  }

  app.get("/health", asyncRoute(async (_req, res) => {
    const databaseReady = await isDatabaseReady();
    res.status(databaseReady ? 200 : 503).json({
      status: databaseReady ? "ok" : "degraded",
      transport: "http",
      database: databaseReady ? "ok" : "unavailable",
    });
  }));

  // --- REST API ---

  // GET /api/search
  app.get("/api/search", asyncRoute(async (req, res) => {
    const resolved = await resolveCredential(req, oauthProvider);
    try {
      const authError = keyValidationError(await resolved.credential.validateMembership());
      if (authError) {
        res.status(authError.status).json({ error: authError.message });
        return;
      }
      const parsed = parseSearchQuery(req.query);
      if (!parsed.ok) {
        res.status(400).json({ error: parsed.error });
        return;
      }
      const input = parsed.value;
      if (!input.query && !input.title && !input.author && !input.isbn && !input.doi) {
        res.status(400).json({ error: "Provide at least one of: query, title, author, isbn, doi" });
        return;
      }
      const results = await search(input);
      res.json({ count: results.length, results });
    } finally {
      resolved.credential.clear();
    }
  }));

  // GET /api/download/:md5
  app.get("/api/download/:md5", asyncRoute(async (req, res) => {
    if (!isMd5(req.params.md5)) {
      res.status(400).json({ error: "md5 must be exactly 32 hexadecimal characters." });
      return;
    }
    const resolved = await resolveCredential(req, oauthProvider);
    try {
      const authError = keyValidationError(await resolved.credential.validateMembership());
      if (authError) {
        res.status(authError.status).json({ error: authError.message });
        return;
      }
      const secretKey = await resolved.credential.getPlaintextKey();
      const result = await getDownloadUrl(req.params.md5, secretKey);
      if (result.error) {
        if (result.errorCode === "invalid_membership_key") {
          await resolved.credential.invalidate();
        }
        res.status(result.errorCode === "invalid_membership_key" ? 401 : 502).json({
          error: result.error,
        });
        return;
      }
      res.json({ download_url: result.downloadUrl });
    } finally {
      resolved.credential.clear();
    }
  }));

  // GET /api/read/:md5
  app.get("/api/read/:md5", asyncRoute(async (req, res) => {
    if (!isMd5(req.params.md5)) {
      res.status(400).json({ error: "md5 must be exactly 32 hexadecimal characters." });
      return;
    }
    const resolved = await resolveCredential(req, oauthProvider);
    try {
      const authError = keyValidationError(await resolved.credential.validateMembership());
      if (authError) {
        res.status(authError.status).json({ error: authError.message });
        return;
      }
      const parsed = parseReadQuery(req.query);
      if (!parsed.ok) {
        res.status(400).json({ error: parsed.error });
        return;
      }
      const secretKey = await resolved.credential.getPlaintextKey();
      const doc = await getByMd5(req.params.md5);
      const ext = doc?.extension || "pdf";

      const result = await readDocument(req.params.md5, ext, secretKey, parsed.value);
      if (result.error) {
        if (result.errorCode === "invalid_membership_key") {
          await resolved.credential.invalidate();
        }
        res.status(result.errorCode === "invalid_membership_key" ? 401 : 502).json({
          error: result.error,
        });
        return;
      }
      res.json({
        document: doc ? { title: doc.title, author: doc.author, format: doc.extension } : null,
        text: result.text,
        page_count: result.pageCount,
        format: result.format,
        chapters: result.chapters,
      });
    } finally {
      resolved.credential.clear();
    }
  }));

  // GET /api/stats
  app.get("/api/stats", asyncRoute(async (_req, res) => {
    const stats = await getStats();
    res.json(stats);
  }));

  // GET /api/book/:md5 — metadata lookup
  app.get("/api/book/:md5", asyncRoute(async (req, res) => {
    if (!isMd5(req.params.md5)) {
      res.status(400).json({ error: "md5 must be exactly 32 hexadecimal characters." });
      return;
    }
    const resolved = await resolveCredential(req, oauthProvider);
    try {
      const authError = keyValidationError(await resolved.credential.validateMembership());
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
    } finally {
      resolved.credential.clear();
    }
  }));

  app.use((
    error: unknown,
    _req: Request,
    res: Response,
    _next: NextFunction,
  ) => {
    if (res.headersSent) {
      res.end();
      return;
    }
    const candidateStatus = typeof error === "object" && error !== null &&
        "status" in error
      ? (error as { status?: unknown }).status
      : undefined;
    const status = candidateStatus === 400 ||
        candidateStatus === 401 ||
        candidateStatus === 413 ||
        candidateStatus === 415
      ? candidateStatus
      : 500;
    if (status === 500) {
      const message = error instanceof Error ? error.message : "Unknown request failure";
      console.error(`Unhandled request error: ${message}`);
    }
    if (status === 401 && resourceMetadataUrl) {
      res.setHeader("WWW-Authenticate", `Bearer resource_metadata="${resourceMetadataUrl}"`);
    }
    res.status(status).json({
      error: status === 401
        ? "Authentication is invalid or expired."
        : status === 400
        ? "Invalid JSON request body."
        : status === 413
        ? "Request body is too large."
        : status === 415
        ? "Unsupported request body encoding."
        : "Internal server error.",
    });
  });

  const port = boundedInteger(process.env.PORT, 3001, 1, 65_535);
  const httpServer = app.listen(port, "0.0.0.0", () => {
    console.log(`MCP server listening on http://0.0.0.0:${port}/mcp`);
  });
  httpServer.headersTimeout = 10_000;
  httpServer.requestTimeout = 180_000;
  httpServer.keepAliveTimeout = 5_000;
  httpServer.maxHeadersCount = 100;
}
