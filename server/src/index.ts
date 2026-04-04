import { createServer } from "./server.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { serve } from "@hono/node-server";
import { Hono } from "hono";
import { apiApp } from "./api.js";

const transport = process.env.TRANSPORT || "http";

if (transport === "stdio") {
  const server = createServer(process.env.ANNAS_SECRET_KEY || "");
  const stdioTransport = new StdioServerTransport();
  await server.connect(stdioTransport);
  console.error("MCP server running on stdio");
} else {
  const app = new Hono();

  // Rate limiting — per IP, in memory
  const RATE_WINDOW_MS = 60_000;
  const RATE_MAX = parseInt(process.env.RATE_LIMIT || "60", 10);
  const hits = new Map<string, { count: number; resetAt: number }>();

  setInterval(() => {
    const now = Date.now();
    for (const [ip, entry] of hits) {
      if (now > entry.resetAt) hits.delete(ip);
    }
  }, 300_000);

  function getClientIp(c: { req: { header: (name: string) => string | undefined; raw: Request } }): string {
    return c.req.header("cf-connecting-ip") ||
      c.req.header("x-forwarded-for")?.split(",")[0]?.trim() ||
      "unknown";
  }

  // Rate limit middleware for /mcp and /api
  app.use("/mcp/*", async (c, next) => {
    const ip = getClientIp(c);
    const now = Date.now();
    let entry = hits.get(ip);
    if (!entry || now > entry.resetAt) {
      entry = { count: 0, resetAt: now + RATE_WINDOW_MS };
      hits.set(ip, entry);
    }
    entry.count++;
    c.header("X-RateLimit-Limit", String(RATE_MAX));
    c.header("X-RateLimit-Remaining", String(Math.max(0, RATE_MAX - entry.count)));
    c.header("X-RateLimit-Reset", String(Math.ceil(entry.resetAt / 1000)));
    if (entry.count > RATE_MAX) {
      return c.json({ error: "Rate limit exceeded. Try again in a minute." }, 429);
    }
    await next();
  });

  app.use("/api/*", async (c, next) => {
    const ip = getClientIp(c);
    const now = Date.now();
    let entry = hits.get(ip);
    if (!entry || now > entry.resetAt) {
      entry = { count: 0, resetAt: now + RATE_WINDOW_MS };
      hits.set(ip, entry);
    }
    entry.count++;
    c.header("X-RateLimit-Limit", String(RATE_MAX));
    c.header("X-RateLimit-Remaining", String(Math.max(0, RATE_MAX - entry.count)));
    c.header("X-RateLimit-Reset", String(Math.ceil(entry.resetAt / 1000)));
    if (entry.count > RATE_MAX) {
      return c.json({ error: "Rate limit exceeded. Try again in a minute." }, 429);
    }
    await next();
  });

  // MCP transport — needs raw Node req/res via @hono/node-server
  app.post("/mcp", async (c) => {
    const secretKey = c.req.header("x-annas-secret-key") || c.req.query("aa_key") || "";
    const server = createServer(secretKey);
    const httpTransport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
    await server.connect(httpTransport);

    // Get raw Node req/res from the node-server binding
    const nodeBindings = c.env as { incoming: import("http").IncomingMessage; outgoing: import("http").ServerResponse };
    const body = await c.req.json();
    await httpTransport.handleRequest(nodeBindings.incoming, nodeBindings.outgoing, body);
    return new Response(null);
  });

  app.get("/mcp", (c) => c.json({ name: "annas-archive", version: "1.0.0", status: "ok" }));
  app.get("/health", (c) => c.json({ status: "ok", transport: "http" }));

  // Mount typed REST API
  app.route("/api", apiApp);

  const port = parseInt(process.env.PORT || "3001", 10);
  serve({ fetch: app.fetch, port }, (info) => {
    console.log(`MCP server listening on http://0.0.0.0:${info.port}/mcp`);
  });
}
