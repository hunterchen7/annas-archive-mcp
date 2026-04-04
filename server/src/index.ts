import { createServer } from "./server.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import express from "express";
import type { Request, Response, NextFunction } from "express";
import { createNodeHandler } from "./api.js";

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

  function getClientIp(req: Request): string {
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

  // MCP transport
  app.use("/mcp", rateLimit);

  app.post("/mcp", async (req, res) => {
    const secretKey =
      (req.headers["x-annas-secret-key"] as string) ||
      (req.query.aa_key as string) ||
      "";
    const server = createServer(secretKey);
    const httpTransport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
    await server.connect(httpTransport);
    await httpTransport.handleRequest(req, res, req.body);
  });

  app.get("/mcp", (_req, res) => {
    res.json({ name: "annas-archive", version: "1.0.0", status: "ok" });
  });

  // REST API (Hono + zod-openapi)
  app.use("/api", rateLimit);
  const apiHandler = createNodeHandler();
  app.use("/api", (req, res) => apiHandler(req, res));

  // Health
  app.get("/health", (_req, res) => {
    res.json({ status: "ok", transport: "http" });
  });

  const port = parseInt(process.env.PORT || "3001", 10);
  app.listen(port, "0.0.0.0", () => {
    console.log(`MCP server listening on http://0.0.0.0:${port}/mcp`);
  });
}
