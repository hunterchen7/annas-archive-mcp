import { createServer } from "./server.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import express from "express";

const transport = process.env.TRANSPORT || "http";

if (transport === "stdio") {
  const server = createServer(process.env.ANNAS_SECRET_KEY || "");
  const stdioTransport = new StdioServerTransport();
  await server.connect(stdioTransport);
  console.error("MCP server running on stdio");
} else {
  const app = express();
  app.use(express.json());

  const AUTH_TOKEN = process.env.AUTH_TOKEN;

  // Auth middleware — supports both header and query param
  if (AUTH_TOKEN) {
    app.use("/mcp", (req, res, next) => {
      const auth = req.headers.authorization;
      const queryToken = req.query.token as string | undefined;
      if (auth === `Bearer ${AUTH_TOKEN}` || queryToken === AUTH_TOKEN) {
        next();
        return;
      }
      res.status(401).json({ error: "Unauthorized" });
    });
  }

  // Streamable HTTP transport — fresh server per request (stateless)
  app.post("/mcp", async (req, res) => {
    const secretKey =
      (req.headers["x-annas-secret-key"] as string) ||
      (req.query.aa_key as string) ||
      "";
    const server = createServer(secretKey);
    const httpTransport = new StreamableHTTPServerTransport({
      sessionIdGenerator: undefined,
    });
    await server.connect(httpTransport);
    await httpTransport.handleRequest(req, res, req.body);
  });

  // Health check
  app.get("/health", (_req, res) => {
    res.json({ status: "ok", transport: "http" });
  });

  const port = parseInt(process.env.PORT || "3001", 10);
  app.listen(port, "0.0.0.0", () => {
    console.log(`MCP server listening on http://0.0.0.0:${port}/mcp`);
  });
}
