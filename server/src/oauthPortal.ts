import express, { type Request, type Response, type Router } from "express";
import { OAuthError } from "@modelcontextprotocol/sdk/server/auth/errors.js";
import type { PostgresOAuthProvider } from "./oauthProvider.js";

const MAX_KEY_BYTES = 512;
const TOKEN_PATTERN = /^[A-Za-z0-9_-]{32,256}$/;

function escapeHtml(value: string): string {
  return value.replace(/[&<>"']/g, (character) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  })[character] || character);
}

function cookie(req: Request, name: string): string {
  const header = req.headers.cookie;
  if (!header) return "";
  for (const pair of header.split(";")) {
    const separator = pair.indexOf("=");
    if (separator < 0) continue;
    if (pair.slice(0, separator).trim() === name) {
      return decodeURIComponent(pair.slice(separator + 1).trim());
    }
  }
  return "";
}

function page(content: string, title = "Link Anna's Archive"): string {
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>${escapeHtml(title)}</title>
  <style>
    :root { color-scheme: light dark; font-family: ui-sans-serif, system-ui, sans-serif; }
    body { margin: 0; background: #101114; color: #f5f4ef; }
    main { max-width: 700px; margin: 0 auto; padding: 48px 22px 72px; }
    h1 { font-size: 2rem; letter-spacing: -.03em; margin: 0 0 12px; }
    h2 { font-size: 1.08rem; margin: 28px 0 8px; }
    p, li { color: #c9c7be; line-height: 1.55; }
    a { color: #9bc2ff; }
    .card { background: #191b20; border: 1px solid #30333b; border-radius: 14px; padding: 20px; margin: 22px 0; }
    .warning { border-color: #80672f; background: #211d14; }
    .error { border-color: #a84646; background: #291818; color: #ffd3d3; }
    label { display: block; margin: 14px 0 7px; font-weight: 650; }
    input[type=password] { box-sizing: border-box; width: 100%; padding: 12px; border: 1px solid #4a4e59; border-radius: 8px; background: #0d0e11; color: inherit; font: inherit; }
    .choice { display: grid; grid-template-columns: auto 1fr; gap: 3px 10px; padding: 12px; border: 1px solid #383b44; border-radius: 9px; margin: 10px 0; }
    .choice input { grid-row: 1 / 3; margin-top: 4px; }
    .choice small { color: #aaa89f; line-height: 1.4; }
    button { margin-top: 18px; width: 100%; padding: 12px; border: 0; border-radius: 9px; background: #f1efe7; color: #111; font: inherit; font-weight: 750; cursor: pointer; }
    .fine { font-size: .9rem; color: #aaa89f; }
    code { background: #292c33; padding: 2px 5px; border-radius: 4px; }
  </style>
</head>
<body><main>${content}</main></body>
</html>`;
}

function disclosures(): string {
  return `<section class="card warning">
  <h2>Exactly when your key is plaintext</h2>
  <ol>
    <li><strong>When you link:</strong> this server receives the key over HTTPS and sends it to Anna's Archive's account endpoint to verify it. Only after validation does it encrypt the key.</li>
    <li><strong>When you download or read:</strong> the server decrypts the key in process memory for that request so it can request the file from Anna's Archive.</li>
    <li><strong>At most once every 24 hours during persistent-token refresh:</strong> the server decrypts and revalidates the key. Token refresh is automatic in compatible clients.</li>
  </ol>
  <p>Normal metadata searches use the prior validation and <strong>do not decrypt the key</strong>.</p>
</section>
<section class="card">
  <h2>What encryption does—and does not—guarantee</h2>
  <ul>
    <li>The database stores AES-256-GCM ciphertext, a random nonce, an authentication tag, and a keyed fingerprint—not the plaintext key.</li>
    <li>The separate server encryption secret is required to decrypt a database copy.</li>
    <li>Encryption at rest does <strong>not</strong> prevent the running application or an operator who controls both the application and its encryption secret from accessing the key.</li>
    <li>Plaintext can remain in ordinary JavaScript process memory until the request finishes. JavaScript strings cannot be reliably zeroed.</li>
    <li>The application does not intentionally put the key in URLs, cookies, browser storage, application logs, or OAuth tokens. TLS proxies and infrastructure must also be configured not to log request bodies or secret headers.</li>
  </ul>
</section>`;
}

function linkPage(
  requestToken: string,
  clientName: string,
  error?: string,
): string {
  return page(`
<h1>Link your membership key</h1>
<p><strong>${escapeHtml(clientName.slice(0, 120))}</strong> is asking to use this Anna's Archive MCP server. There is no account or signup for this server.</p>
${error ? `<div class="card error">${escapeHtml(error)}</div>` : ""}
${disclosures()}
<form method="post" action="/oauth/link" autocomplete="off">
  <input type="hidden" name="request" value="${escapeHtml(requestToken)}">
  <label for="key">Anna's Archive membership secret key</label>
  <input id="key" name="key" type="password" maxlength="512" required autocomplete="off" spellcheck="false">
  <p class="fine">This is your Anna's Archive key, not an OAuth client secret. It is never placed in the callback URL.</p>

  <label>How long should this connection last?</label>
  <label class="choice">
    <input type="radio" name="retention" value="persistent" checked>
    <strong>Until I disconnect</strong>
    <small>Recommended. The encrypted key has no automatic expiry. Access tokens are short-lived and refresh automatically; revoking the connection deletes the encrypted key.</small>
  </label>
  <label class="choice">
    <input type="radio" name="retention" value="session">
    <strong>One-hour session</strong>
    <small>The key is still encrypted at rest, but the connection has no refresh token and is automatically deleted after one hour.</small>
  </label>
  <button type="submit">Validate key and link</button>
</form>
<p class="fine">This independent connector is not operated or endorsed by Anna's Archive. By continuing, you authorize the uses described above.</p>`);
}

function privacyPage(): string {
  return page(`
<h1>Membership-key handling</h1>
<p>This page describes the OAuth linking mode for the Anna's Archive MCP server.</p>
${disclosures()}
<section class="card">
  <h2>Retention and deletion</h2>
  <p><strong>Until I disconnect</strong> has no automatic expiry. The encrypted key remains until the client calls OAuth revocation, the same key is relinked for that client, or an operator deletes the connection.</p>
  <p><strong>One-hour session</strong> creates no refresh token. Its encrypted key is deleted after expiry by periodic cleanup, or lazily when an expired connection is next encountered.</p>
  <p>Legacy <code>X-Annas-Secret-Key</code> requests are not persisted: their key remains request-scoped, apart from a short-lived process-local keyed validation verdict.</p>
</section>`, "Membership-key handling");
}

function portalHeaders(_req: Request, res: Response, next: () => void): void {
  res.setHeader(
    "Content-Security-Policy",
    "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'",
  );
  next();
}

export function oauthPortalRouter(provider: PostgresOAuthProvider): Router {
  const router = express.Router();
  router.use(portalHeaders);
  router.use(express.urlencoded({ extended: false, limit: "4kb", parameterLimit: 10 }));

  router.get("/", (_req, res) => {
    res.type("html").send(privacyPage());
  });

  router.get("/link", async (req, res, next) => {
    try {
      const requestToken = typeof req.query.request === "string" ? req.query.request : "";
      if (!TOKEN_PATTERN.test(requestToken)) {
        res.status(400).type("html").send(page(
          "<h1>Invalid linking request</h1><p>Start the connection again from your MCP client.</p>",
        ));
        return;
      }
      const link = await provider.getLinkRequest(requestToken);
      if (!link) {
        res.status(400).type("html").send(page(
          "<h1>Link expired</h1><p>Start the connection again from your MCP client.</p>",
        ));
        return;
      }
      res.type("html").send(linkPage(link.requestToken, link.clientName));
    } catch (error) {
      next(error);
    }
  });

  router.post("/link", async (req, res, next) => {
    const requestToken = typeof req.body?.request === "string" ? req.body.request : "";
    const membershipKey = typeof req.body?.key === "string" ? req.body.key : "";
    if (req.body && typeof req.body === "object") req.body.key = "[redacted]";
    const retention = req.body?.retention === "session" ? "session" : "persistent";
    try {
      if (
        !TOKEN_PATTERN.test(requestToken) ||
        !membershipKey ||
        Buffer.byteLength(membershipKey) > MAX_KEY_BYTES
      ) {
        throw new Error("Enter a valid linking request and membership key.");
      }
      const csrfToken = cookie(req, "aa_oauth_csrf");
      const completed = await provider.completeLink(
        requestToken,
        csrfToken,
        membershipKey,
        retention,
      );
      res.clearCookie("aa_oauth_csrf", {
        httpOnly: true,
        secure: req.secure,
        sameSite: "lax",
        path: "/oauth",
      });
      res.redirect(302, completed.redirectUrl);
    } catch (error) {
      const link = TOKEN_PATTERN.test(requestToken)
        ? await provider.getLinkRequest(requestToken).catch(() => undefined)
        : undefined;
      if (link && (error instanceof OAuthError || error instanceof Error)) {
        res.status(400).type("html").send(linkPage(
          link.requestToken,
          link.clientName,
          error.message,
        ));
        return;
      }
      next(error);
    }
  });

  return router;
}

