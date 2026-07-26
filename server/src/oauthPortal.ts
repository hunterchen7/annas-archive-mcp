import express, { type Request, type Response, type Router } from "express";
import {
  InvalidGrantError,
  TemporarilyUnavailableError,
} from "@modelcontextprotocol/sdk/server/auth/errors.js";
import {
  oauthCsrfCookieName,
  type LinkRequest,
  type PostgresOAuthProvider,
} from "./oauthProvider.js";
import { isRetention } from "./oauthRetention.js";

const MAX_KEY_BYTES = 512;
const TOKEN_PATTERN = /^[A-Za-z0-9_-]{32,256}$/;

class PortalInputError extends Error {}

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

function page(
  content: string,
  title = "Link Anna's Archive",
  extraHead = "",
): string {
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>${escapeHtml(title)}</title>
  ${extraHead}
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

export function callbackPage(redirectUrl: string): string {
  const destination = new URL(redirectUrl);
  const escapedUrl = escapeHtml(redirectUrl);
  return page(`
<h1>Key linked</h1>
<p>Returning you to <strong>${escapeHtml(destination.origin)}</strong> to finish the connection.</p>
<p><a href="${escapedUrl}">Continue to your MCP client</a> if nothing happens automatically.</p>
<p class="fine">The destination receives the one-time OAuth code, not your Anna's Archive key.</p>`,
  "Returning to your MCP client",
  `<meta http-equiv="refresh" content="0;url=${escapedUrl}">`);
}

function disclosures(): string {
  return `<section class="card warning">
  <h2>Exactly when your key is plaintext</h2>
  <ol>
    <li><strong>When you link:</strong> this server receives the key over HTTPS and sends it to Anna's Archive's account endpoint to verify it. Only after validation does it encrypt the key.</li>
    <li><strong>When you download or read:</strong> the server decrypts the key in process memory for that request so it can request the file from Anna's Archive.</li>
    <li><strong>During refreshable connections:</strong> after 24 hours since the last successful validation, a token-refresh attempt decrypts and revalidates the key. Concurrent refreshes, or retries after Anna's Archive was unreachable, can cause additional decryptions and validation requests.</li>
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
    <li>The key is never placed in this portal's inbound URL, the OAuth callback URL, cookies, browser storage, application logs, or OAuth tokens.</li>
    <li><strong>Outbound URL exception:</strong> Anna's Archive's fast-download API requires the real key in the query string of an outbound HTTPS request during download/read. Anna's Archive and any egress proxy or tracer can observe it there. Operators must disable or redact outbound URL query logging.</li>
    <li>Deleting an active row does not instantly erase encrypted copies from PostgreSQL WAL, replicas, snapshots, or backups. Those copies remain until the operator's retention windows expire, and the global master key can decrypt them while it is retained.</li>
  </ul>
</section>`;
}

function linkPage(
  requestToken: string,
  clientName: string,
  redirectUri: string,
  error?: string,
): string {
  const redirectOrigin = new URL(redirectUri).origin;
  return page(`
<h1>Link your membership key</h1>
<p>A client using the unverified, self-supplied label <strong>${escapeHtml(clientName.slice(0, 120))}</strong> is asking to use this Anna's Archive MCP server. There is no account or signup for this server.</p>
<section class="card warning">
  <h2>Verify the client destination</h2>
  <p>After linking, the one-time OAuth code will be sent to:</p>
  <p><code>${escapeHtml(redirectOrigin)}</code></p>
  <p class="fine">Registered callback: <code>${escapeHtml(redirectUri.slice(0, 2_048))}</code></p>
  <p>The client label is not verified. Continue only if you recognize and trust this destination. The destination receives OAuth tokens for this MCP server, but it does not receive the Anna's Archive key itself.</p>
</section>
${error ? `<div class="card error">${escapeHtml(error)}</div>` : ""}
${disclosures()}
<form method="post" action="/oauth/link" autocomplete="off">
  <input type="hidden" name="request" value="${escapeHtml(requestToken)}">
  <label class="choice">
    <input type="checkbox" name="trust_destination" value="yes" required>
    <strong>I recognize and trust ${escapeHtml(redirectOrigin)}</strong>
    <small>I understand that the displayed client name is self-supplied and unverified.</small>
  </label>
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
    <input type="radio" name="retention" value="days_30">
    <strong>30 days</strong>
    <small>Access stops 30 days after OAuth activation. The encrypted row is deleted by the next hourly cleanup, or later if the service is not running.</small>
  </label>
  <label class="choice">
    <input type="radio" name="retention" value="days_14">
    <strong>14 days</strong>
    <small>Access stops 14 days after OAuth activation. The encrypted row is deleted by the next hourly cleanup, or later if the service is not running.</small>
  </label>
  <label class="choice">
    <input type="radio" name="retention" value="days_7">
    <strong>7 days</strong>
    <small>Access stops 7 days after OAuth activation. The encrypted row is deleted by the next hourly cleanup, or later if the service is not running.</small>
  </label>
  <label class="choice">
    <input type="radio" name="retention" value="session">
    <strong>One-hour session</strong>
    <small>Access expires after one hour and there is no refresh token. The encrypted row is deleted by the next hourly cleanup, or later if the service is not running.</small>
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
  <p><strong>Until I disconnect</strong> has no automatic expiry after the client completes the OAuth code exchange. Before that exchange, the encrypted record is provisional and expires with the five-minute authorization code. An active connection remains until the client calls OAuth revocation, the same key is relinked for that client, or an operator deletes it.</p>
  <p><strong>7, 14, or 30 days</strong> starts when the client successfully exchanges the OAuth code. The client receives refresh tokens until that deadline. At the deadline, access stops even if cleanup has not run yet; the encrypted row is deleted by the next hourly cleanup, or later if the service is not running.</p>
  <p><strong>One-hour session</strong> starts when the client successfully exchanges the OAuth code and creates no refresh token. Access expires after one hour. Its encrypted row is then deleted by the hourly cleanup task (which also runs on server startup), normally within the following hour and later if the service is not running.</p>
  <p>Legacy <code>X-Annas-Secret-Key</code> requests are not persisted: their key remains request-scoped, apart from a short-lived process-local keyed validation verdict.</p>
</section>`, "Membership-key handling");
}

function clearOauthCsrfCookie(
  req: Request,
  res: Response,
  requestToken: string,
): void {
  if (!TOKEN_PATTERN.test(requestToken)) return;
  res.clearCookie(oauthCsrfCookieName(requestToken), {
    httpOnly: true,
    secure: req.secure,
    sameSite: "lax",
    path: "/oauth",
  });
}

function unusableLinkPage(expired: boolean): string {
  const heading = expired ? "Link expired" : "Invalid linking request";
  return page(
    `<h1>${heading}</h1>
<p>This request can no longer create another connection. A previous submission may already have completed.</p>
<p>Start the connection again from your MCP client to get a new link.</p>`,
  );
}

export function portalErrorPage(
  error: unknown,
  validRequestToken: boolean,
  link?: LinkRequest,
): string | undefined {
  if (!(
    error instanceof InvalidGrantError ||
    error instanceof TemporarilyUnavailableError ||
    error instanceof PortalInputError
  )) {
    return undefined;
  }
  if (link) {
    return linkPage(
      link.requestToken,
      link.clientName,
      link.redirectUri,
      error.message,
    );
  }
  return unusableLinkPage(validRequestToken);
}

export async function resolvePortalErrorPage(
  provider: Pick<PostgresOAuthProvider, "getLinkRequest">,
  error: unknown,
  requestToken: string,
): Promise<{ page: string; link?: LinkRequest } | undefined> {
  const validRequestToken = TOKEN_PATTERN.test(requestToken);
  if (!(
    error instanceof InvalidGrantError ||
    error instanceof TemporarilyUnavailableError ||
    error instanceof PortalInputError
  )) {
    return undefined;
  }
  const link = validRequestToken
    ? await provider.getLinkRequest(requestToken)
    : undefined;
  return {
    page: portalErrorPage(error, validRequestToken, link)!,
    link,
  };
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
      res.type("html").send(linkPage(
        link.requestToken,
        link.clientName,
        link.redirectUri,
      ));
    } catch (error) {
      next(error);
    }
  });

  router.post("/link", async (req, res, next) => {
    const requestToken = typeof req.body?.request === "string" ? req.body.request : "";
    const membershipKey = typeof req.body?.key === "string" ? req.body.key : "";
    if (req.body && typeof req.body === "object") req.body.key = "[redacted]";
    const retentionValue = req.body?.retention;
    const trustsDestination = req.body?.trust_destination === "yes";
    try {
      if (
        !TOKEN_PATTERN.test(requestToken) ||
        !membershipKey ||
        Buffer.byteLength(membershipKey) > MAX_KEY_BYTES
      ) {
        throw new PortalInputError("Enter a valid linking request and membership key.");
      }
      if (!isRetention(retentionValue)) {
        throw new PortalInputError("Choose a valid connection retention option.");
      }
      if (!trustsDestination) {
        throw new PortalInputError("Confirm that you recognize and trust the callback destination.");
      }
      const cookieName = oauthCsrfCookieName(requestToken);
      const csrfToken = cookie(req, cookieName);
      const completed = await provider.completeLink(
        requestToken,
        csrfToken,
        membershipKey,
        retentionValue,
      );
      clearOauthCsrfCookie(req, res, requestToken);
      res.status(200).type("html").send(callbackPage(completed.redirectUrl));
    } catch (error) {
      let errorResolution: Awaited<ReturnType<typeof resolvePortalErrorPage>>;
      try {
        errorResolution = await resolvePortalErrorPage(provider, error, requestToken);
      } catch (lookupError) {
        next(lookupError);
        return;
      }
      if (errorResolution) {
        if (!errorResolution.link) {
          clearOauthCsrfCookie(req, res, requestToken);
        }
        res.status(400).type("html").send(errorResolution.page);
        return;
      }
      next(error);
    }
  });

  return router;
}
