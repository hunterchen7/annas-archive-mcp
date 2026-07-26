import assert from "node:assert/strict";
import { createHash, randomBytes } from "node:crypto";
import { after, beforeEach, describe, test } from "node:test";
import type { Response } from "express";
import pg from "pg";
import { KeyProtector, opaqueToken } from "./oauthCrypto.js";
import { PostgresOAuthProvider } from "./oauthProvider.js";

const databaseUrl = process.env.OAUTH_INTEGRATION_DATABASE_URL;
const integrationTest = databaseUrl ? test : test.skip;
const pool = databaseUrl
  ? new pg.Pool({ connectionString: databaseUrl, max: 2 })
  : undefined;

interface Flow {
  provider: PostgresOAuthProvider;
  client: Awaited<ReturnType<NonNullable<PostgresOAuthProvider["clientsStore"]["registerClient"]>>>;
  codeVerifier: string;
  authorizationCode: string;
  membershipKey: string;
}

function pkceChallenge(verifier: string): string {
  return createHash("sha256").update(verifier).digest("base64url");
}

async function linkedFlow(retention: "persistent" | "session"): Promise<Flow> {
  if (!pool) throw new Error("Integration database is not configured.");
  const provider = new PostgresOAuthProvider({
    pool,
    protector: new KeyProtector(randomBytes(32).toString("base64")),
    issuerUrl: new URL("https://mcp.example.test"),
    resourceUrl: new URL("https://mcp.example.test/mcp"),
    validateKey: async (key) => key === "membership-secret"
      ? { ok: true }
      : { ok: false, reason: "invalid" },
  });
  const registerClient = provider.clientsStore.registerClient;
  if (!registerClient) throw new Error("Dynamic registration is unavailable.");
  const client = await registerClient({
    client_id: "test-client",
    client_id_issued_at: Math.floor(Date.now() / 1000),
    client_name: "Integration test client",
    redirect_uris: ["https://client.example.test/oauth/callback"],
    token_endpoint_auth_method: "none",
    grant_types: ["authorization_code", "refresh_token"],
    response_types: ["code"],
  } as never);
  const codeVerifier = opaqueToken(48);
  let csrfToken = "";
  let portalRedirect = "";
  const response = {
    cookie(name: string, value: string) {
      assert.equal(name, "aa_oauth_csrf");
      csrfToken = value;
      return this;
    },
    redirect(status: number, location: string) {
      assert.equal(status, 302);
      portalRedirect = location;
    },
  } as unknown as Response;
  await provider.authorize(client, {
    redirectUri: client.redirect_uris[0],
    codeChallenge: pkceChallenge(codeVerifier),
    scopes: ["annas:use"],
    state: "client-state",
    resource: new URL("https://mcp.example.test/mcp"),
  }, response);
  const requestToken = new URL(portalRedirect).searchParams.get("request");
  assert.ok(requestToken);
  assert.ok(csrfToken);
  const membershipKey = "membership-secret";
  const linked = await provider.completeLink(
    requestToken,
    csrfToken,
    membershipKey,
    retention,
  );
  const callback = new URL(linked.redirectUrl);
  assert.equal(callback.origin, "https://client.example.test");
  assert.equal(callback.searchParams.get("state"), "client-state");
  const authorizationCode = callback.searchParams.get("code");
  assert.ok(authorizationCode);
  return { provider, client, codeVerifier, authorizationCode, membershipKey };
}

describe("PostgresOAuthProvider", () => {
  beforeEach(async () => {
    if (pool) await pool.query("TRUNCATE oauth_clients CASCADE");
  });

  after(async () => {
    await pool?.end();
  });

  integrationTest("links, encrypts, exchanges, rotates, and revokes on replay", async () => {
    if (!pool) return;
    const flow = await linkedFlow("persistent");
    const challenge = await flow.provider.challengeForAuthorizationCode(
      flow.client,
      flow.authorizationCode,
    );
    assert.equal(challenge, pkceChallenge(flow.codeVerifier));
    const tokens = await flow.provider.exchangeAuthorizationCode(
      flow.client,
      flow.authorizationCode,
      undefined,
      flow.client.redirect_uris[0],
      new URL("https://mcp.example.test/mcp"),
    );
    assert.ok(tokens.refresh_token);
    assert.ok(!JSON.stringify(tokens).includes(flow.membershipKey));

    const stored = await pool.query(
      `SELECT id, key_ciphertext, key_iv, key_tag, key_fingerprint
       FROM oauth_connections`,
    );
    assert.equal(stored.rowCount, 1);
    assert.ok(!JSON.stringify(stored.rows).includes(flow.membershipKey));
    const auth = await flow.provider.verifyAccessToken(tokens.access_token);
    assert.equal(auth.clientId, flow.client.client_id);
    assert.equal(
      await flow.provider.decryptConnectionKey(auth.extra?.connectionId as string),
      flow.membershipKey,
    );

    const rotated = await flow.provider.exchangeRefreshToken(
      flow.client,
      tokens.refresh_token,
    );
    assert.ok(rotated.refresh_token);
    await flow.provider.verifyAccessToken(rotated.access_token);
    await assert.rejects(
      () => flow.provider.exchangeRefreshToken(flow.client, tokens.refresh_token!),
      /reuse detected/,
    );
    await assert.rejects(
      () => flow.provider.verifyAccessToken(rotated.access_token),
      /invalid or expired/,
    );
    assert.equal(
      Number((await pool.query("SELECT count(*) FROM oauth_connections")).rows[0].count),
      0,
    );
  });

  integrationTest("issues no refresh token for a one-hour session", async () => {
    const flow = await linkedFlow("session");
    const tokens = await flow.provider.exchangeAuthorizationCode(
      flow.client,
      flow.authorizationCode,
      undefined,
      flow.client.redirect_uris[0],
    );
    assert.equal(tokens.refresh_token, undefined);
    await flow.provider.verifyAccessToken(tokens.access_token);
    if (!pool) return;
    await pool.query(
      "UPDATE oauth_connections SET expires_at = now() - interval '1 second'",
    );
    await flow.provider.cleanupExpired();
    assert.equal(
      Number((await pool.query("SELECT count(*) FROM oauth_connections")).rows[0].count),
      0,
    );
  });
});
