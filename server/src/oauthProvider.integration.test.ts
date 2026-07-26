import assert from "node:assert/strict";
import { createHash, randomBytes } from "node:crypto";
import { after, beforeEach, describe, test } from "node:test";
import type { Response } from "express";
import pg from "pg";
import { KeyProtector, opaqueToken } from "./oauthCrypto.js";
import { PostgresOAuthProvider } from "./oauthProvider.js";
import type { Retention } from "./oauthRetention.js";

const databaseUrl = process.env.OAUTH_INTEGRATION_DATABASE_URL;
const integrationTest = databaseUrl ? test : test.skip;
const pool = databaseUrl
  ? new pg.Pool({ connectionString: databaseUrl, max: 2 })
  : undefined;
const testMasterKey = randomBytes(32).toString("base64");

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

async function linkedFlow(
  retention: Retention,
  now?: () => Date,
): Promise<Flow> {
  if (!pool) throw new Error("Integration database is not configured.");
  const provider = new PostgresOAuthProvider({
    pool,
    protector: new KeyProtector(testMasterKey),
    issuerUrl: new URL("https://mcp.example.test"),
    resourceUrl: new URL("https://mcp.example.test/mcp"),
    validateKey: async (key) => key === "membership-secret"
      ? { ok: true }
      : { ok: false, reason: "invalid" },
    now,
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
  let csrfCookieName = "";
  let portalRedirect = "";
  const response = {
    cookie(name: string, value: string) {
      assert.match(name, /^aa_oauth_csrf_[A-Za-z0-9_-]{16}$/);
      csrfCookieName = name;
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
  assert.ok(csrfCookieName);
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
      `SELECT id, key_ciphertext, key_iv, key_tag, key_fingerprint, expires_at
       FROM oauth_connections`,
    );
    assert.equal(stored.rowCount, 1);
    assert.equal(stored.rows[0].expires_at, null);
    assert.ok(!JSON.stringify(stored.rows).includes(flow.membershipKey));
    const auth = await flow.provider.verifyAccessToken(tokens.access_token);
    assert.equal(auth.clientId, flow.client.client_id);
    assert.equal(
      await flow.provider.decryptConnectionKey(auth.extra?.connectionId as string),
      flow.membershipKey,
    );
    await pool.query(
      "UPDATE oauth_access_tokens SET resource = 'https://wrong.example.test/mcp'",
    );
    await assert.rejects(
      () => flow.provider.verifyAccessToken(tokens.access_token),
      /invalid or expired/,
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

  integrationTest("expires refreshable connections after 7, 14, or 30 days", async () => {
    if (!pool) return;
    for (const [retention, expectedDays] of [
      ["days_7", 7],
      ["days_14", 14],
      ["days_30", 30],
    ] as const) {
      await pool.query("TRUNCATE oauth_clients CASCADE");
      const flow = await linkedFlow(retention);
      const activatedBefore = Date.now();
      const tokens = await flow.provider.exchangeAuthorizationCode(
        flow.client,
        flow.authorizationCode,
        undefined,
        flow.client.redirect_uris[0],
      );
      const activatedAfter = Date.now();
      assert.ok(tokens.refresh_token);

      const stored: pg.QueryResult<{ retention: string; expires_at: Date }> =
        await pool.query(
          "SELECT retention, expires_at FROM oauth_connections",
        );
      assert.equal(stored.rows[0].retention, retention);
      const expiresAt = stored.rows[0].expires_at;
      const expectedTtl = expectedDays * 24 * 60 * 60 * 1000;
      assert.ok(expiresAt.getTime() >= activatedBefore + expectedTtl);
      assert.ok(expiresAt.getTime() <= activatedAfter + expectedTtl);

      await pool.query(
        "UPDATE oauth_connections SET expires_at = now() + interval '30 minutes'",
      );
      const rotated = await flow.provider.exchangeRefreshToken(
        flow.client,
        tokens.refresh_token,
      );
      assert.ok(rotated.refresh_token);
      const expiresIn = rotated.expires_in;
      assert.ok(expiresIn);
      assert.ok(expiresIn > 0);
      assert.ok(expiresIn <= 30 * 60);

      await pool.query(
        "UPDATE oauth_connections SET expires_at = now() - interval '1 second'",
      );
      await assert.rejects(
        () => flow.provider.exchangeRefreshToken(flow.client, rotated.refresh_token!),
        /expired/,
      );
      await flow.provider.cleanupExpired();
      assert.equal(
        Number((await pool.query("SELECT count(*) FROM oauth_connections")).rows[0].count),
        0,
      );
    }
  });

  integrationTest("rolls back refresh when less than one second remains", async () => {
    if (!pool) return;
    let currentTime = new Date("2026-07-26T18:00:00.000Z");
    const flow = await linkedFlow("days_7", () => currentTime);
    const tokens = await flow.provider.exchangeAuthorizationCode(
      flow.client,
      flow.authorizationCode,
      undefined,
      flow.client.redirect_uris[0],
    );
    assert.ok(tokens.refresh_token);
    await pool.query(
      "UPDATE oauth_connections SET expires_at = $1",
      [new Date(currentTime.getTime() + 500)],
    );

    await assert.rejects(
      () => flow.provider.exchangeRefreshToken(flow.client, tokens.refresh_token!),
      /expired/,
    );
    assert.equal(
      Number((await pool.query("SELECT count(*) FROM oauth_connections")).rows[0].count),
      1,
    );
    assert.equal(
      Number((await pool.query(
        "SELECT count(*) FROM oauth_refresh_tokens WHERE consumed_at IS NULL",
      )).rows[0].count),
      1,
    );
    assert.equal(
      Number((await pool.query("SELECT count(*) FROM oauth_access_tokens")).rows[0].count),
      1,
    );

    currentTime = new Date(currentTime.getTime() + 1_000);
    await flow.provider.cleanupExpired();
    assert.equal(
      Number((await pool.query("SELECT count(*) FROM oauth_connections")).rows[0].count),
      0,
    );
  });

  integrationTest("bounds consumed-token and unused-client retention", async () => {
    if (!pool) return;
    const flow = await linkedFlow("persistent");
    const tokens = await flow.provider.exchangeAuthorizationCode(
      flow.client,
      flow.authorizationCode,
      undefined,
      flow.client.redirect_uris[0],
    );
    assert.ok(tokens.refresh_token);
    await flow.provider.exchangeRefreshToken(flow.client, tokens.refresh_token);
    await pool.query(
      `UPDATE oauth_refresh_tokens
       SET consumed_at = now() - interval '8 days'
       WHERE consumed_at IS NOT NULL`,
    );
    await pool.query(
      `INSERT INTO oauth_clients (client_id, metadata, created_at)
       VALUES (
         'unused-client',
         '{"client_id":"unused-client","redirect_uris":["https://client.example.test/callback"],"token_endpoint_auth_method":"none"}',
         now() - interval '31 days'
       )`,
    );

    await flow.provider.cleanupExpired();

    assert.equal(
      Number((await pool.query(
        "SELECT count(*) FROM oauth_refresh_tokens WHERE consumed_at IS NOT NULL",
      )).rows[0].count),
      0,
    );
    assert.equal(
      Number((await pool.query(
        "SELECT count(*) FROM oauth_clients WHERE client_id = 'unused-client'",
      )).rows[0].count),
      0,
    );
    assert.equal(
      Number((await pool.query("SELECT count(*) FROM oauth_connections")).rows[0].count),
      1,
    );
  });

  integrationTest("deletes an abandoned persistent link after its code expires", async () => {
    if (!pool) return;
    const abandoned = await linkedFlow("persistent");
    const provisional = await pool.query(
      "SELECT expires_at FROM oauth_connections",
    );
    assert.equal(provisional.rowCount, 1);
    assert.ok(provisional.rows[0].expires_at instanceof Date);

    await pool.query(
      "UPDATE oauth_authorization_codes SET expires_at = now() - interval '1 second'",
    );
    await pool.query(
      "UPDATE oauth_connections SET expires_at = now() - interval '1 second'",
    );
    await abandoned.provider.cleanupExpired();

    assert.equal(
      Number((await pool.query(
        "SELECT count(*) FROM oauth_connections WHERE retention = 'persistent'",
      )).rows[0].count),
      0,
    );
  });

  integrationTest("serializes duplicate links for the same client and key", async () => {
    if (!pool) return;
    await Promise.all([
      linkedFlow("persistent"),
      linkedFlow("persistent"),
    ]);

    assert.equal(
      Number((await pool.query(
        "SELECT count(*) FROM oauth_connections WHERE client_id = 'test-client'",
      )).rows[0].count),
      1,
    );
  });

  integrationTest("revokes the connection during a concurrent refresh replay", async () => {
    if (!pool) return;
    const flow = await linkedFlow("persistent");
    const tokens = await flow.provider.exchangeAuthorizationCode(
      flow.client,
      flow.authorizationCode,
      undefined,
      flow.client.redirect_uris[0],
    );
    assert.ok(tokens.refresh_token);

    const outcomes = await Promise.allSettled([
      flow.provider.exchangeRefreshToken(flow.client, tokens.refresh_token),
      flow.provider.exchangeRefreshToken(flow.client, tokens.refresh_token),
    ]);
    assert.equal(outcomes.filter((outcome) => outcome.status === "fulfilled").length, 1);
    assert.equal(outcomes.filter((outcome) => outcome.status === "rejected").length, 1);
    assert.equal(
      Number((await pool.query("SELECT count(*) FROM oauth_connections")).rows[0].count),
      0,
    );
  });

  integrationTest("rejects redirect fragments and embedded credentials", async () => {
    const flow = await linkedFlow("session");
    const registerClient = flow.provider.clientsStore.registerClient;
    if (!registerClient) throw new Error("Dynamic registration is unavailable.");
    for (const redirectUri of [
      "https://client.example.test/callback#fragment",
      "https://user:password@client.example.test/callback",
    ]) {
      await assert.rejects(async () => await registerClient({
        client_id: opaqueToken(),
        client_id_issued_at: Math.floor(Date.now() / 1000),
        redirect_uris: [redirectUri],
        token_endpoint_auth_method: "none",
      } as never), /redirect URIs/);
    }
  });
});
