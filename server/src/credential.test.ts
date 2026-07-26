import assert from "node:assert/strict";
import { describe, test } from "node:test";
import {
  OAuthCredential,
  resolveCredential,
} from "./credential.js";
import type { PostgresOAuthProvider } from "./oauthProvider.js";

describe("OAuthCredential", () => {
  test("does not decrypt for membership checks or repeated key reads", async () => {
    let decryptions = 0;
    const provider = {
      async decryptConnectionKey(connectionId: string) {
        decryptions++;
        assert.equal(connectionId, "connection-1");
        return "membership-secret";
      },
    } as unknown as PostgresOAuthProvider;
    const credential = new OAuthCredential(provider, "connection-1");

    assert.deepEqual(await credential.validateMembership(), { ok: true });
    assert.equal(decryptions, 0);
    assert.equal(await credential.getPlaintextKey(), "membership-secret");
    assert.equal(await credential.getPlaintextKey(), "membership-secret");
    assert.equal(decryptions, 1);

    credential.clear();
    assert.equal(await credential.getPlaintextKey(), "membership-secret");
    assert.equal(decryptions, 2);
  });
});

describe("resolveCredential", () => {
  test("redacts and returns the legacy header credential", async () => {
    const req = {
      headers: { "x-annas-secret-key": "membership-secret" },
      rawHeaders: ["X-Annas-Secret-Key", "membership-secret"],
    };
    const resolved = await resolveCredential(req);

    assert.equal(resolved.present, true);
    assert.equal(resolved.oauth, false);
    assert.equal(await resolved.credential.getPlaintextKey(), "membership-secret");
    assert.equal(req.headers["x-annas-secret-key"], undefined);
    assert.equal(req.rawHeaders[1], "[redacted]");
  });

  test("redacts bearer tokens and rejects ambiguous credentials", async () => {
    const provider = {
      async verifyAccessToken(token: string) {
        assert.equal(token, "access-token");
        return {
          token,
          clientId: "client-1",
          scopes: ["annas:use"],
          extra: { connectionId: "connection-1" },
        };
      },
    } as unknown as PostgresOAuthProvider;
    const bearerReq = {
      headers: { authorization: "Bearer access-token" },
      rawHeaders: ["Authorization", "Bearer access-token"],
    };
    const resolved = await resolveCredential(bearerReq, provider);
    assert.equal(resolved.oauth, true);
    assert.equal(bearerReq.headers.authorization, undefined);
    assert.equal(bearerReq.rawHeaders[1], "[redacted]");

    await assert.rejects(() => resolveCredential({
      headers: {
        authorization: "Bearer access-token",
        "x-annas-secret-key": "membership-secret",
      },
      rawHeaders: [
        "Authorization", "Bearer access-token",
        "X-Annas-Secret-Key", "membership-secret",
      ],
    }, provider), /either OAuth/);
  });
});
