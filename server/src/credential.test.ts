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
    let deletions = 0;
    const provider = {
      async decryptConnectionKey(connectionId: string) {
        decryptions++;
        assert.equal(connectionId, "connection-1");
        return "membership-secret";
      },
      async deleteConnection(connectionId: string) {
        assert.equal(connectionId, "connection-1");
        deletions++;
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
    await credential.invalidate();
    assert.equal(deletions, 1);
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
      headers: { authorization: "bearer access-token" },
      rawHeaders: ["Authorization", "bearer access-token"],
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

  test("ignores upstream Authorization when OAuth is disabled", async () => {
    const req = {
      headers: {
        authorization: "Bearer gateway-token",
        "x-annas-secret-key": "membership-secret",
      },
      rawHeaders: [
        "Authorization", "Bearer gateway-token",
        "X-Annas-Secret-Key", "membership-secret",
      ],
    };
    const resolved = await resolveCredential(req);

    assert.equal(resolved.oauth, false);
    assert.equal(resolved.present, true);
    assert.equal(await resolved.credential.getPlaintextKey(), "membership-secret");
    assert.equal(req.rawHeaders[1], "[redacted]");
    assert.equal(req.rawHeaders[3], "[redacted]");
  });
});
