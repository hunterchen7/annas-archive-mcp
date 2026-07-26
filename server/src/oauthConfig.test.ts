import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { oauthConfigFromEnv } from "./oauthConfig.js";

describe("oauthConfigFromEnv", () => {
  test("keeps OAuth disabled when neither setting is present", () => {
    assert.equal(oauthConfigFromEnv({}), undefined);
  });

  test("builds an HTTPS issuer and path-specific MCP resource", () => {
    const config = oauthConfigFromEnv({
      PUBLIC_BASE_URL: "https://aa-mcp.example.com",
      OAUTH_KEY_ENCRYPTION_KEY: "key",
    });
    assert.equal(config?.issuerUrl.href, "https://aa-mcp.example.com/");
    assert.equal(config?.resourceUrl.href, "https://aa-mcp.example.com/mcp");
  });

  test("permits HTTP only for loopback development", () => {
    assert.equal(
      oauthConfigFromEnv({
        PUBLIC_BASE_URL: "http://localhost:3001",
        OAUTH_KEY_ENCRYPTION_KEY: "key",
      })?.issuerUrl.href,
      "http://localhost:3001/",
    );
    assert.throws(() => oauthConfigFromEnv({
      PUBLIC_BASE_URL: "http://example.com",
      OAUTH_KEY_ENCRYPTION_KEY: "key",
    }));
  });

  test("requires both settings and an origin-only base URL", () => {
    assert.throws(() => oauthConfigFromEnv({ PUBLIC_BASE_URL: "https://example.com" }));
    assert.throws(() => oauthConfigFromEnv({ OAUTH_KEY_ENCRYPTION_KEY: "key" }));
    assert.throws(() => oauthConfigFromEnv({
      PUBLIC_BASE_URL: "https://example.com/oauth",
      OAUTH_KEY_ENCRYPTION_KEY: "key",
    }));
  });
});
