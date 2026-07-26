import assert from "node:assert/strict";
import { describe, test } from "node:test";
import type { OAuthServerProvider } from "@modelcontextprotocol/sdk/server/auth/provider.js";
import { publicClientOAuthMetadata } from "./oauthMetadata.js";

describe("publicClientOAuthMetadata", () => {
  test("truthfully advertises public-client token and revocation auth", () => {
    const provider = {
      clientsStore: { getClient: async () => undefined },
      revokeToken: async () => {},
    } as unknown as OAuthServerProvider;
    const metadata = publicClientOAuthMetadata({
      provider,
      issuerUrl: new URL("https://mcp.example.test"),
      resourceServerUrl: new URL("https://mcp.example.test/mcp"),
    });

    assert.deepEqual(metadata.token_endpoint_auth_methods_supported, ["none"]);
    assert.deepEqual(metadata.revocation_endpoint_auth_methods_supported, ["none"]);
    assert.equal(metadata.revocation_endpoint, "https://mcp.example.test/revoke");
  });
});
