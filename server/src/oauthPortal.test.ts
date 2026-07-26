import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { InvalidGrantError } from "@modelcontextprotocol/sdk/server/auth/errors.js";
import { callbackPage, portalErrorPage } from "./oauthPortal.js";

describe("OAuth portal terminal responses", () => {
  test("returns through a CSP-safe callback page instead of a form redirect", () => {
    const callbackUrl =
      "https://claude.ai/api/mcp/auth_callback?code=one-time-code&state=client-state";
    const body = callbackPage(callbackUrl);

    assert.match(body, /http-equiv="refresh"/);
    assert.match(body, /https:\/\/claude\.ai\/api\/mcp\/auth_callback/);
    assert.match(body, /Continue to your MCP client/);
    assert.match(body, /one-time OAuth code, not your Anna's Archive key/);
  });

  test("renders a safe restart page when a valid-looking request has expired", () => {
    const body = portalErrorPage(
      new InvalidGrantError("This linking request is invalid or expired."),
      true,
    );

    assert.ok(body);
    assert.match(body, /Link expired/);
    assert.match(body, /A previous submission may already have completed/);
    assert.match(body, /Start the connection again/);
    assert.ok(!body.includes("This linking request is invalid or expired."));
  });

  test("renders an invalid-link page for malformed submissions", () => {
    const body = portalErrorPage(
      new InvalidGrantError("This linking request is invalid or expired."),
      false,
    );

    assert.ok(body);
    assert.match(body, /Invalid linking request/);
  });

  test("still delegates unexpected provider failures", () => {
    assert.equal(
      portalErrorPage(new Error("database unavailable"), true),
      undefined,
    );
  });
});
