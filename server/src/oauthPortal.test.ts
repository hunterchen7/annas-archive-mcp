import assert from "node:assert/strict";
import { describe, test } from "node:test";
import {
  InvalidGrantError,
  ServerError,
  TemporarilyUnavailableError,
} from "@modelcontextprotocol/sdk/server/auth/errors.js";
import {
  callbackPage,
  portalErrorPage,
  resolvePortalErrorPage,
} from "./oauthPortal.js";

describe("OAuth portal terminal responses", () => {
  test("returns through a CSP-safe callback page instead of a form redirect", () => {
    const callbackUrl =
      "https://claude.ai/api/mcp/auth_callback?code=one-time-code&state=%22%3E%3Cscript%3Ealert(1)%3C%2Fscript%3E&resource=https%3A%2F%2Fexample.com%2Fmcp%3Fa%3D1%26b%3D2";
    const body = callbackPage(callbackUrl);
    const escapedCallbackUrl = callbackUrl.replaceAll("&", "&amp;");

    assert.match(body, /http-equiv="refresh"/);
    assert.match(body, /https:\/\/claude\.ai\/api\/mcp\/auth_callback/);
    assert.equal(
      body.match(new RegExp(escapedCallbackUrl.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "g"))?.length,
      2,
    );
    assert.ok(!body.includes("&state="));
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
    assert.equal(
      portalErrorPage(new ServerError("private provider detail"), true),
      undefined,
    );
  });

  test("does not query link state for unexpected server errors", async () => {
    let lookupCount = 0;
    const provider = {
      async getLinkRequest() {
        lookupCount += 1;
        throw new Error("should not run");
      },
    };

    assert.equal(
      await resolvePortalErrorPage(
        provider,
        new ServerError("private provider detail"),
        "a".repeat(32),
      ),
      undefined,
    );
    assert.equal(lookupCount, 0);
  });

  test("propagates link-state lookup failures for server logging", async () => {
    const databaseError = new Error("database unavailable");
    const provider = {
      async getLinkRequest() {
        throw databaseError;
      },
    };

    await assert.rejects(
      resolvePortalErrorPage(
        provider,
        new InvalidGrantError("This linking request is invalid or expired."),
        "a".repeat(32),
      ),
      databaseError,
    );
  });

  test("marks temporary upstream failures as retryable", async () => {
    const provider = {
      async getLinkRequest() {
        return undefined;
      },
    };

    const resolution = await resolvePortalErrorPage(
      provider,
      new TemporarilyUnavailableError("Anna's Archive could not be reached."),
      "a".repeat(32),
    );

    assert.equal(resolution?.status, 503);
  });
});
