import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { takeSecretKey } from "./requestKey.js";

describe("takeSecretKey", () => {
  test("returns the header value and redacts request references", () => {
    const req = {
      headers: { "x-annas-secret-key": "member-secret", accept: "application/json" },
      rawHeaders: [
        "Accept", "application/json",
        "X-Annas-Secret-Key", "member-secret",
      ],
    };

    assert.equal(takeSecretKey(req), "member-secret");
    assert.equal(req.headers["x-annas-secret-key"], undefined);
    assert.deepEqual(req.rawHeaders, [
      "Accept", "application/json",
      "X-Annas-Secret-Key", "[redacted]",
    ]);
  });

  test("rejects ambiguous repeated header values", () => {
    const req = {
      headers: { "x-annas-secret-key": "first, second" },
      rawHeaders: [
        "X-Annas-Secret-Key", "first",
        "X-Annas-Secret-Key", "second",
      ],
    };

    assert.equal(takeSecretKey(req), "");
    assert.equal(req.headers["x-annas-secret-key"], undefined);
    assert.deepEqual(req.rawHeaders, [
      "X-Annas-Secret-Key", "[redacted]",
      "X-Annas-Secret-Key", "[redacted]",
    ]);
  });

  test("rejects an oversized header value", () => {
    const secret = "x".repeat(513);
    const req = {
      headers: { "x-annas-secret-key": secret },
      rawHeaders: ["X-Annas-Secret-Key", secret],
    };

    assert.equal(takeSecretKey(req), "");
    assert.equal(req.rawHeaders[1], "[redacted]");
  });
});
