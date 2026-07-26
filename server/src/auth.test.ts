import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { keyValidationError } from "./auth.js";

describe("keyValidationError", () => {
  test("maps validation outcomes to stable HTTP-safe errors", () => {
    assert.equal(keyValidationError({ ok: true }), null);
    assert.deepEqual(keyValidationError({ ok: false, reason: "missing" }), {
      status: 401,
      message: "An Anna's Archive membership key is required. Link with OAuth or provide X-Annas-Secret-Key.",
    });
    assert.deepEqual(keyValidationError({ ok: false, reason: "invalid" }), {
      status: 401,
      message: "Invalid Anna's Archive secret key.",
    });
    assert.equal(
      keyValidationError({ ok: false, reason: "unreachable" })?.status,
      503,
    );
  });
});
