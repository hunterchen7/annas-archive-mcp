import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { isMd5 } from "./identifiers.js";

describe("isMd5", () => {
  test("accepts only 32 hexadecimal characters", () => {
    assert.equal(isMd5("0123456789abcdef0123456789abcdef"), true);
    assert.equal(isMd5("ABCDEF0123456789ABCDEF0123456789"), true);
  });

  test("rejects path and shell metacharacters", () => {
    assert.equal(isMd5("../../etc/passwd................"), false);
    assert.equal(isMd5("0123456789abcdef0123456789abcde;"), false);
    assert.equal(isMd5("0123456789abcdef0123456789abcde$"), false);
  });

  test("rejects non-string and wrong-length values", () => {
    assert.equal(isMd5(undefined), false);
    assert.equal(isMd5(["0123456789abcdef0123456789abcdef"]), false);
    assert.equal(isMd5("0123456789abcdef"), false);
  });
});
