import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { boundedInteger } from "./config.js";

describe("boundedInteger", () => {
  test("accepts in-range integers and falls back for unsafe configuration", () => {
    assert.equal(boundedInteger("120", 60, 1, 1000), 120);
    assert.equal(boundedInteger(undefined, 60, 1, 1000), 60);
    assert.equal(boundedInteger("NaN", 60, 1, 1000), 60);
    assert.equal(boundedInteger("0", 60, 1, 1000), 60);
    assert.equal(boundedInteger("1001", 60, 1, 1000), 60);
  });
});
