import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { KeyVerdictCache } from "./keyCache.js";

function makeCache(overrides: Partial<ConstructorParameters<typeof KeyVerdictCache>[0]> = {}) {
  let now = 1_000;
  const cache = new KeyVerdictCache({
    validTtlMs: 100,
    invalidTtlMs: 20,
    maxEntries: 2,
    secret: Buffer.alloc(32, 7),
    now: () => now,
    ...overrides,
  });
  return {
    cache,
    advance(ms: number) {
      now += ms;
    },
  };
}

describe("KeyVerdictCache", () => {
  test("stores only a validation verdict addressable by the original key", () => {
    const { cache } = makeCache();

    cache.set("member-secret", true);

    assert.equal(cache.get("member-secret"), true);
    assert.equal(cache.get("different-secret"), undefined);
  });

  test("uses shorter expiry for invalid keys and removes expired entries", () => {
    const { cache, advance } = makeCache();
    cache.set("valid", true);
    cache.set("invalid", false);

    advance(20);

    assert.equal(cache.get("invalid"), undefined);
    assert.equal(cache.get("valid"), true);
    assert.equal(cache.size, 1);
  });

  test("bounds entries and evicts the oldest verdict", () => {
    const { cache } = makeCache();
    cache.set("first", true);
    cache.set("second", true);
    cache.set("third", true);

    assert.equal(cache.size, 2);
    assert.equal(cache.get("first"), undefined);
    assert.equal(cache.get("second"), true);
    assert.equal(cache.get("third"), true);
  });
});
