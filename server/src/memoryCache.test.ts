import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { MemoryTextCache } from "./memoryCache.js";

describe("MemoryTextCache", () => {
  test("returns cached values and replaces existing entries", () => {
    const cache = new MemoryTextCache(32);

    cache.put("book", "first");
    cache.put("book", "second");

    assert.equal(cache.get("book"), "second");
    assert.equal(cache.count, 1);
    assert.equal(cache.size, Buffer.byteLength("second"));
  });

  test("evicts least-recently-accessed entries when over capacity", () => {
    const cache = new MemoryTextCache(10);

    cache.put("old", "12345");
    cache.put("recent", "6789");
    assert.equal(cache.get("recent"), "6789");
    cache.put("new", "abcd");

    assert.equal(cache.get("old"), null);
    assert.equal(cache.get("recent"), "6789");
    assert.equal(cache.get("new"), "abcd");
  });
});
