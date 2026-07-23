import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { parseReadQuery, parseSearchQuery } from "./httpInput.js";

describe("parseSearchQuery", () => {
  test("parses bounded scalar inputs", () => {
    assert.deepEqual(
      parseSearchQuery({ query: "  security  ", year_from: "2020", limit: "12" }),
      {
        ok: true,
        value: {
          query: "security",
          title: undefined,
          author: undefined,
          publisher: undefined,
          isbn: undefined,
          doi: undefined,
          language: undefined,
          format: undefined,
          yearFrom: 2020,
          yearTo: undefined,
          limit: 12,
        },
      },
    );
  });

  test("rejects repeated, malformed, and inconsistent inputs", () => {
    assert.equal(parseSearchQuery({ query: ["one", "two"] }).ok, false);
    assert.equal(parseSearchQuery({ limit: "1e3" }).ok, false);
    assert.equal(parseSearchQuery({ year_from: "2025", year_to: "2020" }).ok, false);
    assert.equal(parseSearchQuery({ title: "x".repeat(257) }).ok, false);
  });
});

describe("parseReadQuery", () => {
  test("creates a bounded page range", () => {
    assert.deepEqual(parseReadQuery({ start_page: "4" }), {
      ok: true,
      value: { pageRange: "4-23" },
    });
    assert.deepEqual(parseReadQuery({ start_page: "999999" }), {
      ok: true,
      value: { pageRange: "999999-1000000" },
    });
  });

  test("rejects conflicting and oversized ranges", () => {
    assert.equal(parseReadQuery({ chapter: "2", start_page: "1" }).ok, false);
    assert.equal(parseReadQuery({ start_page: "10", end_page: "9" }).ok, false);
    assert.equal(parseReadQuery({ start_page: "1", end_page: "101" }).ok, false);
    assert.equal(parseReadQuery({ end_page: "5" }).ok, false);
  });
});
