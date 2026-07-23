import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, test } from "node:test";
import {
  resolveSafeFile,
  sumZipListingBytes,
  validateArchiveEntryNames,
} from "./safeArchive.js";

describe("validateArchiveEntryNames", () => {
  test("accepts normal relative entries and rejects traversal", () => {
    assert.doesNotThrow(() => validateArchiveEntryNames(["META-INF/container.xml", "OPS/chapter.xhtml"]));
    assert.throws(() => validateArchiveEntryNames(["../../outside"]));
    assert.throws(() => validateArchiveEntryNames(["..\\outside"]));
    assert.throws(() => validateArchiveEntryNames(["/etc/passwd"]));
    assert.throws(() => validateArchiveEntryNames(["C:\\Windows\\system.ini"]));
  });
});

describe("sumZipListingBytes", () => {
  test("sums declared uncompressed sizes and rejects oversized archives", () => {
    const listing = [
      "Archive: book.epub",
      "  Length      Date    Time    Name",
      "---------  ---------- -----   ----",
      "       12  2026-01-01 00:00   first.xhtml",
      "       34  2026-01-01 00:00   second.xhtml",
    ].join("\n");
    assert.equal(sumZipListingBytes(listing, 2), 46);
    assert.equal(
      sumZipListingBytes("       46  07-23-2026 12:34   real-infozip-output.epub", 1),
      46,
    );
    assert.throws(
      () => sumZipListingBytes(listing, 3),
      /Could not verify every archive entry size/,
    );
    assert.throws(
      () => sumZipListingBytes("999999999  2026-01-01 00:00   huge.bin"),
      /too large/,
    );
  });
});

describe("resolveSafeFile", () => {
  test("allows regular contained files and rejects outside paths and symlinks", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "aa-safe-archive-test-"));
    const outside = path.join(path.dirname(root), `${path.basename(root)}-outside`);
    try {
      fs.mkdirSync(path.join(root, "OPS"));
      fs.writeFileSync(path.join(root, "OPS", "chapter.xhtml"), "chapter");
      fs.writeFileSync(outside, "outside");
      fs.symlinkSync(outside, path.join(root, "OPS", "linked.xhtml"));

      assert.equal(
        resolveSafeFile(root, "chapter.xhtml", path.join(root, "OPS")),
        fs.realpathSync(path.join(root, "OPS", "chapter.xhtml")),
      );
      assert.equal(resolveSafeFile(root, "../../outside"), null);
      assert.equal(resolveSafeFile(root, "OPS/linked.xhtml"), null);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
      fs.rmSync(outside, { force: true });
    }
  });
});
