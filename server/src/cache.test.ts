import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, test } from "node:test";
import { FileCache } from "./cache.js";

describe("FileCache.pathFor", () => {
  test("keeps validated cache paths below the configured root", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "aa-cache-test-"));
    try {
      const cache = new FileCache(root, 1024);
      const md5 = "0123456789abcdef0123456789abcdef";
      const filePath = cache.pathFor(md5, "pdf");

      assert.equal(
        path.relative(fs.realpathSync(root), filePath),
        path.join("01", `${md5}.pdf`),
      );
      assert.throws(() => cache.pathFor("../../etc/passwd", "pdf"));
      assert.throws(() => cache.pathFor(md5, "../txt"));

      const linkedMd5 = "ab23456789abcdef0123456789abcdef";
      fs.symlinkSync(os.tmpdir(), path.join(root, "ab"));
      assert.throws(() => cache.pathFor(linkedMd5, "pdf"), /real directory/);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});
