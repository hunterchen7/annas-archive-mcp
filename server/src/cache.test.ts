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

  test("canonicalizes mixed-case MD5 cache keys", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "aa-cache-case-test-"));
    try {
      const cache = new FileCache(root, 1024);
      const lower = "abcdef0123456789abcdef0123456789";
      const upper = lower.toUpperCase();
      const lowerPath = cache.pathFor(lower, "pdf");

      assert.equal(cache.pathFor(upper, "PDF"), lowerPath);
      fs.writeFileSync(lowerPath, "content");
      cache.put(`${upper}.PDF`, lowerPath);
      assert.equal(cache.get(`${lower}.pdf`), lowerPath);
      assert.equal(cache.get(`${upper}.PDF`), lowerPath);
      assert.equal(cache.count, 1);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test("does not follow a replaced shard directory during eviction", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "aa-cache-evict-test-"));
    const outside = fs.mkdtempSync(path.join(os.tmpdir(), "aa-cache-victim-"));
    try {
      const cache = new FileCache(root, 5);
      const first = "aa000000000000000000000000000000";
      const firstPath = cache.pathFor(first, "pdf");
      fs.writeFileSync(firstPath, "1234");
      cache.put(`${first}.pdf`, firstPath);

      const shard = path.dirname(firstPath);
      fs.renameSync(shard, `${shard}-old`);
      const outsideVictim = path.join(outside, path.basename(firstPath));
      fs.writeFileSync(outsideVictim, "do not delete");
      fs.symlinkSync(outside, shard);

      const second = "bb000000000000000000000000000000";
      const secondPath = cache.pathFor(second, "pdf");
      fs.writeFileSync(secondPath, "1234567890");
      cache.put(`${second}.pdf`, secondPath);

      assert.equal(fs.readFileSync(outsideVictim, "utf-8"), "do not delete");
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
      fs.rmSync(outside, { recursive: true, force: true });
    }
  });
});
