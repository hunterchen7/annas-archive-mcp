import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { isPublicAddress, safeRequest } from "./safeHttp.js";

describe("isPublicAddress", () => {
  test("rejects loopback, private, link-local, and metadata ranges", () => {
    for (const address of [
      "127.0.0.1",
      "10.0.0.1",
      "172.16.0.1",
      "192.168.1.1",
      "169.254.169.254",
      "::1",
      "fe80::1",
      "fd00::1",
      "::ffff:127.0.0.1",
    ]) {
      assert.equal(isPublicAddress(address), false, address);
    }
  });

  test("accepts representative public addresses", () => {
    assert.equal(isPublicAddress("1.1.1.1"), true);
    assert.equal(isPublicAddress("2606:4700:4700::1111"), true);
  });
});

describe("safeRequest", () => {
  test("rejects unsafe URLs before making a request", async () => {
    await assert.rejects(
      safeRequest("http://example.com", { maxBytes: 100 }),
      /HTTPS/,
    );
    await assert.rejects(
      safeRequest("https://127.0.0.1/", { maxBytes: 100 }),
      /non-public/,
    );
    await assert.rejects(
      safeRequest("https://example.com:8443/", { maxBytes: 100 }),
      /standard HTTPS port/,
    );
    await assert.rejects(
      safeRequest("https://user:password@example.com/", { maxBytes: 100 }),
      /credentials/,
    );
    await assert.rejects(
      safeRequest("https://example.com/", {
        allowedHosts: new Set(["allowed.example"]),
        maxBytes: 100,
      }),
      /not allowed/,
    );
  });
});
