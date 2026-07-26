import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { describe, test } from "node:test";
import {
  KeyProtector,
  opaqueToken,
  safeTokenEqual,
  tokenHash,
} from "./oauthCrypto.js";

const masterKey = randomBytes(32).toString("base64");

describe("KeyProtector", () => {
  test("round trips a key without deterministic ciphertext", () => {
    const protector = new KeyProtector(masterKey);
    const first = protector.encrypt("membership-secret", "connection-a");
    const second = protector.encrypt("membership-secret", "connection-a");

    assert.equal(protector.decrypt(first, "connection-a"), "membership-secret");
    assert.equal(protector.decrypt(second, "connection-a"), "membership-secret");
    assert.notEqual(first.ciphertext, second.ciphertext);
    assert.notEqual(first.iv, second.iv);
  });

  test("binds ciphertext to its connection and detects tampering", () => {
    const protector = new KeyProtector(masterKey);
    const encrypted = protector.encrypt("membership-secret", "connection-a");

    assert.throws(() => protector.decrypt(encrypted, "connection-b"));
    assert.throws(() => protector.decrypt({
      ...encrypted,
      ciphertext: `${encrypted.ciphertext.slice(0, -1)}A`,
    }, "connection-a"));
  });

  test("uses a deterministic keyed fingerprint without exposing a plain hash", () => {
    const first = new KeyProtector(masterKey);
    const second = new KeyProtector(masterKey);
    const other = new KeyProtector(randomBytes(32).toString("base64"));

    assert.equal(first.fingerprint("membership-secret"), second.fingerprint("membership-secret"));
    assert.notEqual(first.fingerprint("membership-secret"), other.fingerprint("membership-secret"));
    assert.notEqual(first.fingerprint("membership-secret"), tokenHash("membership-secret"));
  });

  test("rejects missing, malformed, and incorrectly sized master keys", () => {
    assert.throws(() => new KeyProtector(""));
    assert.throws(() => new KeyProtector("not base64!"));
    assert.throws(() => new KeyProtector(randomBytes(16).toString("base64")));
  });
});

describe("OAuth tokens", () => {
  test("generates opaque values and compares them without direct string equality", () => {
    const token = opaqueToken();
    assert.ok(token.length >= 43);
    assert.equal(safeTokenEqual(token, token), true);
    assert.equal(safeTokenEqual(token, opaqueToken()), false);
  });
});

