import assert from "node:assert/strict";
import { describe, test } from "node:test";
import {
  isRetention,
  retentionAllowsRefresh,
  retentionExpiresAt,
} from "./oauthRetention.js";

describe("OAuth retention", () => {
  const activatedAt = new Date("2026-07-26T18:00:00.000Z");

  test("accepts only the supported fixed choices", () => {
    for (const value of [
      "session",
      "days_7",
      "days_14",
      "days_30",
      "persistent",
    ]) {
      assert.equal(isRetention(value), true);
    }
    assert.equal(isRetention("days_365"), false);
    assert.equal(isRetention(7), false);
  });

  test("calculates expiry from successful OAuth activation", () => {
    assert.equal(
      retentionExpiresAt("session", activatedAt)?.toISOString(),
      "2026-07-26T19:00:00.000Z",
    );
    assert.equal(
      retentionExpiresAt("days_7", activatedAt)?.toISOString(),
      "2026-08-02T18:00:00.000Z",
    );
    assert.equal(
      retentionExpiresAt("days_14", activatedAt)?.toISOString(),
      "2026-08-09T18:00:00.000Z",
    );
    assert.equal(
      retentionExpiresAt("days_30", activatedAt)?.toISOString(),
      "2026-08-25T18:00:00.000Z",
    );
    assert.equal(retentionExpiresAt("persistent", activatedAt), null);
  });

  test("issues refresh tokens for every choice except one hour", () => {
    assert.equal(retentionAllowsRefresh("session"), false);
    assert.equal(retentionAllowsRefresh("days_7"), true);
    assert.equal(retentionAllowsRefresh("days_14"), true);
    assert.equal(retentionAllowsRefresh("days_30"), true);
    assert.equal(retentionAllowsRefresh("persistent"), true);
  });
});
