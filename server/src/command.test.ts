import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { runTextCommand } from "./command.js";

describe("runTextCommand", () => {
  test("passes metacharacters as a literal argument without a shell", () => {
    const value = '"; echo injected; $("';
    const output = runTextCommand(process.execPath, [
      "-e",
      "process.stdout.write(process.argv[1])",
      value,
    ]);
    assert.equal(output, value);
  });

  test("throws on a non-zero exit", () => {
    assert.throws(
      () => runTextCommand(process.execPath, ["-e", "process.exit(2)"]),
      /status 2/,
    );
  });
});
