import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

interface CommandOptions {
  input?: Buffer;
  timeoutMs?: number;
  maxBufferBytes?: number;
}

export function runTextCommand(
  command: string,
  args: readonly string[],
  options: CommandOptions = {},
): string {
  const privateHome = fs.mkdtempSync(path.join(os.tmpdir(), "aa-parser-home-"));
  const env: NodeJS.ProcessEnv = {
    HOME: privateHome,
    XDG_CACHE_HOME: path.join(privateHome, "cache"),
    XDG_CONFIG_HOME: path.join(privateHome, "config"),
  };
  for (const name of ["PATH", "LANG", "LC_ALL", "LC_CTYPE", "TZ", "TMPDIR"]) {
    if (process.env[name] !== undefined) env[name] = process.env[name];
  }

  try {
    const result = spawnSync(command, [...args], {
      input: options.input,
      encoding: "utf-8",
      env,
      maxBuffer: options.maxBufferBytes ?? 100 * 1024 * 1024,
      timeout: options.timeoutMs ?? 120_000,
      shell: false,
      windowsHide: true,
    });
    if (result.error) throw result.error;
    if (result.status !== 0) {
      const reason = result.signal ? `signal ${result.signal}` : `status ${result.status}`;
      throw new Error(`${command} failed with ${reason}`);
    }
    return result.stdout || "";
  } finally {
    fs.rmSync(privateHome, { recursive: true, force: true });
  }
}
