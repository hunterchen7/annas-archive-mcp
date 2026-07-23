import { spawnSync } from "node:child_process";

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
  const result = spawnSync(command, [...args], {
    input: options.input,
    encoding: "utf-8",
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
}
