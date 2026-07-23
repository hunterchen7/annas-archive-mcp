export const MD5_PATTERN = /^[a-f0-9]{32}$/i;

export function isMd5(value: unknown): value is string {
  return typeof value === "string" && MD5_PATTERN.test(value);
}
