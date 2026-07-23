import type { Request } from "express";

type KeyRequest = Pick<Request, "headers" | "rawHeaders">;
const MAX_SECRET_KEY_BYTES = 512;

/**
 * Take the AA key from a request and redact generic request-object references.
 * The returned string remains request-scoped and must not be persisted.
 */
export function takeSecretKey(req: KeyRequest): string {
  const value = req.headers["x-annas-secret-key"];
  let occurrences = 0;

  delete req.headers["x-annas-secret-key"];
  for (let i = 0; i < req.rawHeaders.length - 1; i += 2) {
    if (req.rawHeaders[i].toLowerCase() === "x-annas-secret-key") {
      occurrences++;
      req.rawHeaders[i + 1] = "[redacted]";
    }
  }

  if (
    occurrences !== 1 ||
    typeof value !== "string" ||
    Buffer.byteLength(value) > MAX_SECRET_KEY_BYTES
  ) {
    return "";
  }
  return value;
}
