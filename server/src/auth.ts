import { KeyVerdictCache } from "./keyCache.js";
import { safeRequest } from "./safeHttp.js";

const DOMAINS = ["annas-archive.gl", "annas-archive.gd", "annas-archive.pk"];
const ALLOWED_HOSTS = new Set(DOMAINS);
const TTL_MS = 15 * 60 * 1000;
const NEG_TTL_MS = 60 * 1000;
const MAX_ENTRIES = 10_000;

const cache = new KeyVerdictCache({
  validTtlMs: TTL_MS,
  invalidTtlMs: NEG_TTL_MS,
  maxEntries: MAX_ENTRIES,
});
const pendingValidations = new Map<string, Promise<ValidationResult>>();

// Expired fingerprints should not linger when traffic is quiet.
const pruneTimer = setInterval(() => cache.pruneExpired(), NEG_TTL_MS);
pruneTimer.unref();

export type ValidationResult =
  | { ok: true }
  | { ok: false; reason: "missing" | "invalid" | "unreachable" };

export function keyValidationError(
  result: ValidationResult,
): { status: 401 | 503; message: string } | null {
  if (result.ok) return null;
  if (result.reason === "missing") {
    return {
      status: 401,
      message: "An Anna's Archive membership secret key is required. Provide it via the X-Annas-Secret-Key header.",
    };
  }
  if (result.reason === "invalid") {
    return { status: 401, message: "Invalid Anna's Archive secret key." };
  }
  return {
    status: 503,
    message: "Could not reach Anna's Archive to validate your key. Try again in a moment.",
  };
}

// POST /account/ with key=<secret>. Valid key → response sets aa_account_id2
// cookie. Invalid key → no such cookie. This is AA's login form for the
// "Enter your secret key to log in" flow.
async function probe(domain: string, key: string): Promise<boolean> {
  const body = new URLSearchParams({ key }).toString();
  const response = await safeRequest(`https://${domain}/account/`, {
    allowedHosts: ALLOWED_HOSTS,
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      "Content-Length": Buffer.byteLength(body),
      "User-Agent": "Mozilla/5.0",
    },
    body,
    maxBytes: 2 * 1024 * 1024,
    timeoutMs: 10_000,
    maxRedirects: 0,
    followRedirects: false,
  });
  const setCookie = response.headers["set-cookie"] || [];
  return setCookie.some((cookie) => cookie.startsWith("aa_account_id2="));
}

async function validateUncached(key: string): Promise<ValidationResult> {
  let lastError = "";
  for (const domain of DOMAINS) {
    try {
      const valid = await probe(domain, key);
      cache.set(key, valid);
      return valid ? { ok: true } : { ok: false, reason: "invalid" };
    } catch (e) {
      lastError = `${e}`;
    }
  }

  console.error(`Key validation unreachable: ${lastError}`);
  return { ok: false, reason: "unreachable" };
}

export async function validateKey(key: string): Promise<ValidationResult> {
  if (!key) return { ok: false, reason: "missing" };

  const cached = cache.get(key);
  if (cached !== undefined) {
    return cached ? { ok: true } : { ok: false, reason: "invalid" };
  }

  const identifier = cache.identifier(key);
  const pending = pendingValidations.get(identifier);
  if (pending) return pending;

  const validation = validateUncached(key).finally(() => {
    pendingValidations.delete(identifier);
  });
  pendingValidations.set(identifier, validation);
  return validation;
}

export function invalidateKey(key: string): void {
  cache.delete(key);
}
