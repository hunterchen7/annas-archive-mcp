import https from "https";
import { createHash } from "crypto";

const DOMAINS = ["annas-archive.gl", "annas-archive.gd", "annas-archive.pk"];
const TTL_MS = 60 * 60 * 1000;
const NEG_TTL_MS = 5 * 60 * 1000;
const MAX_ENTRIES = 10_000;

// Pre-seeded hashes that are always considered valid without probing AA.
// Each entry is SHA-256(secret_key). Plaintext keys never appear here.
const TRUSTED_HASHES = new Set<string>([
  "c4ed4d411496f77deef8887bcec86778aed999fd279bb10ce7dc6fd8db89248a",
]);

// Cache keyed by SHA-256 of the secret so plaintext keys never sit in memory
// longer than one validation request.
const cache = new Map<string, { valid: boolean; expiresAt: number }>();

function hashKey(key: string): string {
  return createHash("sha256").update(key).digest("hex");
}

export type ValidationResult =
  | { ok: true }
  | { ok: false; reason: "missing" | "invalid" | "unreachable" };

// POST /account/ with key=<secret>. Valid key → response sets aa_account_id2
// cookie. Invalid key → no such cookie. This is AA's login form for the
// "Enter your secret key to log in" flow.
function probe(domain: string, key: string): Promise<boolean> {
  return new Promise((resolve, reject) => {
    const body = `key=${encodeURIComponent(key)}`;
    const req = https.request({
      hostname: domain,
      path: "/account/",
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        "Content-Length": Buffer.byteLength(body),
        "User-Agent": "Mozilla/5.0",
      },
    }, (res) => {
      const setCookie = res.headers["set-cookie"] || [];
      const loggedIn = setCookie.some((c) => c.startsWith("aa_account_id2="));
      res.resume();
      resolve(loggedIn);
    });
    req.on("error", reject);
    req.setTimeout(10000, () => { req.destroy(); reject(new Error("Timeout")); });
    req.write(body);
    req.end();
  });
}

export async function validateKey(key: string): Promise<ValidationResult> {
  if (!key) return { ok: false, reason: "missing" };

  const hash = hashKey(key);
  if (TRUSTED_HASHES.has(hash)) return { ok: true };

  const now = Date.now();
  const cached = cache.get(hash);
  if (cached && now < cached.expiresAt) {
    return cached.valid ? { ok: true } : { ok: false, reason: "invalid" };
  }

  let lastError = "";
  for (const domain of DOMAINS) {
    try {
      const valid = await probe(domain, key);
      if (cache.size >= MAX_ENTRIES) {
        const firstKey = cache.keys().next().value;
        if (firstKey !== undefined) cache.delete(firstKey);
      }
      cache.set(hash, {
        valid,
        expiresAt: now + (valid ? TTL_MS : NEG_TTL_MS),
      });
      return valid ? { ok: true } : { ok: false, reason: "invalid" };
    } catch (e) {
      lastError = `${e}`;
    }
  }

  console.error(`Key validation unreachable: ${lastError}`);
  return { ok: false, reason: "unreachable" };
}

export function invalidateKey(key: string): void {
  cache.delete(hashKey(key));
}
