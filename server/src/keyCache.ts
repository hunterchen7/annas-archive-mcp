import { createHmac, randomBytes } from "crypto";

interface CacheEntry {
  valid: boolean;
  expiresAt: number;
}

export interface KeyVerdictCacheOptions {
  validTtlMs: number;
  invalidTtlMs: number;
  maxEntries: number;
  secret?: Buffer;
  now?: () => number;
}

/**
 * In-memory validation cache keyed by a process-specific HMAC of the AA key.
 * The HMAC secret is generated at startup and is never persisted, so cache
 * identifiers cannot be correlated across process restarts.
 */
export class KeyVerdictCache {
  private readonly entries = new Map<string, CacheEntry>();
  private readonly secret: Buffer;
  private readonly now: () => number;

  constructor(private readonly options: KeyVerdictCacheOptions) {
    this.secret = options.secret ?? randomBytes(32);
    this.now = options.now ?? Date.now;
  }

  get(key: string): boolean | undefined {
    const fingerprint = this.identifier(key);
    const entry = this.entries.get(fingerprint);
    if (!entry) return undefined;

    if (this.now() >= entry.expiresAt) {
      this.entries.delete(fingerprint);
      return undefined;
    }
    return entry.valid;
  }

  set(key: string, valid: boolean): void {
    const fingerprint = this.identifier(key);
    this.pruneExpired();

    if (!this.entries.has(fingerprint) && this.entries.size >= this.options.maxEntries) {
      const oldest = this.entries.keys().next().value;
      if (oldest !== undefined) this.entries.delete(oldest);
    }

    // Reinsert existing entries so insertion order reflects the latest verdict update.
    this.entries.delete(fingerprint);
    this.entries.set(fingerprint, {
      valid,
      expiresAt: this.now() + (valid ? this.options.validTtlMs : this.options.invalidTtlMs),
    });
  }

  delete(key: string): void {
    this.entries.delete(this.identifier(key));
  }

  pruneExpired(): void {
    const now = this.now();
    for (const [fingerprint, entry] of this.entries) {
      if (now >= entry.expiresAt) this.entries.delete(fingerprint);
    }
  }

  get size(): number {
    return this.entries.size;
  }

  /**
   * Return a process-local, non-reversible identifier suitable for ephemeral
   * coordination maps. It changes on every process restart.
   */
  identifier(key: string): string {
    return createHmac("sha256", this.secret).update(key).digest("base64url");
  }
}
