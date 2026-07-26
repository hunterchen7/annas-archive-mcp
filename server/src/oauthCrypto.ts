import {
  createCipheriv,
  createDecipheriv,
  createHash,
  createHmac,
  hkdfSync,
  randomBytes,
  timingSafeEqual,
} from "node:crypto";

const KEY_BYTES = 32;
const IV_BYTES = 12;
const TAG_BYTES = 16;
const ENCRYPTION_CONTEXT = "annas-archive-mcp/oauth-key/v1";
const FINGERPRINT_CONTEXT = "annas-archive-mcp/key-fingerprint/v1";

export interface EncryptedSecret {
  ciphertext: string;
  iv: string;
  tag: string;
}

function decodeBase64Key(encoded: string): Buffer {
  if (!encoded || !/^[A-Za-z0-9+/_-]+={0,2}$/.test(encoded)) {
    throw new Error("OAUTH_KEY_ENCRYPTION_KEY must be a base64-encoded 32-byte key.");
  }
  const normalized = encoded.replace(/-/g, "+").replace(/_/g, "/");
  const key = Buffer.from(normalized, "base64");
  if (key.length !== KEY_BYTES) {
    throw new Error("OAUTH_KEY_ENCRYPTION_KEY must decode to exactly 32 bytes.");
  }
  return key;
}

export class KeyProtector {
  private readonly encryptionKey: Buffer;
  private readonly fingerprintKey: Buffer;

  constructor(encodedMasterKey: string) {
    const masterKey = decodeBase64Key(encodedMasterKey);
    this.encryptionKey = Buffer.from(hkdfSync(
      "sha256",
      masterKey,
      Buffer.from(ENCRYPTION_CONTEXT),
      Buffer.from("encryption"),
      KEY_BYTES,
    ));
    this.fingerprintKey = Buffer.from(hkdfSync(
      "sha256",
      masterKey,
      Buffer.from(FINGERPRINT_CONTEXT),
      Buffer.from("fingerprint"),
      KEY_BYTES,
    ));
    masterKey.fill(0);
  }

  encrypt(secret: string, connectionId: string): EncryptedSecret {
    if (!secret) throw new Error("Cannot encrypt an empty key.");
    const iv = randomBytes(IV_BYTES);
    const cipher = createCipheriv("aes-256-gcm", this.encryptionKey, iv, {
      authTagLength: TAG_BYTES,
    });
    cipher.setAAD(Buffer.from(`${ENCRYPTION_CONTEXT}:${connectionId}`));
    const ciphertext = Buffer.concat([
      cipher.update(secret, "utf8"),
      cipher.final(),
    ]);
    const tag = cipher.getAuthTag();
    return {
      ciphertext: ciphertext.toString("base64url"),
      iv: iv.toString("base64url"),
      tag: tag.toString("base64url"),
    };
  }

  decrypt(encrypted: EncryptedSecret, connectionId: string): string {
    const iv = Buffer.from(encrypted.iv, "base64url");
    const tag = Buffer.from(encrypted.tag, "base64url");
    if (iv.length !== IV_BYTES || tag.length !== TAG_BYTES) {
      throw new Error("Stored key envelope is malformed.");
    }
    const decipher = createDecipheriv("aes-256-gcm", this.encryptionKey, iv, {
      authTagLength: TAG_BYTES,
    });
    decipher.setAAD(Buffer.from(`${ENCRYPTION_CONTEXT}:${connectionId}`));
    decipher.setAuthTag(tag);
    return Buffer.concat([
      decipher.update(Buffer.from(encrypted.ciphertext, "base64url")),
      decipher.final(),
    ]).toString("utf8");
  }

  fingerprint(secret: string): string {
    return createHmac("sha256", this.fingerprintKey)
      .update(secret)
      .digest("base64url");
  }
}

export function opaqueToken(bytes = 32): string {
  return randomBytes(bytes).toString("base64url");
}

export function tokenHash(token: string): string {
  return createHash("sha256").update(token).digest("base64url");
}

export function safeTokenEqual(left: string, right: string): boolean {
  const leftHash = Buffer.from(tokenHash(left), "base64url");
  const rightHash = Buffer.from(tokenHash(right), "base64url");
  return timingSafeEqual(leftHash, rightHash);
}

