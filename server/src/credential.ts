import type { Request } from "express";
import type { PostgresOAuthProvider } from "./oauthProvider.js";
import type { ValidationResult } from "./auth.js";
import { invalidateKey, validateKey } from "./auth.js";
import { takeSecretKey } from "./requestKey.js";

const MAX_BEARER_BYTES = 2_048;

export interface MembershipCredential {
  readonly kind: "header" | "oauth";
  validateMembership(): Promise<ValidationResult>;
  getPlaintextKey(): Promise<string>;
  invalidate(): Promise<void>;
  clear(): void;
}

export class HeaderCredential implements MembershipCredential {
  readonly kind = "header" as const;

  constructor(private value: string) {}

  validateMembership(): Promise<ValidationResult> {
    return validateKey(this.value);
  }

  async getPlaintextKey(): Promise<string> {
    return this.value;
  }

  async invalidate(): Promise<void> {
    invalidateKey(this.value);
  }

  clear(): void {
    this.value = "";
  }
}

export class OAuthCredential implements MembershipCredential {
  readonly kind = "oauth" as const;
  private plaintextKey = "";

  constructor(
    private readonly provider: PostgresOAuthProvider,
    private readonly connectionId: string,
  ) {}

  async validateMembership(): Promise<ValidationResult> {
    // The key was validated during linking and is periodically revalidated
    // during refresh. Local metadata searches do not need to decrypt it.
    return { ok: true };
  }

  async getPlaintextKey(): Promise<string> {
    if (!this.plaintextKey) {
      this.plaintextKey = await this.provider.decryptConnectionKey(this.connectionId);
    }
    return this.plaintextKey;
  }

  async invalidate(): Promise<void> {
    await this.provider.deleteConnection(this.connectionId);
    this.clear();
  }

  clear(): void {
    this.plaintextKey = "";
  }
}

export interface ResolvedCredential {
  credential: MembershipCredential;
  oauth: boolean;
  present: boolean;
}

function takeBearerToken(req: Pick<Request, "headers" | "rawHeaders">): string {
  const value = req.headers.authorization;
  let occurrences = 0;
  delete req.headers.authorization;
  for (let index = 0; index < req.rawHeaders.length - 1; index += 2) {
    if (req.rawHeaders[index].toLowerCase() === "authorization") {
      occurrences++;
      req.rawHeaders[index + 1] = "[redacted]";
    }
  }
  if (
    occurrences !== 1 ||
    typeof value !== "string" ||
    Buffer.byteLength(value) > MAX_BEARER_BYTES
  ) {
    return "";
  }
  const match = /^Bearer ([A-Za-z0-9_-]+)$/i.exec(value);
  return match?.[1] || "";
}

export async function resolveCredential(
  req: Pick<Request, "headers" | "rawHeaders">,
  oauthProvider?: PostgresOAuthProvider,
): Promise<ResolvedCredential> {
  const headerKey = takeSecretKey(req);
  const bearerToken = takeBearerToken(req);
  if (!oauthProvider) {
    return {
      credential: new HeaderCredential(headerKey),
      oauth: false,
      present: Boolean(headerKey),
    };
  }
  if (headerKey && bearerToken) {
    throw Object.assign(new Error("Provide either OAuth or X-Annas-Secret-Key, not both."), {
      status: 400,
    });
  }
  if (bearerToken) {
    let auth;
    try {
      auth = await oauthProvider.verifyAccessToken(bearerToken);
    } catch {
      throw Object.assign(new Error("OAuth access token is invalid or expired."), {
        status: 401,
      });
    }
    const connectionId = auth.extra?.connectionId;
    if (typeof connectionId !== "string") {
      throw Object.assign(new Error("OAuth token is missing its connection."), { status: 401 });
    }
    return {
      credential: new OAuthCredential(oauthProvider, connectionId),
      oauth: true,
      present: true,
    };
  }
  return {
    credential: new HeaderCredential(headerKey),
    oauth: false,
    present: Boolean(headerKey),
  };
}
