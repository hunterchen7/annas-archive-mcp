import { randomUUID } from "node:crypto";
import type { Response } from "express";
import type { Pool, PoolClient } from "pg";
import type {
  OAuthClientInformationFull,
  OAuthTokenRevocationRequest,
  OAuthTokens,
} from "@modelcontextprotocol/sdk/shared/auth.js";
import type {
  AuthorizationParams,
  OAuthServerProvider,
} from "@modelcontextprotocol/sdk/server/auth/provider.js";
import type { OAuthRegisteredClientsStore } from "@modelcontextprotocol/sdk/server/auth/clients.js";
import type { AuthInfo } from "@modelcontextprotocol/sdk/server/auth/types.js";
import {
  InvalidGrantError,
  InvalidClientMetadataError,
  InvalidScopeError,
  InvalidTokenError,
  TemporarilyUnavailableError,
} from "@modelcontextprotocol/sdk/server/auth/errors.js";
import type { ValidationResult } from "./auth.js";
import {
  KeyProtector,
  opaqueToken,
  tokenHash,
  type EncryptedSecret,
} from "./oauthCrypto.js";
import {
  retentionAllowsRefresh,
  retentionExpiresAt,
  type Retention,
} from "./oauthRetention.js";

const SCOPE = "annas:use";
const AUTHORIZATION_REQUEST_TTL_MS = 10 * 60 * 1000;
const AUTHORIZATION_CODE_TTL_MS = 5 * 60 * 1000;
const ACCESS_TOKEN_TTL_SECONDS = 60 * 60;
const REVALIDATE_AFTER_MS = 24 * 60 * 60 * 1000;
const USED_REFRESH_TOKEN_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
const UNUSED_CLIENT_RETENTION_MS = 30 * 24 * 60 * 60 * 1000;

interface AuthorizationRequestRow {
  client_id: string;
  redirect_uri: string;
  state: string | null;
  scopes: string[];
  code_challenge: string;
  resource: string | null;
  csrf_hash: string;
  expires_at: Date;
  metadata: OAuthClientInformationFull;
}

interface AuthorizationCodeRow {
  client_id: string;
  connection_id: string;
  redirect_uri: string;
  scopes: string[];
  code_challenge: string;
  resource: string | null;
  expires_at: Date;
  consumed_at: Date | null;
}

interface ConnectionRow {
  id: string;
  client_id: string;
  key_ciphertext: string;
  key_iv: string;
  key_tag: string;
  retention: Retention;
  last_validated_at: Date;
  expires_at: Date | null;
}

interface AccessTokenRow {
  client_id: string;
  connection_id: string;
  scopes: string[];
  resource: string | null;
  expires_at: Date;
  revoked_at: Date | null;
  connection_expires_at: Date | null;
}

interface RefreshTokenRow extends ConnectionRow {
  token_hash: string;
  connection_id: string;
  family_id: string;
  scopes: string[];
  resource: string | null;
  consumed_at: Date | null;
  revoked_at: Date | null;
}

export interface LinkRequest {
  requestToken: string;
  clientName: string;
  redirectUri: string;
  expiresAt: Date;
}

export interface CompletedLink {
  redirectUrl: string;
}

export interface OAuthProviderOptions {
  pool: Pool;
  protector: KeyProtector;
  issuerUrl: URL;
  resourceUrl: URL;
  validateKey: (key: string) => Promise<ValidationResult>;
  now?: () => Date;
}

function validRedirectUri(value: string): boolean {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    return false;
  }
  if (url.hash || url.username || url.password) return false;
  if (url.protocol === "https:") return true;
  return url.protocol === "http:" &&
    (url.hostname === "127.0.0.1" || url.hostname === "localhost" || url.hostname === "[::1]");
}

function scopesOrDefault(scopes: string[] | undefined): string[] {
  const resolved = scopes?.length ? [...new Set(scopes)] : [SCOPE];
  if (resolved.some((scope) => scope !== SCOPE)) {
    throw new InvalidScopeError(`Only the ${SCOPE} scope is supported.`);
  }
  return resolved;
}

function sameResource(expected: string | null, actual: URL | undefined): boolean {
  return actual === undefined ? true : expected === actual.href;
}

function envelope(row: ConnectionRow): EncryptedSecret {
  return {
    ciphertext: row.key_ciphertext,
    iv: row.key_iv,
    tag: row.key_tag,
  };
}

export class PostgresOAuthProvider implements OAuthServerProvider {
  readonly clientsStore: OAuthRegisteredClientsStore;
  private readonly pool: Pool;
  private readonly protector: KeyProtector;
  private readonly issuerUrl: URL;
  private readonly resourceUrl: URL;
  private readonly validateMembershipKey: (key: string) => Promise<ValidationResult>;
  private readonly now: () => Date;

  constructor(options: OAuthProviderOptions) {
    this.pool = options.pool;
    this.protector = options.protector;
    this.issuerUrl = options.issuerUrl;
    this.resourceUrl = options.resourceUrl;
    this.validateMembershipKey = options.validateKey;
    this.now = options.now ?? (() => new Date());
    this.clientsStore = {
      getClient: (clientId) => this.getClient(clientId),
      registerClient: (client) => this.registerClient(client as OAuthClientInformationFull),
    };
  }

  private async getClient(clientId: string): Promise<OAuthClientInformationFull | undefined> {
    const result = await this.pool.query(
      "SELECT metadata FROM oauth_clients WHERE client_id = $1",
      [clientId],
    );
    return result.rows[0]?.metadata as OAuthClientInformationFull | undefined;
  }

  private async registerClient(
    client: OAuthClientInformationFull,
  ): Promise<OAuthClientInformationFull> {
    if (
      client.redirect_uris.length === 0 ||
      client.redirect_uris.length > 10 ||
      client.redirect_uris.some((uri) => uri.length > 2_048 || !validRedirectUri(uri))
    ) {
      throw new InvalidClientMetadataError(
        "OAuth redirect URIs must use HTTPS or an HTTP loopback address.",
      );
    }
    const clientId = client.client_id || randomUUID();
    const publicClient: OAuthClientInformationFull = {
      ...client,
      client_id: clientId,
      client_id_issued_at: client.client_id_issued_at ?? Math.floor(this.now().getTime() / 1000),
      token_endpoint_auth_method: "none",
      client_secret: undefined,
      client_secret_expires_at: undefined,
    };
    await this.pool.query(
      `INSERT INTO oauth_clients (client_id, metadata)
       VALUES ($1, $2::jsonb)
       ON CONFLICT (client_id) DO UPDATE SET metadata = EXCLUDED.metadata`,
      [clientId, JSON.stringify(publicClient)],
    );
    return publicClient;
  }

  async authorize(
    client: OAuthClientInformationFull,
    params: AuthorizationParams,
    res: Response,
  ): Promise<void> {
    const scopes = scopesOrDefault(params.scopes);
    if (params.resource && params.resource.href !== this.resourceUrl.href) {
      throw new InvalidGrantError("The requested resource does not match this MCP server.");
    }
    const requestToken = opaqueToken();
    const csrfToken = opaqueToken();
    const expiresAt = new Date(this.now().getTime() + AUTHORIZATION_REQUEST_TTL_MS);
    await this.pool.query(
      `INSERT INTO oauth_authorization_requests
       (request_hash, client_id, redirect_uri, state, scopes, code_challenge,
        resource, csrf_hash, expires_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
      [
        tokenHash(requestToken),
        client.client_id,
        params.redirectUri,
        params.state ?? null,
        scopes,
        params.codeChallenge,
        params.resource?.href ?? this.resourceUrl.href,
        tokenHash(csrfToken),
        expiresAt,
      ],
    );
    res.cookie(oauthCsrfCookieName(requestToken), csrfToken, {
      httpOnly: true,
      secure: this.issuerUrl.protocol === "https:",
      sameSite: "lax",
      path: "/oauth",
      maxAge: AUTHORIZATION_REQUEST_TTL_MS,
    });
    const linkUrl = new URL("/oauth/link", this.issuerUrl);
    linkUrl.searchParams.set("request", requestToken);
    res.redirect(302, linkUrl.href);
  }

  async getLinkRequest(requestToken: string): Promise<LinkRequest | undefined> {
    const result = await this.pool.query(
      `SELECT r.client_id, r.redirect_uri, r.expires_at, c.metadata
       FROM oauth_authorization_requests r
       JOIN oauth_clients c ON c.client_id = r.client_id
       WHERE r.request_hash = $1 AND r.expires_at > $2`,
      [tokenHash(requestToken), this.now()],
    );
    const row = result.rows[0] as {
      redirect_uri: string;
      expires_at: Date;
      metadata: OAuthClientInformationFull;
    } | undefined;
    if (!row) return undefined;
    return {
      requestToken,
      clientName: row.metadata.client_name || "your MCP client",
      redirectUri: row.redirect_uri,
      expiresAt: row.expires_at,
    };
  }

  async completeLink(
    requestToken: string,
    csrfToken: string,
    membershipKey: string,
    retention: Retention,
  ): Promise<CompletedLink> {
    const request = await this.authorizationRequest(requestToken);
    if (!request || request.csrf_hash !== tokenHash(csrfToken)) {
      throw new InvalidGrantError("This linking request is invalid or expired.");
    }
    const validation = await this.validateMembershipKey(membershipKey);
    if (!validation.ok) {
      if (validation.reason === "unreachable") {
        throw new TemporarilyUnavailableError(
          "Anna's Archive could not be reached to validate the key. Please try again.",
        );
      }
      throw new InvalidGrantError("The Anna's Archive membership key was not accepted.");
    }

    const connectionId = randomUUID();
    const encrypted = this.protector.encrypt(membershipKey, connectionId);
    const fingerprint = this.protector.fingerprint(membershipKey, request.client_id);
    const authorizationCode = opaqueToken();
    const authorizationCodeExpiresAt = new Date(
      this.now().getTime() + AUTHORIZATION_CODE_TTL_MS,
    );
    // All links remain provisional until the client exchanges the OAuth code.
    const connectionExpiresAt = authorizationCodeExpiresAt;
    const database = await this.pool.connect();
    try {
      await database.query("BEGIN");
      const consumed = await database.query(
        `DELETE FROM oauth_authorization_requests
         WHERE request_hash = $1 AND csrf_hash = $2 AND expires_at > $3
         RETURNING client_id`,
        [tokenHash(requestToken), tokenHash(csrfToken), this.now()],
      );
      if (consumed.rowCount !== 1) {
        throw new InvalidGrantError("This linking request has already been used or expired.");
      }
      await database.query(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
        [`${request.client_id}:${fingerprint}`],
      );
      await database.query(
        `DELETE FROM oauth_connections
         WHERE client_id = $1 AND key_fingerprint = $2`,
        [request.client_id, fingerprint],
      );
      await database.query(
        `INSERT INTO oauth_connections
         (id, client_id, key_ciphertext, key_iv, key_tag, key_fingerprint,
          retention, last_validated_at, expires_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
        [
          connectionId,
          request.client_id,
          encrypted.ciphertext,
          encrypted.iv,
          encrypted.tag,
          fingerprint,
          retention,
          this.now(),
          connectionExpiresAt,
        ],
      );
      await database.query(
        `INSERT INTO oauth_authorization_codes
         (code_hash, client_id, connection_id, redirect_uri, scopes,
          code_challenge, resource, expires_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
        [
          tokenHash(authorizationCode),
          request.client_id,
          connectionId,
          request.redirect_uri,
          request.scopes,
          request.code_challenge,
          request.resource,
          authorizationCodeExpiresAt,
        ],
      );
      await database.query("COMMIT");
    } catch (error) {
      await database.query("ROLLBACK");
      throw error;
    } finally {
      database.release();
    }

    const redirectUrl = new URL(request.redirect_uri);
    redirectUrl.searchParams.set("code", authorizationCode);
    if (request.state) redirectUrl.searchParams.set("state", request.state);
    return { redirectUrl: redirectUrl.href };
  }

  private async authorizationRequest(
    requestToken: string,
  ): Promise<AuthorizationRequestRow | undefined> {
    const result = await this.pool.query(
      `SELECT r.*, c.metadata
       FROM oauth_authorization_requests r
       JOIN oauth_clients c ON c.client_id = r.client_id
       WHERE r.request_hash = $1 AND r.expires_at > $2`,
      [tokenHash(requestToken), this.now()],
    );
    return result.rows[0] as AuthorizationRequestRow | undefined;
  }

  async challengeForAuthorizationCode(
    client: OAuthClientInformationFull,
    authorizationCode: string,
  ): Promise<string> {
    const result = await this.pool.query(
      `SELECT code_challenge
       FROM oauth_authorization_codes
       WHERE code_hash = $1 AND client_id = $2
         AND consumed_at IS NULL AND expires_at > $3`,
      [tokenHash(authorizationCode), client.client_id, this.now()],
    );
    if (!result.rows[0]) throw new InvalidGrantError("Authorization code is invalid or expired.");
    return result.rows[0].code_challenge as string;
  }

  async exchangeAuthorizationCode(
    client: OAuthClientInformationFull,
    authorizationCode: string,
    _codeVerifier?: string,
    redirectUri?: string,
    resource?: URL,
  ): Promise<OAuthTokens> {
    const database = await this.pool.connect();
    try {
      await database.query("BEGIN");
      const result = await database.query(
        `UPDATE oauth_authorization_codes
         SET consumed_at = $3
         WHERE code_hash = $1 AND client_id = $2
           AND consumed_at IS NULL AND expires_at > $3
         RETURNING *`,
        [tokenHash(authorizationCode), client.client_id, this.now()],
      );
      const code = result.rows[0] as AuthorizationCodeRow | undefined;
      if (!code) throw new InvalidGrantError("Authorization code is invalid or expired.");
      if (redirectUri && redirectUri !== code.redirect_uri) {
        throw new InvalidGrantError("redirect_uri does not match the authorization request.");
      }
      if (!sameResource(code.resource, resource)) {
        throw new InvalidGrantError("resource does not match the authorization request.");
      }
      const activated = await this.activateConnection(database, code.connection_id);
      const tokens = await this.issueTokens(database, {
        clientId: code.client_id,
        connectionId: code.connection_id,
        scopes: code.scopes,
        resource: code.resource,
        includeRefreshToken: retentionAllowsRefresh(activated.retention),
        connectionExpiresAt: activated.expiresAt,
      });
      await database.query("COMMIT");
      return tokens;
    } catch (error) {
      await database.query("ROLLBACK");
      throw error;
    } finally {
      database.release();
    }
  }

  async exchangeRefreshToken(
    client: OAuthClientInformationFull,
    refreshToken: string,
    requestedScopes?: string[],
    resource?: URL,
  ): Promise<OAuthTokens> {
    const initial = await this.refreshToken(refreshToken);
    if (!initial || initial.client_id !== client.client_id || initial.revoked_at) {
      throw new InvalidGrantError("Refresh token is invalid.");
    }
    if (initial.consumed_at) {
      await this.pool.query("DELETE FROM oauth_connections WHERE id = $1", [
        initial.connection_id,
      ]);
      throw new InvalidGrantError("Refresh token reuse detected; the connection was revoked.");
    }
    if (initial.expires_at && initial.expires_at <= this.now()) {
      await this.pool.query("DELETE FROM oauth_connections WHERE id = $1", [
        initial.connection_id,
      ]);
      throw new InvalidGrantError("The linked connection has expired.");
    }
    const scopes = requestedScopes?.length ? scopesOrDefault(requestedScopes) : initial.scopes;
    if (scopes.some((scope) => !initial.scopes.includes(scope))) {
      throw new InvalidScopeError("Requested scopes exceed the original grant.");
    }
    if (!sameResource(initial.resource, resource)) {
      throw new InvalidGrantError("resource does not match the original grant.");
    }

    let revalidated = false;
    if (this.now().getTime() - initial.last_validated_at.getTime() >= REVALIDATE_AFTER_MS) {
      const validation = await this.revalidateConnection(initial);
      if (!validation.ok) {
        if (validation.reason === "unreachable") {
          throw new TemporarilyUnavailableError(
            "Anna's Archive could not be reached to revalidate the linked key.",
          );
        }
        await this.pool.query("DELETE FROM oauth_connections WHERE id = $1", [initial.id]);
        throw new InvalidGrantError("The linked Anna's Archive key is no longer valid.");
      }
      revalidated = true;
    }

    const database = await this.pool.connect();
    let transactionOpen = false;
    try {
      await database.query("BEGIN");
      transactionOpen = true;
      const consumed = await database.query(
        `UPDATE oauth_refresh_tokens
         SET consumed_at = $3
         WHERE token_hash = $1 AND client_id = $2
           AND consumed_at IS NULL AND revoked_at IS NULL
         RETURNING connection_id`,
        [tokenHash(refreshToken), client.client_id, this.now()],
      );
      if (consumed.rowCount !== 1) {
        await database.query("ROLLBACK");
        transactionOpen = false;
        await database.query("DELETE FROM oauth_connections WHERE id = $1", [
          initial.connection_id,
        ]);
        throw new InvalidGrantError("Refresh token reuse detected; the connection was revoked.");
      }
      if (revalidated) {
        await database.query(
          "UPDATE oauth_connections SET last_validated_at = $2 WHERE id = $1",
          [initial.connection_id, this.now()],
        );
      }
      const tokens = await this.issueTokens(database, {
        clientId: client.client_id,
        connectionId: initial.connection_id,
        scopes,
        resource: initial.resource,
        includeRefreshToken: true,
        familyId: initial.family_id,
        connectionExpiresAt: initial.expires_at,
      });
      await database.query("COMMIT");
      transactionOpen = false;
      return tokens;
    } catch (error) {
      if (transactionOpen) await database.query("ROLLBACK");
      throw error;
    } finally {
      database.release();
    }
  }

  private async revalidateConnection(connection: ConnectionRow): Promise<ValidationResult> {
    let plaintextKey = this.protector.decrypt(envelope(connection), connection.id);
    try {
      return await this.validateMembershipKey(plaintextKey);
    } finally {
      plaintextKey = "";
    }
  }

  private async refreshToken(token: string): Promise<RefreshTokenRow | undefined> {
    const result = await this.pool.query(
      `SELECT rt.*, c.id, c.key_ciphertext, c.key_iv, c.key_tag, c.retention,
              c.last_validated_at, c.expires_at
       FROM oauth_refresh_tokens rt
       JOIN oauth_connections c ON c.id = rt.connection_id
       WHERE rt.token_hash = $1`,
      [tokenHash(token)],
    );
    return result.rows[0] as RefreshTokenRow | undefined;
  }

  private async activateConnection(
    database: PoolClient,
    connectionId: string,
  ): Promise<{ retention: Retention; expiresAt: Date | null }> {
    const result = await database.query(
      `SELECT retention FROM oauth_connections
       WHERE id = $1 AND expires_at > $2
       FOR UPDATE`,
      [connectionId, this.now()],
    );
    if (!result.rows[0]) throw new InvalidGrantError("Linked connection is expired.");
    const retention = result.rows[0].retention as Retention;
    const expiresAt = retentionExpiresAt(retention, this.now());
    await database.query(
      "UPDATE oauth_connections SET expires_at = $2 WHERE id = $1",
      [connectionId, expiresAt],
    );
    return { retention, expiresAt };
  }

  private async issueTokens(
    database: PoolClient,
    options: {
      clientId: string;
      connectionId: string;
      scopes: string[];
      resource: string | null;
      includeRefreshToken: boolean;
      familyId?: string;
      connectionExpiresAt?: Date | null;
    },
  ): Promise<OAuthTokens> {
    const accessToken = opaqueToken();
    const issuedAt = this.now();
    const normalExpiresAt = new Date(
      issuedAt.getTime() + ACCESS_TOKEN_TTL_SECONDS * 1000,
    );
    if (
      options.connectionExpiresAt &&
      options.connectionExpiresAt <= issuedAt
    ) {
      throw new InvalidGrantError("Linked connection is expired.");
    }
    const expiresAt = options.connectionExpiresAt &&
        options.connectionExpiresAt < normalExpiresAt
      ? options.connectionExpiresAt
      : normalExpiresAt;
    const expiresIn = Math.floor(
      (expiresAt.getTime() - issuedAt.getTime()) / 1000,
    );
    if (expiresIn < 1) {
      throw new InvalidGrantError("Linked connection is expired.");
    }
    await database.query(
      `INSERT INTO oauth_access_tokens
       (token_hash, client_id, connection_id, scopes, resource, expires_at)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [
        tokenHash(accessToken),
        options.clientId,
        options.connectionId,
        options.scopes,
        options.resource,
        expiresAt,
      ],
    );
    let refreshToken: string | undefined;
    if (options.includeRefreshToken) {
      refreshToken = opaqueToken();
      await database.query(
        `INSERT INTO oauth_refresh_tokens
         (token_hash, client_id, connection_id, family_id, scopes, resource)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [
          tokenHash(refreshToken),
          options.clientId,
          options.connectionId,
          options.familyId ?? randomUUID(),
          options.scopes,
          options.resource,
        ],
      );
    }
    return {
      access_token: accessToken,
      token_type: "Bearer",
      expires_in: expiresIn,
      scope: options.scopes.join(" "),
      refresh_token: refreshToken,
    };
  }

  async verifyAccessToken(token: string): Promise<AuthInfo> {
    const result = await this.pool.query(
      `SELECT at.client_id, at.connection_id, at.scopes, at.resource,
              at.expires_at, at.revoked_at, c.expires_at AS connection_expires_at
       FROM oauth_access_tokens at
       JOIN oauth_connections c ON c.id = at.connection_id
       WHERE at.token_hash = $1`,
      [tokenHash(token)],
    );
    const row = result.rows[0] as AccessTokenRow | undefined;
    if (
      !row ||
      row.revoked_at ||
      row.expires_at <= this.now() ||
      row.resource !== this.resourceUrl.href ||
      (row.connection_expires_at && row.connection_expires_at <= this.now())
    ) {
      throw new InvalidTokenError("Access token is invalid or expired.");
    }
    return {
      token,
      clientId: row.client_id,
      scopes: row.scopes,
      expiresAt: Math.floor(row.expires_at.getTime() / 1000),
      resource: row.resource ? new URL(row.resource) : undefined,
      extra: { connectionId: row.connection_id },
    };
  }

  async decryptConnectionKey(connectionId: string): Promise<string> {
    const result = await this.pool.query(
      `SELECT * FROM oauth_connections
       WHERE id = $1 AND (expires_at IS NULL OR expires_at > $2)`,
      [connectionId, this.now()],
    );
    const row = result.rows[0] as ConnectionRow | undefined;
    if (!row) throw new InvalidTokenError("Linked connection is invalid or expired.");
    return this.protector.decrypt(envelope(row), row.id);
  }

  async deleteConnection(connectionId: string): Promise<void> {
    await this.pool.query("DELETE FROM oauth_connections WHERE id = $1", [
      connectionId,
    ]);
  }

  async revokeToken(
    client: OAuthClientInformationFull,
    request: OAuthTokenRevocationRequest,
  ): Promise<void> {
    const hash = tokenHash(request.token);
    const result = await this.pool.query(
      `SELECT connection_id, client_id FROM oauth_access_tokens WHERE token_hash = $1
       UNION ALL
       SELECT connection_id, client_id FROM oauth_refresh_tokens WHERE token_hash = $1
       LIMIT 1`,
      [hash],
    );
    const row = result.rows[0] as { connection_id: string; client_id: string } | undefined;
    if (row?.client_id === client.client_id) {
      await this.pool.query("DELETE FROM oauth_connections WHERE id = $1", [
        row.connection_id,
      ]);
    }
  }

  async cleanupExpired(): Promise<void> {
    const now = this.now();
    await this.pool.query(
      "DELETE FROM oauth_authorization_requests WHERE expires_at <= $1",
      [now],
    );
    await this.pool.query(
      "DELETE FROM oauth_authorization_codes WHERE expires_at <= $1",
      [now],
    );
    await this.pool.query(
      "DELETE FROM oauth_access_tokens WHERE expires_at <= $1",
      [now],
    );
    await this.pool.query(
      "DELETE FROM oauth_connections WHERE expires_at IS NOT NULL AND expires_at <= $1",
      [now],
    );
    await this.pool.query(
      "DELETE FROM oauth_refresh_tokens WHERE consumed_at IS NOT NULL AND consumed_at <= $1",
      [new Date(now.getTime() - USED_REFRESH_TOKEN_RETENTION_MS)],
    );
    await this.pool.query(
      `DELETE FROM oauth_clients c
       WHERE c.created_at <= $1
         AND NOT EXISTS (SELECT 1 FROM oauth_connections x WHERE x.client_id = c.client_id)
         AND NOT EXISTS (SELECT 1 FROM oauth_authorization_requests r WHERE r.client_id = c.client_id)
         AND NOT EXISTS (SELECT 1 FROM oauth_authorization_codes a WHERE a.client_id = c.client_id)
         AND NOT EXISTS (SELECT 1 FROM oauth_access_tokens t WHERE t.client_id = c.client_id)
         AND NOT EXISTS (SELECT 1 FROM oauth_refresh_tokens f WHERE f.client_id = c.client_id)`,
      [new Date(now.getTime() - UNUSED_CLIENT_RETENTION_MS)],
    );
  }
}

export const oauthScope = SCOPE;

export function oauthCsrfCookieName(requestToken: string): string {
  return `aa_oauth_csrf_${tokenHash(requestToken).slice(0, 16)}`;
}
