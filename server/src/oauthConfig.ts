export interface OAuthConfig {
  issuerUrl: URL;
  resourceUrl: URL;
  encryptionKey: string;
}

export function oauthConfigFromEnv(
  env: NodeJS.ProcessEnv = process.env,
): OAuthConfig | undefined {
  const encryptionKey = env.OAUTH_KEY_ENCRYPTION_KEY?.trim();
  const publicBaseUrl = env.PUBLIC_BASE_URL?.trim();
  if (!encryptionKey && !publicBaseUrl) return undefined;
  if (!encryptionKey || !publicBaseUrl) {
    throw new Error(
      "PUBLIC_BASE_URL and OAUTH_KEY_ENCRYPTION_KEY must both be set to enable OAuth.",
    );
  }
  const issuerUrl = new URL(publicBaseUrl);
  const isLoopback = issuerUrl.hostname === "localhost" ||
    issuerUrl.hostname === "127.0.0.1" ||
    issuerUrl.hostname === "[::1]";
  if (issuerUrl.protocol !== "https:" && !(issuerUrl.protocol === "http:" && isLoopback)) {
    throw new Error("PUBLIC_BASE_URL must use HTTPS, except for local development.");
  }
  if (
    issuerUrl.username ||
    issuerUrl.password ||
    issuerUrl.search ||
    issuerUrl.hash ||
    (issuerUrl.pathname !== "/" && issuerUrl.pathname !== "")
  ) {
    throw new Error("PUBLIC_BASE_URL must be an origin without credentials, path, query, or fragment.");
  }
  issuerUrl.pathname = "/";
  return {
    issuerUrl,
    resourceUrl: new URL("/mcp", issuerUrl),
    encryptionKey,
  };
}

