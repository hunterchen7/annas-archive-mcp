import {
  createOAuthMetadata,
  type AuthRouterOptions,
} from "@modelcontextprotocol/sdk/server/auth/router.js";
import type { OAuthMetadata } from "@modelcontextprotocol/sdk/shared/auth.js";

export function publicClientOAuthMetadata(
  options: AuthRouterOptions,
): OAuthMetadata {
  return {
    ...createOAuthMetadata(options),
    token_endpoint_auth_methods_supported: ["none"],
    revocation_endpoint_auth_methods_supported: options.provider.revokeToken
      ? ["none"]
      : undefined,
  };
}
