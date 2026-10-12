/** Translate host-issued codes only; upstream descriptions can contain credentials. */
export function oauthFailureMessage(code: string | undefined, canConfigure = false): string {
  switch (code) {
    case 'oauth_client_configuration':
      return canConfigure
        ? 'The service rejected the OAuth client ID or secret. Check the client ID, client secret, and client authentication method.'
        : 'Ask the provider owner to repair the OAuth application.'
    case 'access_denied':
      return 'You denied access. Sign in again to approve access.'
    case 'oauth_token_response_invalid':
      return canConfigure
        ? 'The token endpoint returned an invalid response. Check the token URL.'
        : 'The token endpoint returned an invalid response. Ask the provider owner to check the token URL.'
    case 'oauth_token_unavailable':
      return 'The token endpoint is unavailable. Check the token URL or try again later.'
    case 'oauth_token_timeout':
      return 'The token endpoint did not respond in time. Try again later.'
    case 'oauth_code_rejected':
      return 'The service rejected the sign-in code. Sign in again to get a new code.'
    case 'oauth_redirect_mismatch':
      return canConfigure
        ? 'The callback URL does not match the OAuth application. Check the registered callback URL.'
        : 'The callback URL does not match the OAuth application. Ask the provider owner to check the registered callback URL.'
    case 'oauth_scope_rejected':
      return canConfigure
        ? 'The service rejected the requested permissions. Check the requested scopes.'
        : 'The service rejected the requested permissions. Ask the provider owner to check the requested scopes.'
    case 'oauth_request_rejected':
      return canConfigure
        ? 'The service rejected the token request. Check the OAuth application settings.'
        : 'The service rejected the token request. Ask the provider owner to check the OAuth application settings.'
    default:
      return 'Sign-in did not complete. Try again.'
  }
}
