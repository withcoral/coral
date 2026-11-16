/** Translate host-issued codes only; upstream descriptions can contain credentials. */
export function oauthFailureMessage(code: string | undefined, canConfigure = false): string {
  switch (code) {
    case 'oauth_client_configuration':
      return canConfigure
        ? 'The service rejected the OAuth client ID or secret. Check the client ID, client secret, and client authentication method.'
        : 'Ask the provider owner to repair the OAuth application.'
    // The service reports a denial in two places, and Coral stores a code for each:
    // `access_denied` from the browser's return, `oauth_access_denied` from the token endpoint.
    case 'access_denied':
    case 'oauth_access_denied':
      return 'You denied access. Sign in again to approve access.'
    case 'oauth_device_expired':
      return 'The sign-in code expired. Sign in again to get a new code.'
    // The last two are answers to a device sign-in's poll. Anywhere else they do not fit the request.
    case 'oauth_token_response_invalid':
    case 'oauth_authorization_pending':
    case 'oauth_slow_down':
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
    // The copy names no address: the refusal does not say which one, or why.
    case 'outbound_destination_rejected':
      return canConfigure
        ? "Coral's outbound settings do not allow this service's sign-in address. Check the deployment's outbound settings before you try again."
        : "Coral's outbound settings do not allow this service's sign-in address. Ask the provider owner to check the deployment's outbound settings."
    case 'configuration_changed':
      return 'This provider changed during sign-in. Sign in again.'
    // `authorization_failed` is Coral's own fault, and any other code is not one Coral issued.
    default:
      return 'Sign-in did not complete. Try again.'
  }
}
