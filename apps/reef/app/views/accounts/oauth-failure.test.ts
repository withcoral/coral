import { describe, expect, it } from 'vitest'

import { oauthFailureMessage } from './oauth-failure'

const unexplained = 'Sign-in did not complete. Try again.'

describe('oauthFailureMessage', () => {
  // Every code Coral stores on a failed sign-in, with the cause its message names.
  it.for([
    ['oauth_client_configuration', 'The service rejected the OAuth client ID or secret.'],
    ['access_denied', 'You denied access.'],
    ['oauth_access_denied', 'You denied access.'],
    ['oauth_device_expired', 'The sign-in code expired.'],
    ['oauth_code_rejected', 'The service rejected the sign-in code.'],
    ['oauth_redirect_mismatch', 'The callback URL does not match the OAuth application.'],
    ['oauth_scope_rejected', 'The service rejected the requested permissions.'],
    ['oauth_request_rejected', 'The service rejected the token request.'],
    ['oauth_token_response_invalid', 'The token endpoint returned an invalid response.'],
    ['oauth_authorization_pending', 'The token endpoint returned an invalid response.'],
    ['oauth_slow_down', 'The token endpoint returned an invalid response.'],
    ['oauth_token_unavailable', 'The token endpoint is unavailable.'],
    ['oauth_token_timeout', 'The token endpoint did not respond in time.'],
    ['outbound_destination_rejected', "Coral's outbound settings do not allow"],
    ['configuration_changed', 'This provider changed during sign-in.'],
  ])('names the cause of %s', ([code, cause]) => {
    expect(oauthFailureMessage(code, true)).toContain(cause)
    // A person who cannot configure the provider also gets more than the fallback.
    expect(oauthFailureMessage(code)).not.toBe(unexplained)
  })

  it.for([
    // Coral's own faults share one code. The person who signs in cannot act on them.
    ['authorization_failed'],
    // A code that Coral did not issue can hold an upstream description.
    ['private-upstream-description-canary'],
    [undefined],
  ])('does not explain or repeat %s', ([code]) => {
    for (const canConfigure of [true, false])
      expect(oauthFailureMessage(code, canConfigure)).toBe(unexplained)
  })
})
