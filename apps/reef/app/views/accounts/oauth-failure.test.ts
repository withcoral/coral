import { describe, expect, it } from 'vitest'

import { oauthFailureMessage } from './oauth-failure'

describe('oauthFailureMessage', () => {
  it.for([
    ['oauth_client_configuration', 'The service rejected the OAuth client ID or secret.'],
    ['oauth_code_rejected', 'The service rejected the sign-in code.'],
    ['oauth_redirect_mismatch', 'The callback URL does not match the OAuth application.'],
  ])('names the cause of %s for the person who configures the provider', ([code, cause]) => {
    expect(oauthFailureMessage(code, true)).toContain(cause)
  })

  it('never repeats a code that Coral did not issue', () => {
    const upstream = 'private-upstream-description-canary'
    expect(oauthFailureMessage(upstream, true)).not.toContain(upstream)
    expect(oauthFailureMessage(upstream, true)).toBe('Sign-in did not complete. Try again.')
  })
})
