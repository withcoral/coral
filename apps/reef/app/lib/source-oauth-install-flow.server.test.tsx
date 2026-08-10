import { renderToString } from 'react-dom/server'
import { describe, expect, it, vi } from 'vitest'

import { EXPIRED_SESSION_LOGIN_HEADER } from '@/auth/response'

import { expiredSessionLoginLocation, useOAuthInstallFlow } from './source-oauth-install-flow'

function OAuthFlowHarness() {
  useOAuthInstallFlow({
    fetchOAuthInstall: vi.fn(),
    onComplete: vi.fn(),
    openAuthorization: vi.fn(),
  })
  return null
}

describe('useOAuthInstallFlow server rendering', () => {
  it('does not read browser globals while rendering', () => {
    expect(() => renderToString(<OAuthFlowHarness />)).not.toThrow()
  })
})

// Regression: a stream fetch whose session expired must send the visitor to the
// login location the server named, rather than surfacing as an install error.
// Reef's Vitest coverage is Node-only, so this asserts the decision the hook
// makes rather than driving a render.
describe('expiredSessionLoginLocation', () => {
  it('reads the login location an expired-session response carries', () => {
    const loginLocation = '/login?returnTo=%2Fworkspaces%2Fanalytics%2Fsources%2Foauth-import'
    const response = new Response(null, {
      headers: { [EXPIRED_SESSION_LOGIN_HEADER]: loginLocation },
      status: 401,
    })

    expect(expiredSessionLoginLocation(response)).toBe(loginLocation)
  })

  it.each([
    ['an ordinary success', new Response(null, { status: 200 })],
    ['an ordinary failure', new Response(null, { status: 500 })],
  ])('returns null for %s', (_label, response) => {
    expect(expiredSessionLoginLocation(response)).toBeNull()
  })
})
