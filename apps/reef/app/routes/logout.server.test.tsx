import { beforeEach, describe, expect, it, vi } from 'vitest'

const authMocks = vi.hoisted(() => ({
  clearCsrfToken: vi.fn(),
  clearOAuthTransaction: vi.fn(),
  clearReefSession: vi.fn(),
  reefAuthConfig: vi.fn(),
  validateCsrfToken: vi.fn(),
}))

vi.mock('@/auth/config.server', () => ({ reefAuthConfig: authMocks.reefAuthConfig }))
vi.mock('@/auth/csrf.server', () => ({
  clearCsrfToken: authMocks.clearCsrfToken,
  validateCsrfToken: authMocks.validateCsrfToken,
}))
vi.mock('@/auth/session.server', () => ({
  clearOAuthTransaction: authMocks.clearOAuthTransaction,
  clearReefSession: authMocks.clearReefSession,
}))

import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import type { RequiredAuthConfig } from '@/auth/types'

import { action, loader } from './logout'

const requiredConfig: RequiredAuthConfig = {
  clientId: 'coral-cloud-ui',
  cookieName: 'reef_session',
  cookieSecure: true,
  issuer: 'https://login.example.test',
  mode: 'required',
  redirectUri: 'https://reef.example.test/auth/callback',
  scope: 'coral:mcp',
  sessionMaxAgeSeconds: 3600,
  sessionSecret: '0123456789abcdef0123456789abcdef',
}

describe('logout route', () => {
  beforeEach(() => {
    authMocks.clearCsrfToken.mockReset()
    authMocks.clearOAuthTransaction.mockReset()
    authMocks.clearReefSession.mockReset()
    authMocks.reefAuthConfig.mockReset()
    authMocks.validateCsrfToken.mockReset()
    authMocks.clearCsrfToken.mockResolvedValue('reef_csrf=; Max-Age=0; Path=/')
    authMocks.clearOAuthTransaction.mockResolvedValue('reef_oauth=; Max-Age=0; Path=/')
    authMocks.clearReefSession.mockResolvedValue('reef_session=; Max-Age=0; Path=/')
  })

  it('redirects GET requests home without clearing a session', async () => {
    const response = await loader()

    expect(response.status).toBe(302)
    expect(response.headers.get('Location')).toBe('/')
    expect(response.headers.get('Cache-Control')).toBe('private, no-store')
    expect(authMocks.clearReefSession).not.toHaveBeenCalled()
  })

  it('keeps disabled local and desktop logout inert', async () => {
    authMocks.reefAuthConfig.mockReturnValue({ mode: 'disabled' })
    const request = logoutRequest('local-token')

    const response = await action(authRouteTestArgs(request, {}, null))

    expect(response.headers.get('Location')).toBe('/')
    expect(authMocks.validateCsrfToken).not.toHaveBeenCalled()
    expect(authMocks.clearReefSession).not.toHaveBeenCalled()
  })

  it('rejects non-POST mutations', async () => {
    const request = new Request('https://reef.example.test/logout', { method: 'DELETE' })

    await expect(action(authRouteTestArgs(request, {}))).rejects.toMatchObject({ status: 405 })
    expect(authMocks.reefAuthConfig).not.toHaveBeenCalled()
  })

  it('rejects a hosted logout with an invalid CSRF token', async () => {
    authMocks.reefAuthConfig.mockReturnValue(requiredConfig)
    authMocks.validateCsrfToken.mockResolvedValue(false)
    const request = logoutRequest('invalid-token')

    await expect(action(authRouteTestArgs(request, {}))).rejects.toMatchObject({ status: 403 })
    expect(authMocks.validateCsrfToken).toHaveBeenCalledWith(request, requiredConfig)
    expect(authMocks.clearReefSession).not.toHaveBeenCalled()
  })

  it('clears hosted auth cookies and lands on the signed-out screen', async () => {
    authMocks.reefAuthConfig.mockReturnValue(requiredConfig)
    authMocks.validateCsrfToken.mockResolvedValue(true)
    const request = logoutRequest('valid-token')

    const response = await action(authRouteTestArgs(request, {}))

    expect(response.status).toBe(302)
    expect(response.headers.get('Location')).toBe('/login?signedOut=1')
    expect(response.headers.getSetCookie()).toEqual([
      'reef_csrf=; Max-Age=0; Path=/',
      'reef_oauth=; Max-Age=0; Path=/',
      'reef_session=; Max-Age=0; Path=/',
    ])
    expect(response.headers.get('Cache-Control')).toBe('private, no-store')
  })
})

function logoutRequest(csrf: string): Request {
  return new Request('https://reef.example.test/logout', {
    body: new URLSearchParams({ csrf }),
    method: 'POST',
  })
}
