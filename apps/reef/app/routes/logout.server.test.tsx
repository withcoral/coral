import { beforeEach, describe, expect, it, vi } from 'vitest'

const authMocks = vi.hoisted(() => ({
  clearCsrfToken: vi.fn(),
  clearOAuthTransaction: vi.fn(),
  clearReefSession: vi.fn(),
  readReefSession: vi.fn(),
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
  readReefSession: authMocks.readReefSession,
}))

import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import type { RequiredAuthConfig } from '@/auth/types'

import { action } from './logout'

const requiredConfig: RequiredAuthConfig = {
  cookieName: 'reef_session',
  issuer: 'https://coral.example.test',
  mode: 'required',
  publicUrl: 'https://reef.example.test',
  sessionMaxAgeSeconds: 3600,
  sessionSecret: '0123456789abcdef0123456789abcdef',
}
const session = {
  accessToken: 'server-only-token',
  expiresAt: 4_102_444_800,
  tokenType: 'Bearer',
}

describe('logout route', () => {
  beforeEach(() => {
    authMocks.clearCsrfToken.mockReset()
    authMocks.clearOAuthTransaction.mockReset()
    authMocks.clearReefSession.mockReset()
    authMocks.reefAuthConfig.mockReset()
    authMocks.readReefSession.mockReset()
    authMocks.validateCsrfToken.mockReset()
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
    authMocks.readReefSession.mockResolvedValue(session)
    authMocks.validateCsrfToken.mockResolvedValue(false)
    const request = logoutRequest('invalid-token')

    await expect(action(authRouteTestArgs(request, {}))).rejects.toMatchObject({ status: 403 })
    expect(authMocks.validateCsrfToken).toHaveBeenCalledWith(request, requiredConfig, session)
    expect(authMocks.clearReefSession).not.toHaveBeenCalled()
  })

  it('rejects a hosted logout without an active session', async () => {
    authMocks.reefAuthConfig.mockReturnValue(requiredConfig)
    authMocks.readReefSession.mockResolvedValue(null)
    const request = logoutRequest('orphaned-token')

    await expect(action(authRouteTestArgs(request, {}))).rejects.toMatchObject({ status: 403 })
    expect(authMocks.validateCsrfToken).not.toHaveBeenCalled()
    expect(authMocks.clearReefSession).not.toHaveBeenCalled()
  })

  it('clears hosted auth state and lands on the signed-out screen', async () => {
    authMocks.reefAuthConfig.mockReturnValue(requiredConfig)
    authMocks.readReefSession.mockResolvedValue(session)
    authMocks.validateCsrfToken.mockResolvedValue(true)
    authMocks.clearCsrfToken.mockResolvedValue('reef_session_csrf=; Max-Age=0; Path=/')
    authMocks.clearOAuthTransaction.mockResolvedValue('reef_oauth=; Max-Age=0; Path=/')
    authMocks.clearReefSession.mockResolvedValue('reef_session=; Max-Age=0; Path=/')
    const request = logoutRequest('valid-token')

    const response = await action(authRouteTestArgs(request, {}))

    expect(response.status).toBe(302)
    expect(response.headers.get('Location')).toBe('/login?signedOut=1')
    expect(response.headers.get('Cache-Control')).toBe('private, no-store')
    expect(response.headers.get('Vary')).toBe('Cookie')
    expect(response.headers.getSetCookie().map(cookieName)).toEqual([
      'reef_session_csrf',
      'reef_oauth',
      'reef_session',
    ])
    expect(response.headers.getSetCookie().every((cookie) => cookie.includes('Max-Age=0'))).toBe(
      true,
    )
    expect(authMocks.validateCsrfToken).toHaveBeenCalledWith(request, requiredConfig, session)
    expect(authMocks.clearCsrfToken).toHaveBeenCalledWith(requiredConfig)
    expect(authMocks.clearOAuthTransaction).toHaveBeenCalledWith(requiredConfig)
    expect(authMocks.clearReefSession).toHaveBeenCalledWith(requiredConfig)
  })
})

function logoutRequest(csrf: string): Request {
  return new Request('https://reef.example.test/logout', {
    body: new URLSearchParams({ csrf }),
    method: 'POST',
  })
}

function cookieName(cookie: string): string {
  return cookie.slice(0, cookie.indexOf('='))
}
