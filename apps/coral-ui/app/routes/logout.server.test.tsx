import { beforeEach, describe, expect, it, vi } from 'vitest'

const authMocks = vi.hoisted(() => ({
  clearCsrfToken: vi.fn(),
  clearOAuthTransaction: vi.fn(),
  clearCoralUISession: vi.fn(),
  readCoralUISession: vi.fn(),
  coralUIAuthConfig: vi.fn(),
  validateCsrfToken: vi.fn(),
}))

vi.mock('@/auth/config.server', () => ({ coralUIAuthConfig: authMocks.coralUIAuthConfig }))
vi.mock('@/auth/csrf.server', () => ({
  clearCsrfToken: authMocks.clearCsrfToken,
  validateCsrfToken: authMocks.validateCsrfToken,
}))
vi.mock('@/auth/session.server', () => ({
  clearOAuthTransaction: authMocks.clearOAuthTransaction,
  clearCoralUISession: authMocks.clearCoralUISession,
  readCoralUISession: authMocks.readCoralUISession,
}))

import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import type { RequiredAuthConfig } from '@/auth/types'

import { action } from './logout'

const requiredConfig: RequiredAuthConfig = {
  cookieName: 'coral_ui_session',
  issuer: 'https://coral.example.test',
  mode: 'required',
  publicUrl: 'https://coral-ui.example.test',
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
    authMocks.clearCoralUISession.mockReset()
    authMocks.coralUIAuthConfig.mockReset()
    authMocks.readCoralUISession.mockReset()
    authMocks.validateCsrfToken.mockReset()
  })

  it('keeps disabled local and desktop logout inert', async () => {
    authMocks.coralUIAuthConfig.mockReturnValue({ mode: 'disabled' })
    const request = logoutRequest('local-token')

    const response = await action(authRouteTestArgs(request, {}, null))

    expect(response.headers.get('Location')).toBe('/')
    expect(authMocks.validateCsrfToken).not.toHaveBeenCalled()
    expect(authMocks.clearCoralUISession).not.toHaveBeenCalled()
  })

  it('rejects non-POST mutations', async () => {
    const request = new Request('https://coral-ui.example.test/logout', { method: 'DELETE' })

    await expect(action(authRouteTestArgs(request, {}))).rejects.toMatchObject({ status: 405 })
    expect(authMocks.coralUIAuthConfig).not.toHaveBeenCalled()
  })

  it('rejects a hosted logout with an invalid CSRF token', async () => {
    authMocks.coralUIAuthConfig.mockReturnValue(requiredConfig)
    authMocks.readCoralUISession.mockResolvedValue(session)
    authMocks.validateCsrfToken.mockResolvedValue(false)
    const request = logoutRequest('invalid-token')

    await expect(action(authRouteTestArgs(request, {}))).rejects.toMatchObject({ status: 403 })
    expect(authMocks.validateCsrfToken).toHaveBeenCalledWith(request, requiredConfig, session)
    expect(authMocks.clearCoralUISession).not.toHaveBeenCalled()
  })

  it('rejects a hosted logout without an active session', async () => {
    authMocks.coralUIAuthConfig.mockReturnValue(requiredConfig)
    authMocks.readCoralUISession.mockResolvedValue(null)
    const request = logoutRequest('orphaned-token')

    await expect(action(authRouteTestArgs(request, {}))).rejects.toMatchObject({ status: 403 })
    expect(authMocks.validateCsrfToken).not.toHaveBeenCalled()
    expect(authMocks.clearCoralUISession).not.toHaveBeenCalled()
  })

  it('clears hosted auth state and lands on the signed-out screen', async () => {
    authMocks.coralUIAuthConfig.mockReturnValue(requiredConfig)
    authMocks.readCoralUISession.mockResolvedValue(session)
    authMocks.validateCsrfToken.mockResolvedValue(true)
    authMocks.clearCsrfToken.mockResolvedValue('coral_ui_session_csrf=; Max-Age=0; Path=/')
    authMocks.clearOAuthTransaction.mockResolvedValue('coral_ui_oauth=; Max-Age=0; Path=/')
    authMocks.clearCoralUISession.mockResolvedValue('coral_ui_session=; Max-Age=0; Path=/')
    const request = logoutRequest('valid-token')

    const response = await action(authRouteTestArgs(request, {}))

    expect(response.status).toBe(302)
    expect(response.headers.get('Location')).toBe('/login?signedOut=1')
    expect(response.headers.get('Cache-Control')).toBe('private, no-store')
    expect(response.headers.get('Vary')).toBe('Cookie')
    expect(response.headers.getSetCookie().map(cookieName)).toEqual([
      'coral_ui_session_csrf',
      'coral_ui_oauth',
      'coral_ui_session',
    ])
    expect(response.headers.getSetCookie().every((cookie) => cookie.includes('Max-Age=0'))).toBe(
      true,
    )
    expect(authMocks.validateCsrfToken).toHaveBeenCalledWith(request, requiredConfig, session)
    expect(authMocks.clearCsrfToken).toHaveBeenCalledWith(requiredConfig)
    expect(authMocks.clearOAuthTransaction).toHaveBeenCalledWith(requiredConfig)
    expect(authMocks.clearCoralUISession).toHaveBeenCalledWith(requiredConfig)
  })
})

function logoutRequest(csrf: string): Request {
  return new Request('https://coral-ui.example.test/logout', {
    body: new URLSearchParams({ csrf }),
    method: 'POST',
  })
}

function cookieName(cookie: string): string {
  return cookie.slice(0, cookie.indexOf('='))
}
