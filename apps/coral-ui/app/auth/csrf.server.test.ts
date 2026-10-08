import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { clearCsrfToken, csrfTokenForRequest, validateCsrfToken } from './csrf.server'
import type { AuthSession, RequiredAuthConfig } from './types'

const config: RequiredAuthConfig = {
  cookieName: 'coral_ui_session',
  issuer: 'https://coral.example.test',
  mode: 'required',
  publicUrl: 'https://coral-ui.example.test',
  sessionMaxAgeSeconds: 3600,
  sessionSecret: '0123456789abcdef0123456789abcdef',
}
const session: AuthSession = {
  accessToken: 'first-coral-access-token',
  expiresAt: 4_102_444_800,
  tokenType: 'Bearer',
}
const otherSession: AuthSession = {
  ...session,
  accessToken: 'second-coral-access-token',
}

describe('Coral UI CSRF tokens', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-01-01T00:00:00Z'))
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('creates and reuses a signed, HTTP-only token bound to the active session', async () => {
    const created = await csrfTokenForRequest(
      new Request('https://coral-ui.example.test/'),
      config,
      session,
    )

    expect(created.setCookie).toContain('coral_ui_session_csrf=')
    expect(created.setCookie).not.toContain(session.accessToken)
    expect(created.setCookie).toContain('Secure')
    // The two attributes the name of this test promises and its body did not
    // check. `SameSite` comes along because it is the attribute a CSRF cookie
    // most depends on, and it was asserted nowhere in this file either.
    expect(created.setCookie).toContain('HttpOnly')
    expect(created.setCookie).toContain('SameSite=Lax')

    const reused = await csrfTokenForRequest(requestWithCookie(created.setCookie!), config, session)
    expect(reused).toEqual({ setCookie: null, token: created.token })
  })

  it('rotates a CSRF cookie inherited from a different authenticated session', async () => {
    const created = await csrfTokenForRequest(
      new Request('https://coral-ui.example.test/'),
      config,
      session,
    )

    const rotated = await csrfTokenForRequest(
      requestWithCookie(created.setCookie!),
      config,
      otherSession,
    )

    expect(rotated.token).not.toBe(created.token)
    expect(rotated.setCookie).toContain('coral_ui_session_csrf=')
  })

  it('accepts only a fresh form token bound to the current authenticated session', async () => {
    const created = await csrfTokenForRequest(
      new Request('https://coral-ui.example.test/'),
      config,
      session,
    )

    await expect(
      validateCsrfToken(postRequest(created.setCookie!, created.token), config, session),
    ).resolves.toBe(true)
    await expect(
      validateCsrfToken(postRequest(created.setCookie!, created.token), config, otherSession),
    ).resolves.toBe(false)
    await expect(
      validateCsrfToken(postRequest(created.setCookie!, 'wrong-token'), config, session),
    ).resolves.toBe(false)
    await expect(
      validateCsrfToken(
        postRequest('coral_ui_session_csrf=tampered', created.token),
        config,
        session,
      ),
    ).resolves.toBe(false)

    vi.advanceTimersByTime((config.sessionMaxAgeSeconds + 1) * 1000)
    await expect(
      validateCsrfToken(postRequest(created.setCookie!, created.token), config, session),
    ).resolves.toBe(false)
  })

  it('derives the cookie name and Secure attribute from the auth configuration', async () => {
    const loopbackConfig = {
      ...config,
      cookieName: 'custom_auth',
      issuer: 'http://127.0.0.1:3000',
      publicUrl: 'http://localhost:5173',
    }
    const created = await csrfTokenForRequest(
      new Request('http://localhost:5173/'),
      loopbackConfig,
      session,
    )
    const cleared = await clearCsrfToken(loopbackConfig)

    expect(created.setCookie).toContain('custom_auth_csrf=')
    expect(created.setCookie).not.toContain('Secure')
    expect(cleared).toContain('custom_auth_csrf=')
  })
})

function requestWithCookie(setCookie: string): Request {
  return new Request('https://coral-ui.example.test/', {
    headers: { cookie: setCookie.split(';', 1)[0] },
  })
}

function postRequest(setCookie: string, csrf: string): Request {
  return new Request('https://coral-ui.example.test/logout', {
    body: new URLSearchParams({ csrf }),
    headers: { cookie: setCookie.split(';', 1)[0] },
    method: 'POST',
  })
}
