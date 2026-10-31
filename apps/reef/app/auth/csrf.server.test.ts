import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { clearCsrfToken, csrfTokenForRequest, validateCsrfToken } from './csrf.server'
import type { AuthSession } from './types'
import { requiredAuthConfig } from './test-config'

const config = requiredAuthConfig
const session: AuthSession = {
  accessToken: 'first-coral-access-token',
  expiresAt: 4_102_444_800,
  tokenType: 'Bearer',
}
const otherSession: AuthSession = {
  ...session,
  accessToken: 'second-coral-access-token',
}

describe('Coral web CSRF tokens', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-01-01T00:00:00Z'))
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('creates and reuses a signed, HTTP-only token bound to the active session', async () => {
    const created = await csrfTokenForRequest(
      new Request('https://coral-web.example.test/'),
      config,
      session,
    )

    // The CSRF cookie inherits the session cookie's `__Host-` binding by name.
    expect(created.setCookie).toContain('__Host-coral_web_session_csrf=')
    expect(created.setCookie).not.toContain(session.accessToken)
    expect(created.setCookie).toContain('Secure')
    // The two attributes the name of this test promises and its body did not
    // check. `SameSite` comes along because it is the attribute a CSRF cookie
    // most depends on, and it was asserted nowhere in this file either.
    expect(created.setCookie).toContain('HttpOnly')
    expect(created.setCookie).toContain('SameSite=Lax')

    const reused = await csrfTokenForRequest(requestWithCookie(created.setCookie!), config, session)
    expect(reused.setCookie).toBeNull()
    await expect(
      validateCsrfToken(postRequest(created.setCookie!, reused.token), config, session),
    ).resolves.toBe(true)
  })

  it('rotates a CSRF cookie inherited from a different authenticated session', async () => {
    const created = await csrfTokenForRequest(
      new Request('https://coral-web.example.test/'),
      config,
      session,
    )

    const rotated = await csrfTokenForRequest(
      requestWithCookie(created.setCookie!),
      config,
      otherSession,
    )

    expect(rotated.setCookie).toContain(`${config.cookieName}_csrf=`)
    await expect(
      validateCsrfToken(postRequest(rotated.setCookie!, rotated.token), config, otherSession),
    ).resolves.toBe(true)
    await expect(
      validateCsrfToken(postRequest(rotated.setCookie!, created.token), config, otherSession),
    ).resolves.toBe(false)
  })

  it('accepts only a fresh form token bound to the current authenticated session', async () => {
    const created = await csrfTokenForRequest(
      new Request('https://coral-web.example.test/'),
      config,
      session,
    )

    const another = await csrfTokenForRequest(
      new Request('https://coral-web.example.test/'),
      config,
      session,
    )
    const masked = created.token
    await expect(
      validateCsrfToken(postRequest(created.setCookie!, masked), config, session),
    ).resolves.toBe(true)
    await expect(
      validateCsrfToken(postRequest(created.setCookie!, masked), config, otherSession),
    ).resolves.toBe(false)
    await expect(
      validateCsrfToken(postRequest(created.setCookie!, another.token), config, session),
    ).resolves.toBe(false)
    await expect(
      validateCsrfToken(postRequest(`${config.cookieName}_csrf=tampered`, masked), config, session),
    ).resolves.toBe(false)

    vi.advanceTimersByTime((config.sessionMaxAgeSeconds + 1) * 1000)
    await expect(
      validateCsrfToken(postRequest(created.setCookie!, masked), config, session),
    ).resolves.toBe(false)
  })

  // Catches: a token that repeats in compressed pages, whose sizes can reveal
  // it beside reflected input (BREACH), or a mask that stops it validating.
  it('masks the token differently in each response', async () => {
    const created = await csrfTokenForRequest(
      new Request('https://coral-web.example.test/'),
      config,
      session,
    )
    const repeat = () => csrfTokenForRequest(requestWithCookie(created.setCookie!), config, session)
    const [first, second] = [(await repeat()).token, (await repeat()).token]
    expect(first).not.toBe(second)
    for (const masked of [first, second]) {
      await expect(
        validateCsrfToken(postRequest(created.setCookie!, masked), config, session),
      ).resolves.toBe(true)
    }
    for (const invalid of ['unmasked-token', 'a.b.c', `${first}x`, '.', '']) {
      await expect(
        validateCsrfToken(postRequest(created.setCookie!, invalid), config, session),
      ).resolves.toBe(false)
    }
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
  return new Request('https://coral-web.example.test/', {
    headers: { cookie: setCookie.split(';', 1)[0] },
  })
}

function postRequest(setCookie: string, csrf: string): Request {
  return new Request('https://coral-web.example.test/logout', {
    body: new URLSearchParams({ csrf }),
    headers: { cookie: setCookie.split(';', 1)[0] },
    method: 'POST',
  })
}
