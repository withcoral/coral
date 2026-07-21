import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { clearCsrfToken, csrfTokenForRequest, validateCsrfToken } from './csrf.server'
import type { RequiredAuthConfig } from './types'

const config: RequiredAuthConfig = {
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

describe('Reef CSRF tokens', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-01-01T00:00:00Z'))
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('creates and reuses a signed, HTTP-only token cookie', async () => {
    const created = await csrfTokenForRequest(new Request('https://reef.example.test/'), config)

    expect(created.token).toBeTruthy()
    expect(created.setCookie).toContain('reef_csrf=')
    expect(created.setCookie).toContain('HttpOnly')
    expect(created.setCookie).toContain('SameSite=Lax')
    expect(created.setCookie).toContain('Secure')

    const reused = await csrfTokenForRequest(requestWithCookie(created.setCookie!), config)
    expect(reused).toEqual({ setCookie: null, token: created.token })
  })

  it('accepts only a fresh form token matching the signed cookie', async () => {
    const created = await csrfTokenForRequest(new Request('https://reef.example.test/'), config)

    await expect(
      validateCsrfToken(postRequest(created.setCookie!, created.token), config),
    ).resolves.toBe(true)
    await expect(
      validateCsrfToken(postRequest(created.setCookie!, 'wrong-token'), config),
    ).resolves.toBe(false)
    await expect(
      validateCsrfToken(postRequest('reef_csrf=tampered', created.token), config),
    ).resolves.toBe(false)

    vi.advanceTimersByTime((config.sessionMaxAgeSeconds + 1) * 1000)
    await expect(
      validateCsrfToken(postRequest(created.setCookie!, created.token), config),
    ).resolves.toBe(false)
  })

  it('clears the token cookie with matching security attributes', async () => {
    const cookie = await clearCsrfToken(config)

    expect(cookie).toContain('reef_csrf=')
    expect(cookie).toContain('Max-Age=0')
    expect(cookie).toContain('HttpOnly')
    expect(cookie).toContain('Secure')
  })
})

function requestWithCookie(setCookie: string): Request {
  return new Request('https://reef.example.test/', {
    headers: { cookie: setCookie.split(';', 1)[0] },
  })
}

function postRequest(setCookie: string, csrf: string): Request {
  return new Request('https://reef.example.test/logout', {
    body: new URLSearchParams({ csrf }),
    headers: { cookie: setCookie.split(';', 1)[0] },
    method: 'POST',
  })
}
