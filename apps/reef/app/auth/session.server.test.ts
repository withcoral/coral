import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { commitReefSession, readReefSession } from './session.server'
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

describe('Reef auth sessions', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-01-01T00:00:00Z'))
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('round trips an encrypted server session with secure cookie attributes', async () => {
    const session = {
      accessToken: 'coral-access-token',
      expiresAt: unixTimestamp() + 1800,
      tokenType: 'Bearer',
    }
    const cookie = await commitReefSession(session, config)

    expect(cookie).toContain('reef_session=')
    expect(cookie).not.toContain('coral-access-token')
    expect(cookie).toContain('HttpOnly')
    expect(cookie).toContain('SameSite=Lax')
    expect(cookie).toContain('Secure')
    await expect(readReefSession(requestWith(cookie), config)).resolves.toEqual(session)
  })

  it('keeps a representative opaque access token below the cookie size limit', async () => {
    const cookie = await commitReefSession(
      {
        accessToken: 'a'.repeat(1500),
        expiresAt: unixTimestamp() + 1800,
        tokenType: 'Bearer',
      },
      config,
    )

    expect(Buffer.byteLength(cookie)).toBeLessThan(4096)
    await expect(readReefSession(requestWith(cookie), config)).resolves.toMatchObject({
      accessToken: 'a'.repeat(1500),
    })
  })

  it('ignores absent, malformed, tampered, and expired sessions', async () => {
    await expect(
      readReefSession(new Request('https://reef.example.test/'), config),
    ).resolves.toBeNull()
    await expect(
      readReefSession(requestWith('reef_session=not-a-session'), config),
    ).resolves.toBeNull()

    const cookie = await commitReefSession(
      { accessToken: 'token', expiresAt: unixTimestamp() + 20, tokenType: 'Bearer' },
      config,
    )
    await expect(readReefSession(requestWith(cookie), config)).resolves.toBeNull()

    const wrongSecret = { ...config, sessionSecret: 'abcdef0123456789abcdef0123456789' }
    await expect(readReefSession(requestWith(cookie), wrongSecret)).resolves.toBeNull()
  })
})

function requestWith(setCookie: string): Request {
  return new Request('https://reef.example.test/', {
    headers: { cookie: setCookie.split(';', 1)[0] },
  })
}

function unixTimestamp(): number {
  return Math.floor(Date.now() / 1000)
}
