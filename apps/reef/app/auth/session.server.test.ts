import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import {
  commitOAuthTransaction,
  commitReefSession,
  readOAuthTransaction,
  readReefSession,
} from './session.server'
import type { RequiredAuthConfig } from './types'

const config: RequiredAuthConfig = {
  cookieName: 'reef_session',
  issuer: 'https://coral.example.test',
  mode: 'required',
  publicUrl: 'https://reef.example.test',
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
        accessToken: representativeSessionToken(),
        expiresAt: unixTimestamp() + 1800,
        tokenType: 'Bearer',
      },
      config,
    )

    expect(Buffer.byteLength(cookie)).toBeLessThan(4096)
    await expect(readReefSession(requestWith(cookie), config)).resolves.toMatchObject({
      accessToken: representativeSessionToken(),
    })
  })

  it('stores only PKCE, return path, and state in an OAuth transaction', async () => {
    const transaction = {
      codeVerifier: 'verifier',
      returnTo: '/workspaces/analytics',
      state: 'state',
    }
    const cookie = await commitOAuthTransaction(transaction, config)

    expect(cookie).toContain('reef_oauth=')
    expect(cookie).toContain('Secure')
    await expect(readOAuthTransaction(requestWith(cookie), config)).resolves.toEqual(transaction)
  })

  it('omits Secure only for the accepted all-loopback HTTP topology', async () => {
    const loopbackConfig = {
      ...config,
      issuer: 'http://127.0.0.1:3000',
      publicUrl: 'http://localhost:5173',
    }

    const sessionCookie = await commitReefSession(
      { accessToken: 'token', expiresAt: unixTimestamp() + 1800, tokenType: 'Bearer' },
      loopbackConfig,
    )
    const transactionCookie = await commitOAuthTransaction(
      { codeVerifier: 'verifier', returnTo: '/', state: 'state' },
      loopbackConfig,
    )

    expect(sessionCookie).not.toContain('Secure')
    expect(transactionCookie).not.toContain('Secure')
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

function representativeSessionToken(): string {
  const header = base64UrlJson({
    alg: 'ES256',
    kid: '8n83UnqxWBGpQWJmVpkam9SuO9AmOoa3ik4fdurN7N0',
    typ: 'at+jwt',
  })
  const claims = base64UrlJson({
    aud: 'https://reef.example.test',
    client_id: 'https://reef.example.test/.well-known/oauth-client',
    exp: 1_767_226_800,
    iat: 1_767_225_600,
    iss: 'https://coral.example.test',
    jti: '4b9677e0-2a75-4227-b1e2-45e8584f940e',
    sub: 'opaque:subject/123',
  })
  const signature = 's'.repeat(86)
  return `${header}.${claims}.${signature}`
}

function base64UrlJson(value: unknown): string {
  return Buffer.from(JSON.stringify(value)).toString('base64url')
}
