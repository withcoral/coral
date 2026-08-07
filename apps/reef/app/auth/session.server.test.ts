import { createCipheriv, createHash, randomBytes } from 'node:crypto'

import { createCookie } from 'react-router'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import {
  commitOAuthTransaction,
  commitReefSession,
  readOAuthTransaction,
  readReefSession,
} from './session.server'
import type { RequiredAuthConfig } from './types'

// Mirrors CLOCK_SKEW_SECONDS in session.server.ts, which is deliberately not
// exported — the tests below assert the behaviour at its edges, so the number
// has to be stated somewhere they can see it.
const CLOCK_SKEW_SECONDS = 30

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

  // Previously one case named four rejections and demonstrated two of them. It
  // is split up because each of these is a distinct guard that can regress on
  // its own, and because two of the four were not being reached at all: the
  // "tampered" case supplied a wrong *key* rather than altered ciphertext, and
  // the "expired" case used a timestamp inside the clock-skew window, so the
  // skew guard rejected it before the expiry check ever ran.
  it('ignores a request carrying no session cookie', async () => {
    await expect(
      readReefSession(new Request('https://reef.example.test/'), config),
    ).resolves.toBeNull()
  })

  it('ignores a session cookie that is not validly signed', async () => {
    await expect(
      readReefSession(requestWith('reef_session=not-a-session'), config),
    ).resolves.toBeNull()
  })

  // Correctly signed, so the cookie layer hands each of these through to the
  // decryptor rather than turning it away first — the only way to exercise what
  // the decryptor does with a payload it cannot use.
  //
  // These pin the rejection, not the line that performs it: removing the
  // three-part guard in `decryptSession` leaves them all passing, because the
  // surrounding try/catch already rejects the same inputs. That guard is
  // redundancy, and no test can tell it apart from the catch.
  it.each([
    ['a payload that is not three parts', 'aGVsbG8.d29ybGQ'],
    ['a payload with an empty part', 'aGVsbG8..d29ybGQ'],
    ['parts that are not valid ciphertext', 'aGVsbG8.d29ybGQ.YnJva2Vu'],
  ])('ignores %s', async (_label, payload) => {
    await expect(
      readReefSession(requestWith(await signedCookie(payload)), config),
    ).resolves.toBeNull()
  })

  it('ignores a session whose ciphertext was altered under the right key', async () => {
    const cookie = await commitReefSession(validSession(), config)
    const [iv, tag, ciphertext] = cookieValue(cookie).split('.')
    const flipped = ciphertext.slice(0, -1) + (ciphertext.endsWith('A') ? 'B' : 'A')

    await expect(
      readReefSession(requestWith(await signedCookie(`${iv}.${tag}.${flipped}`)), config),
    ).resolves.toBeNull()
  })

  it('ignores a session encrypted under a different secret', async () => {
    const cookie = await commitReefSession(validSession(), config)
    const wrongSecret = { ...config, sessionSecret: 'abcdef0123456789abcdef0123456789' }

    await expect(readReefSession(requestWith(cookie), wrongSecret)).resolves.toBeNull()
  })

  // Decrypts cleanly and is still not a session. Nothing else in this file
  // reaches the shape check, because everything else fails before the plaintext
  // exists.
  it.each([
    ['is missing a token', { expiresAt: 4_102_444_800, tokenType: 'Bearer' }],
    ['carries an empty token', { accessToken: '', expiresAt: 4_102_444_800, tokenType: 'Bearer' }],
    ['carries a non-numeric expiry', { accessToken: 't', expiresAt: 'soon', tokenType: 'Bearer' }],
    ['is missing a token type', { accessToken: 't', expiresAt: 4_102_444_800 }],
    ['is not an object at all', 'a string'],
  ])('ignores a decryptable payload that %s', async (_label, payload) => {
    const cookie = await signedCookie(encryptForTest(payload))

    await expect(readReefSession(requestWith(cookie), config)).resolves.toBeNull()
  })

  it('ignores a session once it has genuinely expired', async () => {
    const cookie = await commitReefSession(validSession(), config)
    await expect(readReefSession(requestWith(cookie), config)).resolves.not.toBeNull()

    // Well past expiry, so the skew window plays no part in the rejection.
    advanceSeconds(1800 + CLOCK_SKEW_SECONDS + 60)
    await expect(readReefSession(requestWith(cookie), config)).resolves.toBeNull()
  })

  it('ignores a session that is still valid but inside the clock-skew window', async () => {
    const cookie = await commitReefSession(validSession(), config)

    // 20s of life left: not expired, but too close to hand to a request.
    advanceSeconds(1800 - 20)
    await expect(readReefSession(requestWith(cookie), config)).resolves.toBeNull()
  })

  // The other half of that guard. A session this short is unreadable the instant
  // it is written, and the resulting `/login` → callback → `/login` loop reports
  // no error anywhere, so the refusal has to happen on the way in.
  it('refuses to commit a session that expires inside the clock-skew window', async () => {
    await expect(
      commitReefSession(
        {
          accessToken: 'token',
          expiresAt: unixTimestamp() + CLOCK_SKEW_SECONDS,
          tokenType: 'Bearer',
        },
        config,
      ),
    ).rejects.toThrow('Refusing to establish a Reef session that expires within 30s')

    await expect(
      commitReefSession(
        { accessToken: 'token', expiresAt: unixTimestamp() - 1, tokenType: 'Bearer' },
        config,
      ),
    ).rejects.toThrow('Refusing to establish a Reef session that expires within 30s')
  })

  it('commits a session that clears the clock-skew window by a second', async () => {
    await expect(
      commitReefSession(
        {
          accessToken: 'token',
          expiresAt: unixTimestamp() + CLOCK_SKEW_SECONDS + 1,
          tokenType: 'Bearer',
        },
        config,
      ),
    ).resolves.toContain('reef_session=')
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

function advanceSeconds(seconds: number): void {
  vi.setSystemTime(new Date(Date.now() + seconds * 1000))
}

function validSession() {
  return {
    accessToken: 'coral-access-token',
    expiresAt: unixTimestamp() + 1800,
    tokenType: 'Bearer',
  }
}

function cookieValue(setCookie: string): string {
  const value = setCookie.split(';', 1)[0].split('=').slice(1).join('=')
  // react-router signs a cookie as `<base64 payload>.<signature>`, and the
  // payload itself is the base64 of what was serialized.
  return Buffer.from(decodeURIComponent(value).split('.')[0], 'base64').toString('utf8')
}

// Signs an arbitrary payload the way the session cookie is signed, so a
// hand-built value reaches the decryptor instead of being turned away by the
// cookie layer. Only `secrets` matters for that.
async function signedCookie(payload: string): Promise<string> {
  return createCookie(config.cookieName, { secrets: [config.sessionSecret] }).serialize(payload)
}

// Mirrors the module's own AES-256-GCM envelope. Reproduced rather than exported
// from the source, because the point is to feed the reader something that
// decrypts perfectly and is still not a session — the one case that cannot be
// built out of the public API.
function encryptForTest(payload: unknown): string {
  const iv = randomBytes(12)
  const key = createHash('sha256').update(config.sessionSecret).digest()
  const cipher = createCipheriv('aes-256-gcm', key, iv)
  const ciphertext = Buffer.concat([cipher.update(JSON.stringify(payload), 'utf8'), cipher.final()])

  return [iv, cipher.getAuthTag(), ciphertext].map((part) => part.toString('base64url')).join('.')
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
