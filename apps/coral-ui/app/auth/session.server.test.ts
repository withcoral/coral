import { createCipheriv, createHash, randomBytes } from 'node:crypto'

import { createCookie } from 'react-router'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import {
  commitOAuthTransaction,
  commitCoralUISession,
  oauthCookieName,
  readOAuthTransaction,
  readCoralUISession,
} from './session.server'
import type { RequiredAuthConfig } from './types'

// Mirrors CLOCK_SKEW_SECONDS and OAUTH_MAX_AGE_SECONDS in session.server.ts,
// which are deliberately not exported — the tests below assert the behaviour at
// their edges, so the numbers have to be stated somewhere they can see them.
const CLOCK_SKEW_SECONDS = 30
const OAUTH_MAX_AGE_SECONDS = 10 * 60

const config: RequiredAuthConfig = {
  cookieName: 'coral_ui_session',
  issuer: 'https://coral.example.test',
  mode: 'required',
  publicUrl: 'https://coral-ui.example.test',
  sessionMaxAgeSeconds: 3600,
  sessionSecret: '0123456789abcdef0123456789abcdef',
}

// The one topology that serves Coral UI over HTTP: loopback throughout, so neither
// cookie can carry `Secure` and the transaction cookie cannot carry `__Host-`.
const loopbackConfig: RequiredAuthConfig = {
  ...config,
  issuer: 'http://127.0.0.1:3000',
  publicUrl: 'http://localhost:5173',
}

describe('Coral UI auth sessions', () => {
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
    const cookie = await commitCoralUISession(session, config)

    expect(cookie).toContain('coral_ui_session=')
    expect(cookie).not.toContain('coral-access-token')
    expect(cookie).toContain('HttpOnly')
    expect(cookie).toContain('SameSite=Lax')
    expect(cookie).toContain('Secure')
    await expect(readCoralUISession(requestWith(cookie), config)).resolves.toEqual(session)
  })

  it('keeps a representative opaque access token below the cookie size limit', async () => {
    const cookie = await commitCoralUISession(
      {
        accessToken: representativeSessionToken(),
        expiresAt: unixTimestamp() + 1800,
        tokenType: 'Bearer',
      },
      config,
    )

    expect(Buffer.byteLength(cookie)).toBeLessThan(4096)
    await expect(readCoralUISession(requestWith(cookie), config)).resolves.toMatchObject({
      accessToken: representativeSessionToken(),
    })
  })

  // `toEqual` and not `toMatchObject`: the reader is also expected to strip the
  // `issuedAt` it stamps on the way in, so a caller sees the three fields it
  // committed and nothing else.
  it('stores only PKCE, return path, and state in an OAuth transaction', async () => {
    const transaction = {
      codeVerifier: 'verifier',
      returnTo: '/workspaces/analytics',
      state: 'state',
    }
    const cookie = await commitOAuthTransaction(transaction, config)

    expect(cookie).toContain('__Host-coral_ui_oauth=')
    expect(cookie).toContain('Secure')
    await expect(readOAuthTransaction(requestWith(cookie), config)).resolves.toEqual(transaction)
  })

  // The prefix is only meaningful alongside the `Path=/` and absent `Domain`
  // that this cookie already had, so all three are pinned together.
  it('names the OAuth transaction cookie so a sibling host cannot toss one', async () => {
    const cookie = await commitOAuthTransaction(validTransaction(), config)

    expect(cookie).toContain('__Host-coral_ui_oauth=')
    expect(cookie).toContain('Path=/')
    expect(cookie).not.toContain('Domain=')
  })

  // `__Host-` requires `Secure`, and a browser discards a cookie carrying the
  // prefix without it — so over loopback HTTP the prefix would cost every login
  // rather than protect it.
  it('drops the __Host- prefix where the browser would refuse it', async () => {
    const cookie = await commitOAuthTransaction(validTransaction(), loopbackConfig)

    expect(cookie).toContain('coral_ui_oauth=')
    expect(cookie).not.toContain('__Host-')
    await expect(readOAuthTransaction(requestWith(cookie), loopbackConfig)).resolves.toEqual(
      validTransaction(),
    )
  })

  // `Max-Age` bounds the cookie in a browser and nowhere else. These two pin the
  // server-side bound that makes a captured transaction stop working.
  it('ignores an OAuth transaction issued outside its lifetime', async () => {
    const cookie = await commitOAuthTransaction(validTransaction(), config)
    await expect(readOAuthTransaction(requestWith(cookie), config)).resolves.not.toBeNull()

    advanceSeconds(OAUTH_MAX_AGE_SECONDS + CLOCK_SKEW_SECONDS + 1)
    await expect(readOAuthTransaction(requestWith(cookie), config)).resolves.toBeNull()
  })

  it('still reads an OAuth transaction inside the skew slack', async () => {
    const cookie = await commitOAuthTransaction(validTransaction(), config)

    advanceSeconds(OAUTH_MAX_AGE_SECONDS + CLOCK_SKEW_SECONDS)
    await expect(readOAuthTransaction(requestWith(cookie), config)).resolves.toEqual(
      validTransaction(),
    )
  })

  // A clock that runs ahead of the one that wrote the cookie is the only thing
  // that produces this, since the value cannot be forged. Rejecting it would
  // break those logins and stop no attack.
  it('reads an OAuth transaction dated in the future', async () => {
    const cookie = await commitOAuthTransaction(validTransaction(), config)

    advanceSeconds(-300)
    await expect(readOAuthTransaction(requestWith(cookie), config)).resolves.toEqual(
      validTransaction(),
    )
  })

  // Correctly signed and correct in every field the callback reads, so only the
  // missing timestamp turns it away. This is also the shape written by any
  // version of this module before `issuedAt` existed: an in-flight login across
  // the deploy fails once and works on retry.
  it('ignores an OAuth transaction carrying no issue time', async () => {
    const cookie = await signedOAuthCookie(validTransaction())

    await expect(readOAuthTransaction(requestWith(cookie), config)).resolves.toBeNull()
  })

  // `Number.isFinite` in the guard is deliberately not among these: JSON has no
  // infinity, so no signed cookie can carry one. It is redundancy of the same
  // kind as the three-part check in `decryptSession`, and no test reaches it.
  it.each([
    ['a non-numeric issue time', { ...validTransaction(), issuedAt: 'recently' }],
    ['no state', { codeVerifier: 'verifier', issuedAt: 0, returnTo: '/' }],
    ['no transaction at all', 'a string'],
  ])('ignores an OAuth transaction with %s', async (_label, payload) => {
    const cookie = await signedOAuthCookie(payload)

    await expect(readOAuthTransaction(requestWith(cookie), config)).resolves.toBeNull()
  })

  it('omits Secure only for the accepted all-loopback HTTP topology', async () => {
    const sessionCookie = await commitCoralUISession(
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
      readCoralUISession(new Request('https://coral-ui.example.test/'), config),
    ).resolves.toBeNull()
  })

  it('ignores a session cookie that is not validly signed', async () => {
    await expect(
      readCoralUISession(requestWith('coral_ui_session=not-a-session'), config),
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
      readCoralUISession(requestWith(await signedCookie(payload)), config),
    ).resolves.toBeNull()
  })

  it('ignores a session whose ciphertext was altered under the right key', async () => {
    const cookie = await commitCoralUISession(validSession(), config)
    const [iv, tag, ciphertext] = cookieValue(cookie).split('.')
    const flipped = ciphertext.slice(0, -1) + (ciphertext.endsWith('A') ? 'B' : 'A')

    await expect(
      readCoralUISession(requestWith(await signedCookie(`${iv}.${tag}.${flipped}`)), config),
    ).resolves.toBeNull()
  })

  it('ignores a session encrypted under a different secret', async () => {
    const cookie = await commitCoralUISession(validSession(), config)
    const wrongSecret = { ...config, sessionSecret: 'abcdef0123456789abcdef0123456789' }

    await expect(readCoralUISession(requestWith(cookie), wrongSecret)).resolves.toBeNull()
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

    await expect(readCoralUISession(requestWith(cookie), config)).resolves.toBeNull()
  })

  it('ignores a session once it has genuinely expired', async () => {
    const cookie = await commitCoralUISession(validSession(), config)
    await expect(readCoralUISession(requestWith(cookie), config)).resolves.not.toBeNull()

    // Well past expiry, so the skew window plays no part in the rejection.
    advanceSeconds(1800 + CLOCK_SKEW_SECONDS + 60)
    await expect(readCoralUISession(requestWith(cookie), config)).resolves.toBeNull()
  })

  it('ignores a session that is still valid but inside the clock-skew window', async () => {
    const cookie = await commitCoralUISession(validSession(), config)

    // 20s of life left: not expired, but too close to hand to a request.
    advanceSeconds(1800 - 20)
    await expect(readCoralUISession(requestWith(cookie), config)).resolves.toBeNull()
  })

  // The other half of that guard. A session this short is unreadable the instant
  // it is written, and the resulting `/login` → callback → `/login` loop reports
  // no error anywhere, so the refusal has to happen on the way in.
  it('refuses to commit a session that expires inside the clock-skew window', async () => {
    await expect(
      commitCoralUISession(
        {
          accessToken: 'token',
          expiresAt: unixTimestamp() + CLOCK_SKEW_SECONDS,
          tokenType: 'Bearer',
        },
        config,
      ),
    ).rejects.toThrow('Refusing to establish a Coral UI session that expires within 30s')

    await expect(
      commitCoralUISession(
        { accessToken: 'token', expiresAt: unixTimestamp() - 1, tokenType: 'Bearer' },
        config,
      ),
    ).rejects.toThrow('Refusing to establish a Coral UI session that expires within 30s')
  })

  it('commits a session that clears the clock-skew window by a second', async () => {
    await expect(
      commitCoralUISession(
        {
          accessToken: 'token',
          expiresAt: unixTimestamp() + CLOCK_SKEW_SECONDS + 1,
          tokenType: 'Bearer',
        },
        config,
      ),
    ).resolves.toContain('coral_ui_session=')
  })
})

function requestWith(setCookie: string): Request {
  return new Request('https://coral-ui.example.test/', {
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

function validTransaction() {
  return { codeVerifier: 'verifier', returnTo: '/workspaces/analytics', state: 'state' }
}

// Signs an arbitrary payload under the transaction cookie's name and secret, so
// a hand-built value reaches the shape and age checks instead of being turned
// away by the cookie layer. The name is asked for rather than written out
// because it moves with `CORAL_UI_PUBLIC_URL`.
async function signedOAuthCookie(payload: unknown): Promise<string> {
  return createCookie(oauthCookieName(config), { secrets: [config.sessionSecret] }).serialize(
    payload,
  )
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
    aud: 'https://coral-ui.example.test',
    client_id: 'https://coral-ui.example.test/.well-known/oauth-client',
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
