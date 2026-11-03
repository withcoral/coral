import { createCookie } from 'react-router'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { authClientId } from './config.server'
import {
  commitCoralWebSession,
  commitOAuthTransaction,
  finishPendingSignOuts,
  oauthCookieName,
  readOAuthTransaction,
  resolveCoralWebSession,
  signOutCoralWebSession,
} from './session.server'
import { loopbackAuthConfig as loopbackConfig, requiredAuthConfig } from './test-config'

// Mirrors CLOCK_SKEW_SECONDS and OAUTH_MAX_AGE_SECONDS in session.server.ts,
// which are deliberately not exported — the tests below assert the behaviour at
// their edges, so the numbers have to be stated somewhere they can see them.
const CLOCK_SKEW_SECONDS = 30
const OAUTH_MAX_AGE_SECONDS = 10 * 60

const config = requiredAuthConfig
const ENDPOINT = `${config.issuer}/oauth/session`
const HANDLE = 'coral_ws_handle'

describe('Coral web auth sessions', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-01-01T00:00:00Z'))
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.unstubAllGlobals()
  })

  it('keeps only the session handle in a secure cookie and resolves it through Coral', async () => {
    const cookie = await startedSession()
    expect(cookie).toContain('__Host-coral_web_session=')
    expect(cookie).toContain('HttpOnly')
    expect(cookie).toContain('SameSite=Lax')
    expect(cookie).toContain('Secure')
    // The cookie outlives the session, so its end reaches the reader as a refusal.
    expect(cookie).toContain('Max-Age=3900')
    expect(JSON.parse(cookieValue(cookie))).toEqual({
      endpoint: ENDPOINT,
      endsAt: unixTimestamp() + 3600,
      handle: HANDLE,
    })

    const fetch = mockFetch(minted())
    await expect(resolveCoralWebSession(requestWith(cookie), config)).resolves.toEqual({
      accessToken: 'minted-access-token',
      expiresAt: unixTimestamp() + 300,
      handle: HANDLE,
      sessionExpiresAt: unixTimestamp() + 1800,
    })
    const [url, init] = fetch.mock.calls[0] as [string, RequestInit]
    expect(url).toBe(`${ENDPOINT}/token`)
    expect(Object.fromEntries(init.body as URLSearchParams)).toEqual({
      client_id: authClientId(config),
      session: HANDLE,
    })
    expect(init.redirect).toBe('manual')
  })

  // Every protected request resolves again, so a renewed access token reaches
  // the next request without the cookie changing at all.
  it('gets a fresh access token on each request from the same cookie', async () => {
    const cookie = await startedSession()
    mockFetch(minted('first-token'), minted('second-token'))

    const first = await resolveCoralWebSession(requestWith(cookie), config)
    advanceSeconds(400)
    const second = await resolveCoralWebSession(requestWith(cookie), config)

    expect(first?.accessToken).toBe('first-token')
    expect(second?.accessToken).toBe('second-token')
    expect(second?.handle).toBe(first?.handle)
  })

  it.each([
    ['carries no session cookie', () => new Request('https://coral-web.example.test/')],
    [
      'carries a session cookie that is not validly signed',
      () => requestWith(`${config.cookieName}=not-a-session`),
    ],
    [
      'names a session endpoint off the issuer',
      async () =>
        requestWith(
          await signedSessionCookie({
            endpoint: 'https://attacker.test/session',
            endsAt: 4_102_444_800,
            handle: HANDLE,
          }),
        ),
    ],
    [
      'carries an end that is not a time',
      async () =>
        requestWith(
          await signedSessionCookie({ endpoint: ENDPOINT, endsAt: 'soon', handle: HANDLE }),
        ),
    ],
    [
      'carries no handle',
      async () => requestWith(await signedSessionCookie({ endpoint: ENDPOINT })),
    ],
  ])('has no session, and asks Coral nothing, when the request %s', async (_label, request) => {
    const fetch = mockFetch()

    await expect(resolveCoralWebSession(await request(), config)).resolves.toBeNull()
    expect(fetch).not.toHaveBeenCalled()
  })

  // Coral alone decides when a session ends: a session with a second left is
  // still a session, and one Coral refuses is over.
  it('keeps a session until Coral refuses it', async () => {
    const cookie = await startedSession()
    mockFetch(minted('last-token', 1), jsonResponse({ error: 'invalid_grant' }, 400))

    await expect(resolveCoralWebSession(requestWith(cookie), config)).resolves.toMatchObject({
      accessToken: 'last-token',
      sessionExpiresAt: unixTimestamp() + 1,
    })
    await expect(resolveCoralWebSession(requestWith(cookie), config)).resolves.toBeNull()
  })

  // A failure that says nothing about the session leaves it in place: the
  // caller answers 503 rather than signing the visitor out.
  it.each([
    ['Coral is unavailable', () => Promise.resolve(jsonResponse({ error: 'server_error' }, 503))],
    [
      'Coral rejects the request itself',
      () => Promise.resolve(jsonResponse({ error: 'invalid_request' }, 400)),
    ],
    ['the answer is not a token', () => Promise.resolve(jsonResponse({ access_token: '' }))],
    ['Coral cannot be reached', () => Promise.reject(new TypeError('fetch failed'))],
  ])('reports a temporary failure when %s', async (_label, response) => {
    const cookie = await startedSession()
    mockFetch(response)

    const failure = await resolveCoralWebSession(requestWith(cookie), config).catch(
      (error: unknown) => error,
    )
    expect(failure).toBeInstanceOf(Response)
    expect((failure as Response).status).toBe(503)
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

    expect(cookie).toContain('__Host-coral_web_oauth=')
    expect(cookie).toContain('Secure')
    await expect(readOAuthTransaction(requestWith(cookie), config)).resolves.toEqual(transaction)
  })

  // The prefix is only meaningful alongside the `Path=/` and absent `Domain`
  // that this cookie already had, so all three are pinned together.
  it('names the OAuth transaction cookie so a sibling host cannot toss one', async () => {
    const cookie = await commitOAuthTransaction(validTransaction(), config)

    expect(cookie).toContain('__Host-coral_web_oauth=')
    expect(cookie).toContain('Path=/')
    expect(cookie).not.toContain('Domain=')
  })

  // `__Host-` requires `Secure`, and a browser discards a cookie carrying the
  // prefix without it — so over loopback HTTP the prefix would cost every login
  // rather than protect it.
  it('drops the __Host- prefix where the browser would refuse it', async () => {
    const cookie = await commitOAuthTransaction(validTransaction(), loopbackConfig)

    expect(cookie).toContain('coral_web_oauth=')
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
    const sessionCookie = await commitCoralWebSession(
      {
        endpoint: `${loopbackConfig.issuer}/oauth/session`,
        expiresIn: 1800,
        handle: HANDLE,
      },
      loopbackConfig,
    )
    const transactionCookie = await commitOAuthTransaction(
      { codeVerifier: 'verifier', returnTo: '/', state: 'state' },
      loopbackConfig,
    )

    expect(sessionCookie).not.toContain('Secure')
    expect(transactionCookie).not.toContain('Secure')
  })

  // A session this short is unusable the instant it is written, and the
  // resulting `/login` → callback → `/login` loop reports no error anywhere, so
  // the refusal has to happen on the way in.
  it('refuses to commit a session that ends inside the clock-skew window', async () => {
    for (const expiresIn of [CLOCK_SKEW_SECONDS, 0]) {
      await expect(
        commitCoralWebSession({ endpoint: ENDPOINT, expiresIn, handle: HANDLE }, config),
      ).rejects.toThrow('Refusing to establish a Coral web session that expires within 30s')
    }
    await expect(
      commitCoralWebSession(
        { endpoint: ENDPOINT, expiresIn: CLOCK_SKEW_SECONDS + 1, handle: HANDLE },
        config,
      ),
    ).resolves.toContain('Max-Age=331')
  })

  it('signs out at Coral and clears the session cookie', async () => {
    const cookie = await startedSession()
    const fetch = mockFetch(new Response(null, { status: 200 }))

    const signOut = await signOutCoralWebSession(requestWith(cookie), config)

    expect(signOut.unfinished).toBe(false)
    expect(endedHandles(fetch)).toEqual([HANDLE])
    expect(signOut.setCookies.map(cookieName).toSorted()).toEqual(
      [config.cookieName, `${config.cookieName}_signout`].toSorted(),
    )
    for (const set of signOut.setCookies) expect(set).toContain('Max-Age=0')
  })

  // Catches a retry that reads the newer session cookie and signs the reader
  // out of a sign-in they just made, leaving the old session alive.
  it('keeps an unanswered sign-out and later finishes only that one', async () => {
    const old = await startedSession()
    mockFetch(jsonResponse({ error: 'server_error' }, 503))
    const signOut = await signOutCoralWebSession(requestWith(old), config)
    expect(signOut.unfinished).toBe(true)
    const pending = signOut.setCookies.find(
      (set) => cookieName(set) === `${config.cookieName}_signout`,
    )!
    // The pending cookie lasts no longer than the session it would end.
    expect(pending).toContain('Max-Age=3600')

    const newer = await commitCoralWebSession(
      { endpoint: ENDPOINT, expiresIn: 3600, handle: 'coral_ws_newer' },
      config,
    )
    const fetch = mockFetch(new Response(null, { status: 200 }))
    const finished = await finishPendingSignOuts(requestWith(newer, pending), config)

    expect(endedHandles(fetch)).toEqual([HANDLE])
    expect(finished).toEqual({ setCookie: expect.stringContaining('Max-Age=0'), unfinished: false })
    mockFetch(minted())
    await expect(
      resolveCoralWebSession(requestWith(newer, pending), config),
    ).resolves.toMatchObject({
      handle: 'coral_ws_newer',
    })
  })

  it('drops a pending sign-out once its session has ended on its own', async () => {
    mockFetch(jsonResponse({ error: 'server_error' }, 503))
    const signOut = await signOutCoralWebSession(requestWith(await startedSession()), config)
    const pending = signOut.setCookies.find(
      (set) => cookieName(set) === `${config.cookieName}_signout`,
    )!

    advanceSeconds(3600)
    const fetch = mockFetch()
    await expect(finishPendingSignOuts(requestWith(pending), config)).resolves.toEqual({
      setCookie: expect.stringContaining('Max-Age=0'),
      unfinished: false,
    })
    expect(fetch).not.toHaveBeenCalled()
    await expect(
      finishPendingSignOuts(new Request('https://coral-web.example.test/'), config),
    ).resolves.toBeNull()
  })
})

function requestWith(...setCookies: string[]): Request {
  return new Request('https://coral-web.example.test/', {
    headers: { cookie: setCookies.map((set) => set.split(';', 1)[0]).join('; ') },
  })
}

function cookieName(setCookie: string): string {
  return setCookie.slice(0, setCookie.indexOf('='))
}

/** The session handles each fetch asked Coral to end, in order. */
function endedHandles(fetch: ReturnType<typeof mockFetch>): string[] {
  return (fetch.mock.calls as [string, RequestInit][]).map(([url, init]) => {
    expect(url).toBe(`${ENDPOINT}/end`)
    expect((init.body as URLSearchParams).get('client_id')).toBe(authClientId(config))
    return (init.body as URLSearchParams).get('session') ?? ''
  })
}

function unixTimestamp(): number {
  return Math.floor(Date.now() / 1000)
}

function advanceSeconds(seconds: number): void {
  vi.setSystemTime(new Date(Date.now() + seconds * 1000))
}

function startedSession(): Promise<string> {
  return commitCoralWebSession({ endpoint: ENDPOINT, expiresIn: 3600, handle: HANDLE }, config)
}

function minted(accessToken = 'minted-access-token', sessionExpiresIn = 1800): Response {
  return jsonResponse({
    access_token: accessToken,
    expires_in: 300,
    session_expires_in: sessionExpiresIn,
    token_type: 'Bearer',
  })
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    headers: { 'content-type': 'application/json' },
    status,
  })
}

/** Answers each fetch with the next response; a function runs when its fetch does. */
function mockFetch(...responses: (Response | (() => Promise<Response>))[]) {
  const fetch = vi.fn<typeof globalThis.fetch>()
  for (const response of responses) {
    fetch.mockImplementationOnce(() =>
      typeof response === 'function' ? response() : Promise.resolve(response),
    )
  }
  vi.stubGlobal('fetch', fetch)
  return fetch
}

function validTransaction() {
  return { codeVerifier: 'verifier', returnTo: '/workspaces/analytics', state: 'state' }
}

// Signs an arbitrary payload under the transaction cookie's name and secret, so
// a hand-built value reaches the shape and age checks instead of being turned
// away by the cookie layer. The name is asked for rather than written out
// because it moves with `CORAL_WEB_PUBLIC_URL`.
async function signedOAuthCookie(payload: unknown): Promise<string> {
  return createCookie(oauthCookieName(config), {
    secrets: [config.sessionSecret],
    secure: true,
  }).serialize(payload)
}

// Signs an arbitrary payload the way the session cookie is signed, so a
// hand-built value reaches the shape check instead of being turned away by the
// cookie layer.
async function signedSessionCookie(payload: unknown): Promise<string> {
  return createCookie(config.cookieName, {
    secrets: [config.sessionSecret],
    secure: true,
  }).serialize(payload)
}

function cookieValue(setCookie: string): string {
  const value = setCookie.split(';', 1)[0].split('=').slice(1).join('=')
  // react-router signs a cookie as `<base64 payload>.<signature>`, and the
  // payload itself is the base64 of what was serialized.
  return Buffer.from(decodeURIComponent(value).split('.')[0], 'base64').toString('utf8')
}
