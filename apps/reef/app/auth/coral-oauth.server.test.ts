import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { authClientId, authRedirectUri, authResource } from './config.server'
import { completeCoralOAuthLogin, startCoralOAuthLogin } from './coral-oauth.server'
import { coralWebSessionHandle, oauthCookieName, readOAuthTransaction } from './session.server'
import { loopbackAuthConfig, requiredAuthConfig } from './test-config'

const config = requiredAuthConfig
const AUTH_ISSUER = config.issuer
const metadata = {
  authorization_endpoint: `${AUTH_ISSUER}/authorize`,
  coral_web_session_endpoint: `${AUTH_ISSUER}/oauth/session`,
  issuer: AUTH_ISSUER,
  token_endpoint: `${AUTH_ISSUER}/token`,
}
const tokens = { access_token: 'opaque-coral-token', expires_in: 7200, refresh_token: 'coral_rt_r' }
const startedSession = { expires_in: 3600, session: 'coral_ws_handle' }
const untrustedEndpointCases = [
  ['lookalike host', 'https://coral.example.test.attacker.test/oauth'],
  ['alternate port', 'https://coral.example.test:444/oauth'],
  ['scheme change', 'http://coral.example.test/oauth'],
  ['credentials', 'https://user:password@coral.example.test/oauth'],
  ['relative URL', '/oauth'],
  ['malformed URL', 'not a URL'],
  ['non-HTTP URL', 'ftp://coral.example.test/oauth'],
] as const

describe('Coral OAuth adapter', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-01-01T00:00:00Z'))
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.unstubAllGlobals()
  })

  it('starts login with PKCE and identity derived only from the Coral web public URL', async () => {
    const fetch = mockFetch(jsonResponse(metadata))
    const response = await startCoralOAuthLogin(
      new Request('https://internal-proxy/login?returnTo=/workspaces/analytics?tab=mine'),
      config,
    )
    const location = new URL(response.headers.get('location') ?? '')
    const transaction = await readOAuthTransaction(
      new Request('https://coral-web.example.test/', {
        headers: { cookie: cookieHeader(response, oauthCookieName(config)) },
      }),
      config,
    )

    expect(fetch).toHaveBeenCalledWith(`${AUTH_ISSUER}/.well-known/oauth-authorization-server`, {
      headers: { accept: 'application/json' },
    })
    expect(fetch).toHaveBeenCalledTimes(1)
    expect(location.origin).toBe(AUTH_ISSUER)
    expect(location.searchParams.get('client_id')).toBe(authClientId(config))
    expect(location.searchParams.get('redirect_uri')).toBe(authRedirectUri(config))
    expect(location.searchParams.get('resource')).toBe(authResource(config))
    expect(location.searchParams.has('scope')).toBe(false)
    expect(location.searchParams.get('state')).toBeTruthy()
    expect(location.searchParams.get('code_challenge')).toBeTruthy()
    expect(location.searchParams.get('code_challenge_method')).toBe('S256')
    expect(transaction).toEqual({
      codeVerifier: expect.any(String),
      returnTo: '/workspaces/analytics?tab=mine',
      state: location.searchParams.get('state'),
    })
  })

  it('completes login, hands the refresh token to a Coral session, and keeps only its handle', async () => {
    const fetch = mockFetch(
      jsonResponse(metadata),
      jsonResponse(metadata),
      jsonResponse(tokens),
      jsonResponse(startedSession, { status: 201 }),
    )
    const login = await startCoralOAuthLogin(
      new Request('https://coral-web.example.test/login?returnTo=/workspaces/analytics?tab=mine'),
      config,
    )
    const state = new URL(login.headers.get('location') ?? '').searchParams.get('state')
    const callback = await completeCoralOAuthLogin(
      new Request(`https://internal-proxy/auth/callback?code=abc123&state=${state}`, {
        headers: { cookie: cookieHeader(login, oauthCookieName(config)) },
      }),
      config,
    )
    const cookies = setCookies(callback)
    const handle = await coralWebSessionHandle(
      new Request('https://coral-web.example.test/', {
        headers: { cookie: cookieHeader(callback, config.cookieName) },
      }),
      config,
    )

    expect(callback.headers.get('location')).toBe('/workspaces/analytics?tab=mine')
    expect(cookies[0]).toContain(`${oauthCookieName(config)}=`)
    expect(cookies[0]).toContain('Max-Age=0')
    expect(cookies[1]).toContain(`${config.cookieName}=`)
    expect(handle).toBe('coral_ws_handle')
    for (const secret of ['opaque-coral-token', 'coral_rt_r']) {
      expect(decodedCookies(callback)).not.toContain(secret)
    }

    const tokenRequest = fetch.mock.calls[2]
    const body = tokenRequest?.[1]?.body as URLSearchParams
    expect(tokenRequest?.[0]).toBe(`${AUTH_ISSUER}/token`)
    expect(body.get('grant_type')).toBe('authorization_code')
    expect(body.get('code')).toBe('abc123')
    expect(body.get('code_verifier')).toBeTruthy()
    expect(body.get('client_id')).toBe(authClientId(config))
    expect(body.get('redirect_uri')).toBe(authRedirectUri(config))
    expect(body.get('resource')).toBe(authResource(config))
    expect(body.get('coral_audience')).toBe('reef')
    expect(body.has('scope')).toBe(false)

    const sessionRequest = fetch.mock.calls[3]
    expect(sessionRequest?.[0]).toBe(metadata.coral_web_session_endpoint)
    expect(Object.fromEntries(sessionRequest?.[1]?.body as URLSearchParams)).toEqual({
      client_id: authClientId(config),
      max_age: String(config.sessionMaxAgeSeconds),
      refresh_token: 'coral_rt_r',
    })
    expect(sessionRequest?.[1]?.redirect).toBe('manual')
  })

  it('derives the same OAuth identity for the accepted all-loopback topology', async () => {
    const loopbackConfig = loopbackAuthConfig
    const loopbackMetadata = {
      authorization_endpoint: 'http://127.0.0.1:3000/authorize',
      coral_web_session_endpoint: 'http://127.0.0.1:3000/oauth/session',
      issuer: loopbackConfig.issuer,
      token_endpoint: 'http://127.0.0.1:3000/token',
    }
    mockFetch(jsonResponse(loopbackMetadata))

    const response = await startCoralOAuthLogin(
      new Request('http://127.0.0.1:8000/login'),
      loopbackConfig,
    )
    const location = new URL(response.headers.get('location') ?? '')

    expect(location.searchParams.get('client_id')).toBe(
      'http://localhost:5173/.well-known/oauth-client',
    )
    expect(location.searchParams.get('redirect_uri')).toBe('http://localhost:5173/auth/callback')
    expect(location.searchParams.get('resource')).toBe('http://localhost:5173')
    expect(setCookies(response)[0]).not.toContain('Secure')
  })

  // The absolute form is the one an external target is usually written as. The
  // traversal forms are the ones that reach `Location` if the destination is
  // normalized rather than validated — `URL` turns `/..//evil.example` into the
  // scheme-relative path `//evil.example`, and a browser follows that off-origin
  // exactly as it would an absolute URL. Both are checked through the whole
  // round trip, because the value is only dangerous once it comes back out of
  // the transaction cookie as a header.
  it.each([
    'https://evil.example/phish',
    '/..//evil.example',
    '/..//evil.example/phish',
    '/./..//evil.example',
  ])('falls back to home for an external return target: %s', async (returnTo) => {
    mockFetch(
      jsonResponse(metadata),
      jsonResponse(metadata),
      jsonResponse(tokens),
      jsonResponse(startedSession, { status: 201 }),
    )
    const login = await startCoralOAuthLogin(
      new Request(`https://coral-web.example.test/login?returnTo=${encodeURIComponent(returnTo)}`),
      config,
    )
    const state = new URL(login.headers.get('location') ?? '').searchParams.get('state')
    const callback = await completeCoralOAuthLogin(
      new Request(`https://coral-web.example.test/auth/callback?code=abc123&state=${state}`, {
        headers: { cookie: cookieHeader(login, oauthCookieName(config)) },
      }),
      config,
    )

    expect(callback.headers.get('location')).toBe('/')
  })

  it('clears OAuth state only after a callback matches the active transaction', async () => {
    const fetch = mockFetch(jsonResponse(metadata))
    const login = await startCoralOAuthLogin(
      new Request('https://coral-web.example.test/login'),
      config,
    )
    const cookie = cookieHeader(login, oauthCookieName(config))
    const state = new URL(login.headers.get('location') ?? '').searchParams.get('state')

    const providerError = await completeCoralOAuthLogin(
      new Request(
        `https://coral-web.example.test/auth/callback?error=access_denied&state=${state}`,
        {
          headers: { cookie },
        },
      ),
      config,
    ).catch((error: unknown) => error)
    const stateError = await completeCoralOAuthLogin(
      new Request('https://coral-web.example.test/auth/callback?code=abc&state=wrong', {
        headers: { cookie },
      }),
      config,
    ).catch((error: unknown) => error)

    expect(providerError).toMatchObject({ status: 400 })
    expect((providerError as Response).headers.get('Set-Cookie')).toContain('Max-Age=0')
    expect(stateError).toMatchObject({ status: 400 })
    expect((stateError as Response).headers.has('Set-Cookie')).toBe(false)
    expect(fetch).toHaveBeenCalledTimes(1)
  })

  it('normalizes direct token-endpoint OAuth JSON into the callback error contract', async () => {
    mockFetch(
      jsonResponse(metadata),
      jsonResponse(metadata),
      jsonResponse(
        { error: 'invalid_target', error_description: 'resource is not allowed' },
        { status: 400 },
      ),
    )
    const login = await startCoralOAuthLogin(
      new Request('https://coral-web.example.test/login'),
      config,
    )
    const state = new URL(login.headers.get('location') ?? '').searchParams.get('state')

    const error = await completeCoralOAuthLogin(
      new Request(`https://coral-web.example.test/auth/callback?code=abc&state=${state}`, {
        headers: { cookie: cookieHeader(login, oauthCookieName(config)) },
      }),
      config,
    ).catch((caught: unknown) => caught)

    expect(error).toBeInstanceOf(Response)
    expect((error as Response).status).toBe(400)
    await expect((error as Response).text()).resolves.toBe(
      'invalid_target: resource is not allowed',
    )
    expect((error as Response).headers.get('Set-Cookie')).toContain('Max-Age=0')
  })

  it('uses RFC 8414 discovery for a path issuer and requires its exact identifier', async () => {
    const issuer = 'https://coral.example.test/tenant/'
    const pathConfig = { ...config, issuer }
    const pathMetadata = {
      authorization_endpoint: `${issuer}authorize`,
      coral_web_session_endpoint: `${issuer}session`,
      issuer,
      token_endpoint: `${issuer}token`,
    }
    const fetch = mockFetch(jsonResponse(pathMetadata))

    await startCoralOAuthLogin(new Request('https://coral-web.example.test/login'), pathConfig)

    expect(fetch).toHaveBeenCalledWith(
      'https://coral.example.test/.well-known/oauth-authorization-server/tenant/',
      { headers: { accept: 'application/json' } },
    )
  })

  it('ignores dynamic registration metadata and always uses the derived CIMD client ID', async () => {
    const fetch = mockFetch(
      jsonResponse({ ...metadata, registration_endpoint: `${AUTH_ISSUER}/register` }),
    )
    const response = await startCoralOAuthLogin(
      new Request('https://coral-web.example.test/login'),
      config,
    )

    expect(fetch).toHaveBeenCalledTimes(1)
    expect(new URL(response.headers.get('location') ?? '').searchParams.get('client_id')).toBe(
      authClientId(config),
    )
  })

  it('rejects metadata for a different issuer', async () => {
    mockFetch(
      jsonResponse({
        ...metadata,
        authorization_endpoint: 'not a URL',
        issuer: 'https://attacker.example',
      }),
    )

    await expect(
      startCoralOAuthLogin(new Request('https://coral-web.example.test/login'), config),
    ).rejects.toThrow('metadata issuer does not match')
  })

  it.each(untrustedEndpointCases)(
    'rejects an untrusted authorization endpoint using a %s',
    async (_, endpoint) => {
      const fetch = mockFetch(jsonResponse({ ...metadata, authorization_endpoint: endpoint }))

      await expect(
        startCoralOAuthLogin(new Request('https://coral-web.example.test/login'), config),
      ).rejects.toThrow('authorization_endpoint must be an absolute same-origin HTTP(S) URL')
      expect(fetch).toHaveBeenCalledTimes(1)
    },
  )

  it.each(untrustedEndpointCases)(
    'rejects an untrusted token endpoint using a %s',
    async (_, endpoint) => {
      const fetch = mockFetch(
        jsonResponse(metadata),
        jsonResponse({ ...metadata, token_endpoint: endpoint }),
      )
      const login = await startCoralOAuthLogin(
        new Request('https://coral-web.example.test/login'),
        config,
      )
      const state = new URL(login.headers.get('location') ?? '').searchParams.get('state')

      await expect(
        completeCoralOAuthLogin(
          new Request(`https://coral-web.example.test/auth/callback?code=abc&state=${state}`, {
            headers: { cookie: cookieHeader(login, oauthCookieName(config)) },
          }),
          config,
        ),
      ).rejects.toThrow('token_endpoint must be an absolute same-origin HTTP(S) URL')
      expect(fetch).toHaveBeenCalledTimes(2)
    },
  )

  it('rejects a token endpoint redirect without retaining OAuth state', async () => {
    const attackerEndpoint = 'https://attacker.example/token'
    const metadataEndpoint = `${AUTH_ISSUER}/.well-known/oauth-authorization-server`
    const fetch = vi.fn<typeof globalThis.fetch>()
    fetch.mockImplementation(async (input, init) => {
      const url = input instanceof Request ? input.url : input.toString()
      if (url === metadataEndpoint) return jsonResponse(metadata)
      if (url === metadata.token_endpoint) {
        if (init?.redirect === 'manual') {
          return new Response(null, {
            headers: { location: attackerEndpoint },
            status: 307,
          })
        }
        return fetch(attackerEndpoint, init)
      }
      if (url === attackerEndpoint) {
        return jsonResponse({ access_token: 'stolen-token', expires_in: 3600 })
      }
      throw new Error(`unexpected fetch to ${url}`)
    })
    vi.stubGlobal('fetch', fetch)

    const login = await startCoralOAuthLogin(
      new Request('https://coral-web.example.test/login'),
      config,
    )
    const state = new URL(login.headers.get('location') ?? '').searchParams.get('state')

    const error = await completeCoralOAuthLogin(
      new Request(`https://coral-web.example.test/auth/callback?code=abc&state=${state}`, {
        headers: { cookie: cookieHeader(login, oauthCookieName(config)) },
      }),
      config,
    ).catch((caught: unknown) => caught)

    expect(error).toBeInstanceOf(Response)
    expect((error as Response).status).toBe(400)
    await expect((error as Response).text()).resolves.toBe(
      'Coral OAuth token exchange failed with HTTP 307',
    )
    expect((error as Response).headers.get('Set-Cookie')).toContain('Max-Age=0')
    expect(fetch.mock.calls.map(([input]) => input.toString())).toEqual([
      metadataEndpoint,
      metadataEndpoint,
      metadata.token_endpoint,
    ])
  })

  it.each(untrustedEndpointCases)(
    'rejects an untrusted web session endpoint using a %s',
    async (_, endpoint) => {
      const fetch = mockFetch(jsonResponse({ ...metadata, coral_web_session_endpoint: endpoint }))

      await expect(
        startCoralOAuthLogin(new Request('https://coral-web.example.test/login'), config),
      ).rejects.toThrow('coral_web_session_endpoint must be an absolute same-origin HTTP(S) URL')
      expect(fetch).toHaveBeenCalledTimes(1)
    },
  )

  // Without a refresh token Reef has nothing to keep a session with, and it
  // never stores the access token instead.
  it.each([
    ['returns no refresh token', jsonResponse({ access_token: 'token', expires_in: 3600 }), null],
    [
      'starts a session without saying which',
      jsonResponse(tokens),
      jsonResponse({ expires_in: 0 }, { status: 201 }),
    ],
    [
      'refuses to start a session',
      jsonResponse(tokens),
      jsonResponse({ error: 'invalid_grant' }, { status: 400 }),
    ],
  ])('fails sign-in when Coral %s', async (_label, tokenResponse, sessionResponse) => {
    mockFetch(
      jsonResponse(metadata),
      jsonResponse(metadata),
      tokenResponse,
      ...(sessionResponse ? [sessionResponse] : []),
    )
    const login = await startCoralOAuthLogin(
      new Request('https://coral-web.example.test/login'),
      config,
    )
    const state = new URL(login.headers.get('location') ?? '').searchParams.get('state')

    const failure = await completeCoralOAuthLogin(
      new Request(`https://coral-web.example.test/auth/callback?code=abc&state=${state}`, {
        headers: { cookie: cookieHeader(login, oauthCookieName(config)) },
      }),
      config,
    ).catch((error: unknown) => error)
    expect(failure).toBeInstanceOf(Response)
    expect((failure as Response).status).toBe(400)
    expect(setCookies(failure as Response).join()).not.toContain(`${config.cookieName}=`)
  })
})

function jsonResponse(body: unknown, init: ResponseInit = {}): Response {
  return new Response(JSON.stringify(body), {
    headers: { 'content-type': 'application/json' },
    ...init,
  })
}

function mockFetch(...responses: Response[]) {
  const fetch = vi.fn<typeof globalThis.fetch>(async () => {
    const response = responses.shift()
    if (!response) throw new Error('unexpected fetch call')
    return response
  })
  vi.stubGlobal('fetch', fetch)
  return fetch
}

function setCookies(response: Response): string[] {
  const headers = response.headers as Headers & { getSetCookie?: () => string[] }
  const cookies = headers.getSetCookie?.()
  if (cookies?.length) return cookies
  const cookie = response.headers.get('set-cookie')
  return cookie ? [cookie] : []
}

function cookieHeader(response: Response, name: string): string {
  const cookie = setCookies(response).find((value) => value.startsWith(`${name}=`))
  if (!cookie) throw new Error(`missing ${name} Set-Cookie header`)
  return cookie.split(';', 1)[0]
}

/** Every cookie the response sets, decoded as a browser would store it. */
function decodedCookies(response: Response): string {
  return setCookies(response)
    .map((cookie) => {
      const value = decodeURIComponent(cookie.split(';', 1)[0].split('=').slice(1).join('='))
      return Buffer.from(value.split('.')[0] ?? '', 'base64').toString('utf8')
    })
    .join('\n')
}
