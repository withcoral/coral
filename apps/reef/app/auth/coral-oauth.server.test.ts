import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { authClientId, authRedirectUri, authResource } from './config.server'
import { completeCoralOAuthLogin, startCoralOAuthLogin } from './coral-oauth.server'
import { oauthCookieName, readOAuthTransaction, readReefSession } from './session.server'
import type { RequiredAuthConfig } from './types'

const AUTH_ISSUER = 'https://coral.example.test'
const config: RequiredAuthConfig = {
  cookieName: 'reef_session',
  issuer: AUTH_ISSUER,
  mode: 'required',
  publicUrl: 'https://reef.example.test',
  sessionMaxAgeSeconds: 3600,
  sessionSecret: '0123456789abcdef0123456789abcdef',
}
const metadata = {
  authorization_endpoint: `${AUTH_ISSUER}/authorize`,
  issuer: AUTH_ISSUER,
  token_endpoint: `${AUTH_ISSUER}/token`,
}
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

  it('starts login with PKCE and identity derived only from the Reef public URL', async () => {
    const fetch = mockFetch(jsonResponse(metadata))
    const response = await startCoralOAuthLogin(
      new Request('https://internal-proxy/login?returnTo=/workspaces/analytics?tab=mine'),
      config,
    )
    const location = new URL(response.headers.get('location') ?? '')
    const transaction = await readOAuthTransaction(
      new Request('https://reef.example.test/', {
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

  it('completes login, clears OAuth state, and stores a bounded encrypted session', async () => {
    const fetch = mockFetch(
      jsonResponse(metadata),
      jsonResponse(metadata),
      jsonResponse({ access_token: 'opaque-coral-token', expires_in: 7200, token_type: 'Bearer' }),
    )
    const login = await startCoralOAuthLogin(
      new Request('https://reef.example.test/login?returnTo=/workspaces/analytics?tab=mine'),
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
    const session = await readReefSession(
      new Request('https://reef.example.test/', {
        headers: { cookie: cookieHeader(callback, 'reef_session') },
      }),
      config,
    )

    expect(callback.headers.get('location')).toBe('/workspaces/analytics?tab=mine')
    expect(cookies[0]).toContain(`${oauthCookieName(config)}=`)
    expect(cookies[0]).toContain('Max-Age=0')
    expect(cookies[1]).toContain('reef_session=')
    expect(cookies[1]).not.toContain('opaque-coral-token')
    expect(session).toEqual({
      accessToken: 'opaque-coral-token',
      expiresAt: unixTimestamp() + 3600,
      tokenType: 'Bearer',
    })

    const tokenRequest = fetch.mock.calls[2]
    const body = tokenRequest?.[1]?.body as URLSearchParams
    expect(tokenRequest?.[0]).toBe(`${AUTH_ISSUER}/token`)
    expect(body.get('grant_type')).toBe('authorization_code')
    expect(body.get('code')).toBe('abc123')
    expect(body.get('code_verifier')).toBeTruthy()
    expect(body.get('client_id')).toBe(authClientId(config))
    expect(body.get('redirect_uri')).toBe(authRedirectUri(config))
    expect(body.get('resource')).toBe(authResource(config))
    expect(body.has('scope')).toBe(false)
  })

  it('derives the same OAuth identity for the accepted all-loopback topology', async () => {
    const loopbackConfig = {
      ...config,
      issuer: 'http://127.0.0.1:3000',
      publicUrl: 'http://localhost:5173',
    }
    const loopbackMetadata = {
      authorization_endpoint: 'http://127.0.0.1:3000/authorize',
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
      jsonResponse({ access_token: 'token', expires_in: 3600 }),
    )
    const login = await startCoralOAuthLogin(
      new Request(`https://reef.example.test/login?returnTo=${encodeURIComponent(returnTo)}`),
      config,
    )
    const state = new URL(login.headers.get('location') ?? '').searchParams.get('state')
    const callback = await completeCoralOAuthLogin(
      new Request(`https://reef.example.test/auth/callback?code=abc123&state=${state}`, {
        headers: { cookie: cookieHeader(login, oauthCookieName(config)) },
      }),
      config,
    )

    expect(callback.headers.get('location')).toBe('/')
  })

  it('clears OAuth state only after a callback matches the active transaction', async () => {
    const fetch = mockFetch(jsonResponse(metadata))
    const login = await startCoralOAuthLogin(new Request('https://reef.example.test/login'), config)
    const cookie = cookieHeader(login, oauthCookieName(config))
    const state = new URL(login.headers.get('location') ?? '').searchParams.get('state')

    const providerError = await completeCoralOAuthLogin(
      new Request(`https://reef.example.test/auth/callback?error=access_denied&state=${state}`, {
        headers: { cookie },
      }),
      config,
    ).catch((error: unknown) => error)
    const stateError = await completeCoralOAuthLogin(
      new Request('https://reef.example.test/auth/callback?code=abc&state=wrong', {
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
    const login = await startCoralOAuthLogin(new Request('https://reef.example.test/login'), config)
    const state = new URL(login.headers.get('location') ?? '').searchParams.get('state')

    const error = await completeCoralOAuthLogin(
      new Request(`https://reef.example.test/auth/callback?code=abc&state=${state}`, {
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
      issuer,
      token_endpoint: `${issuer}token`,
    }
    const fetch = mockFetch(jsonResponse(pathMetadata))

    await startCoralOAuthLogin(new Request('https://reef.example.test/login'), pathConfig)

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
      new Request('https://reef.example.test/login'),
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
      startCoralOAuthLogin(new Request('https://reef.example.test/login'), config),
    ).rejects.toThrow('metadata issuer does not match')
  })

  it.each(untrustedEndpointCases)(
    'rejects an untrusted authorization endpoint using a %s',
    async (_, endpoint) => {
      const fetch = mockFetch(jsonResponse({ ...metadata, authorization_endpoint: endpoint }))

      await expect(
        startCoralOAuthLogin(new Request('https://reef.example.test/login'), config),
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
        new Request('https://reef.example.test/login'),
        config,
      )
      const state = new URL(login.headers.get('location') ?? '').searchParams.get('state')

      await expect(
        completeCoralOAuthLogin(
          new Request(`https://reef.example.test/auth/callback?code=abc&state=${state}`, {
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

    const login = await startCoralOAuthLogin(new Request('https://reef.example.test/login'), config)
    const state = new URL(login.headers.get('location') ?? '').searchParams.get('state')

    const error = await completeCoralOAuthLogin(
      new Request(`https://reef.example.test/auth/callback?code=abc&state=${state}`, {
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

  it('requires a positive standards-based token expiry', async () => {
    mockFetch(
      jsonResponse(metadata),
      jsonResponse(metadata),
      jsonResponse({ access_token: 'token-with-ignored-jwt-shape' }),
    )
    const login = await startCoralOAuthLogin(new Request('https://reef.example.test/login'), config)
    const state = new URL(login.headers.get('location') ?? '').searchParams.get('state')

    await expect(
      completeCoralOAuthLogin(
        new Request(`https://reef.example.test/auth/callback?code=abc&state=${state}`, {
          headers: { cookie: cookieHeader(login, oauthCookieName(config)) },
        }),
        config,
      ),
    ).rejects.toThrow('positive expires_in')
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

function unixTimestamp(): number {
  return Math.floor(Date.now() / 1000)
}
