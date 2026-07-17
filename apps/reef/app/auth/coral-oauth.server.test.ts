import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { completeCoralOAuthLogin, startCoralOAuthLogin } from './coral-oauth.server'
import { readReefSession } from './session.server'
import type { RequiredAuthConfig } from './types'

const AUTH_ISSUER = 'https://login.example.test'
const CLIENT_ID = 'coral-cloud-ui'
const config: RequiredAuthConfig = {
  clientId: 'coral-cloud-ui',
  cookieName: 'reef_session',
  cookieSecure: true,
  issuer: AUTH_ISSUER,
  mode: 'required',
  redirectUri: 'https://reef.example.test/auth/callback',
  scope: 'coral:mcp',
  sessionMaxAgeSeconds: 3600,
  sessionSecret: '0123456789abcdef0123456789abcdef',
}
const metadata = {
  authorization_endpoint: `${AUTH_ISSUER}/authorize`,
  issuer: AUTH_ISSUER,
  token_endpoint: `${AUTH_ISSUER}/token`,
}

describe('Coral OAuth adapter', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-01-01T00:00:00Z'))
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.unstubAllGlobals()
  })

  it('starts login with PKCE, provider-neutral config, and a safe return path', async () => {
    const fetch = mockFetch(jsonResponse(metadata))
    const response = await startCoralOAuthLogin(
      new Request('https://reef.example.test/login?returnTo=/workspaces/analytics?tab=mine'),
      config,
    )
    const location = new URL(response.headers.get('location') ?? '')

    expect(fetch).toHaveBeenCalledWith(`${AUTH_ISSUER}/.well-known/oauth-authorization-server`, {
      headers: { accept: 'application/json' },
    })
    expect(location.origin).toBe(AUTH_ISSUER)
    expect(location.searchParams.get('client_id')).toBe(CLIENT_ID)
    expect(location.searchParams.get('redirect_uri')).toBe(config.redirectUri)
    expect(location.searchParams.get('scope')).toBe('coral:mcp')
    expect(location.searchParams.get('state')).toBeTruthy()
    expect(location.searchParams.get('code_challenge')).toBeTruthy()
    expect(location.searchParams.get('code_challenge_method')).toBe('S256')
    expect(setCookies(response)).toEqual([expect.stringContaining('reef_oauth=')])
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
      new Request(`https://reef.example.test/auth/callback?code=abc123&state=${state}`, {
        headers: { cookie: cookieHeader(login, 'reef_oauth') },
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
    expect(cookies[0]).toContain('reef_oauth=')
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
    expect(body.get('client_id')).toBe(CLIENT_ID)
  })

  it('falls back to home for an external return target', async () => {
    mockFetch(
      jsonResponse(metadata),
      jsonResponse(metadata),
      jsonResponse({ access_token: 'token', expires_in: 3600 }),
    )
    const login = await startCoralOAuthLogin(
      new Request('https://reef.example.test/login?returnTo=https://evil.example/phish'),
      config,
    )
    const state = new URL(login.headers.get('location') ?? '').searchParams.get('state')
    const callback = await completeCoralOAuthLogin(
      new Request(`https://reef.example.test/auth/callback?code=abc123&state=${state}`, {
        headers: { cookie: cookieHeader(login, 'reef_oauth') },
      }),
      config,
    )

    expect(callback.headers.get('location')).toBe('/')
  })

  it('clears OAuth state only after a callback matches the active transaction', async () => {
    const fetch = mockFetch(jsonResponse(metadata))
    const login = await startCoralOAuthLogin(new Request('https://reef.example.test/login'), config)
    const cookie = cookieHeader(login, 'reef_oauth')
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

  it('uses RFC 8414 discovery for a path issuer and requires its exact identifier', async () => {
    const issuer = 'https://login.example.test/tenant/'
    const pathConfig = { ...config, issuer }
    const pathMetadata = {
      ...metadata,
      authorization_endpoint: `${issuer}authorize`,
      issuer,
      token_endpoint: `${issuer}token`,
    }
    const fetch = mockFetch(jsonResponse(pathMetadata))

    await startCoralOAuthLogin(new Request('https://reef.example.test/login'), pathConfig)

    expect(fetch).toHaveBeenCalledWith(
      'https://login.example.test/.well-known/oauth-authorization-server/tenant/',
      { headers: { accept: 'application/json' } },
    )
  })

  it('dynamically registers a client when none is configured', async () => {
    const registrationEndpoint = `${AUTH_ISSUER}/register`
    const fetch = mockFetch(
      jsonResponse({ ...metadata, registration_endpoint: registrationEndpoint }),
      jsonResponse({ client_id: 'dynamic-client' }),
    )
    const response = await startCoralOAuthLogin(new Request('http://localhost:5173/login'), {
      ...config,
      clientId: null,
      cookieSecure: false,
      redirectUri: null,
    })

    expect(fetch.mock.calls[1]?.[0]).toBe(registrationEndpoint)
    expect(new URL(response.headers.get('location') ?? '').searchParams.get('client_id')).toBe(
      'dynamic-client',
    )
  })

  it('rejects metadata for a different issuer', async () => {
    mockFetch(jsonResponse({ ...metadata, issuer: 'https://attacker.example' }))

    await expect(
      startCoralOAuthLogin(new Request('https://reef.example.test/login'), config),
    ).rejects.toThrow('metadata issuer does not match')
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
          headers: { cookie: cookieHeader(login, 'reef_oauth') },
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
