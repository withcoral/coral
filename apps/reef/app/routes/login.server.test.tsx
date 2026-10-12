import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { localRuntimeConfig, publicRouteTestArgs } from '@/auth/server-context.test-helper'
import { commitCoralWebSession, oauthCookieName } from '@/auth/session.server'
import { hostedRuntimeConfig, requiredAuthConfig } from '@/auth/test-config'
import type { CoralWebRuntimeConfig } from '@/lib/runtime-config.server'

import { headers, loader } from './login'

const issuer = requiredAuthConfig.issuer
const discovery = vi.fn(async () =>
  Response.json({
    authorization_endpoint: `${issuer}/authorize`,
    issuer,
    token_endpoint: `${issuer}/token`,
  }),
)

describe('login route', () => {
  beforeEach(() => {
    discovery.mockClear()
    vi.stubGlobal('fetch', discovery)
  })

  afterEach(() => vi.unstubAllGlobals())

  it('marks rendered login responses private at the route boundary', () => {
    const responseHeaders = new Headers(headers())

    expect(responseHeaders.get('Cache-Control')).toBe('private, no-store')
    expect(responseHeaders.get('Vary')).toBe('Cookie')
  })

  it('returns disabled local and desktop requests home without starting OAuth', async () => {
    const response = await runLoader('http://localhost:5173/login', undefined, localRuntimeConfig)

    expect(response.status).toBe(302)
    expect(response.headers.get('location')).toBe('/')
    expect(discovery).not.toHaveBeenCalled()
  })

  it('returns an existing hosted session home instead of restarting OAuth', async () => {
    const session = await commitCoralWebSession(
      { accessToken: 'server-only-token', expiresAt: 4_102_444_800, tokenType: 'Bearer' },
      requiredAuthConfig,
    )

    const response = await runLoader('https://coral-web.example.test/login', {
      cookie: session.split(';', 1)[0],
    })

    expect(response.status).toBe(302)
    expect(response.headers.get('location')).toBe('/')
    expect(response.headers.get('Cache-Control')).toBe('private, no-store')
    expect(discovery).not.toHaveBeenCalled()
  })

  it('renders the signed-out interstitial without immediately restarting SSO', async () => {
    await expect(
      runLoaderValue('https://coral-web.example.test/login?signedOut=1'),
    ).resolves.toEqual({
      returnTo: '/',
      sessionExpired: false,
    })
    expect(discovery).not.toHaveBeenCalled()
  })

  // The interstitial renders instead of redirecting, so unlike every other
  // branch it has to carry the destination across the pause itself.
  it('carries a destination through the interstitial', async () => {
    await expect(
      runLoaderValue(
        'https://coral-web.example.test/login?signedOut=1&returnTo=%2Fworkspaces%2Fanalytics%3Ftab%3Dmine',
      ),
    ).resolves.toEqual({ returnTo: '/workspaces/analytics?tab=mine', sessionExpired: false })
  })

  it('drops a destination the interstitial should not send anyone to', async () => {
    await expect(
      runLoaderValue(
        'https://coral-web.example.test/login?signedOut=1&returnTo=%2F..%2F%2Fevil.example',
      ),
    ).resolves.toEqual({ returnTo: '/', sessionExpired: false })
  })

  it('starts hosted OAuth at the issuer and sets the transaction cookie privately', async () => {
    const response = await runLoader('https://coral-web.example.test/login?returnTo=%2Fworkspaces')

    expect(response.status).toBe(302)
    expect(new URL(response.headers.get('location')!).origin).toBe(issuer)
    expect(response.headers.getSetCookie().map((cookie) => cookie.split('=', 1)[0])).toEqual([
      oauthCookieName(requiredAuthConfig),
    ])
    expect(response.headers.get('Cache-Control')).toBe('private, no-store')
    expect(response.headers.get('Vary')).toContain('Cookie')
  })
})

async function runLoader(
  url: string,
  init?: HeadersInit,
  runtime?: CoralWebRuntimeConfig,
): Promise<Response> {
  const result = await runLoaderValue(url, init, runtime)
  expect(result).toBeInstanceOf(Response)
  return result as Response
}

async function runLoaderValue(
  url: string,
  init?: HeadersInit,
  runtime: CoralWebRuntimeConfig = hostedRuntimeConfig,
) {
  return loader(publicRouteTestArgs(new Request(url, { headers: init }), runtime))
}
