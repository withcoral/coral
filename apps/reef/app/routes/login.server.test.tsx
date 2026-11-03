import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { localRuntimeConfig, publicRouteTestArgs } from '@/auth/server-context.test-helper'
import { commitCoralWebSession, commitPendingSignOut, oauthCookieName } from '@/auth/session.server'
import { hostedRuntimeConfig, requiredAuthConfig } from '@/auth/test-config'
import type { CoralWebRuntimeConfig } from '@/lib/runtime-config.server'

import { headers, loader } from './login'

const issuer = requiredAuthConfig.issuer
const started = { endpoint: `${issuer}/oauth/session`, expiresIn: 3600, handle: 'coral_ws_handle' }
/** How Coral answers ending a session, unless a test changes it. */
let endAnswer = 200
// Discovery, session resolves, and session ends, routed by URL.
const discovery = vi.fn(async (input: string | URL | Request) => {
  const url = input.toString()
  if (url.endsWith('/oauth/session/token')) {
    return Response.json({ access_token: 'minted', expires_in: 300, session_expires_in: 3000 })
  }
  if (url.endsWith('/oauth/session/end')) return new Response(null, { status: endAnswer })
  return Response.json({
    authorization_endpoint: `${issuer}/authorize`,
    coral_web_session_endpoint: `${issuer}/oauth/session`,
    issuer,
    token_endpoint: `${issuer}/token`,
  })
})

describe('login route', () => {
  beforeEach(() => {
    endAnswer = 200
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
    const session = await commitCoralWebSession(started, requiredAuthConfig)

    const response = await runLoader('https://coral-web.example.test/login', {
      cookie: session.split(';', 1)[0],
    })

    expect(response.status).toBe(302)
    expect(response.headers.get('location')).toBe('/')
    expect(response.headers.get('Cache-Control')).toBe('private, no-store')
    expect(discovery.mock.calls.map(([url]) => url.toString())).toEqual([
      `${started.endpoint}/token`,
    ])
  })

  it.each([
    [200, false],
    [503, true],
  ])(
    'finishes a pending sign-out on the next visit when Coral answers %i',
    async (status, stillPending) => {
      endAnswer = status
      const pending = await commitPendingSignOut(
        new Request('https://coral-web.example.test/', {
          headers: {
            cookie: (await commitCoralWebSession(started, requiredAuthConfig)).split(';', 1)[0],
          },
        }),
        requiredAuthConfig,
      )

      const result = await runLoaderValue('https://coral-web.example.test/login?signedOut=1', {
        cookie: pending!.split(';', 1)[0],
      })

      expect(discovery.mock.calls.map(([url]) => url.toString())).toEqual([
        `${started.endpoint}/end`,
      ])
      expect(loaderValue(result)).toMatchObject({ signOutPending: stillPending })
      const cleared = loaderHeaders(result)
        .getSetCookie()
        .some((cookie) => cookie.startsWith(`${requiredAuthConfig.cookieName}_signout=;`))
      expect(cleared).toBe(!stillPending)
    },
  )

  it('renders the signed-out interstitial without immediately restarting SSO', async () => {
    expect(
      loaderValue(await runLoaderValue('https://coral-web.example.test/login?signedOut=1')),
    ).toEqual({
      returnTo: '/',
      sessionExpired: false,
      signOutPending: false,
    })
    expect(discovery).not.toHaveBeenCalled()
  })

  // The interstitial renders instead of redirecting, so unlike every other
  // branch it has to carry the destination across the pause itself.
  it('carries a destination through the interstitial', async () => {
    expect(
      loaderValue(
        await runLoaderValue(
          'https://coral-web.example.test/login?signedOut=1&returnTo=%2Fworkspaces%2Fanalytics%3Ftab%3Dmine',
        ),
      ),
    ).toEqual({
      returnTo: '/workspaces/analytics?tab=mine',
      sessionExpired: false,
      signOutPending: false,
    })
  })

  it('drops a destination the interstitial should not send anyone to', async () => {
    expect(
      loaderValue(
        await runLoaderValue(
          'https://coral-web.example.test/login?signedOut=1&returnTo=%2F..%2F%2Fevil.example',
        ),
      ),
    ).toEqual({ returnTo: '/', sessionExpired: false, signOutPending: false })
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

/** The value a loader returned with `data()`, or returned bare. */
function loaderValue(result: unknown): unknown {
  return result && typeof result === 'object' && 'data' in result ? result.data : result
}

/** The headers a loader returned with `data()`. */
function loaderHeaders(result: unknown): Headers {
  const init =
    result && typeof result === 'object' && 'init' in result
      ? (result.init as ResponseInit | null)
      : null
  return new Headers(init?.headers)
}
