import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { csrfTokenForRequest } from '@/auth/csrf.server'
import { requestAuthContext } from '@/auth/server-context'
import { localRuntimeConfig, runtimeTestContext } from '@/auth/server-context.test-helper'
import { commitCoralWebSession } from '@/auth/session.server'
import { hostedRuntimeConfig, requiredAuthConfig } from '@/auth/test-config'
import type { CoralWebRuntimeConfig } from '@/lib/runtime-config.server'

import { middleware } from './_protected'

const config = requiredAuthConfig
const started = {
  endpoint: `${config.issuer}/oauth/session`,
  expiresIn: 3600,
  handle: 'coral_ws_handle',
}

/** Coral's answer to every session resolve, unless a test replaces it. */
let coral: () => Promise<Response>
const minted = () =>
  Promise.resolve(
    Response.json({ access_token: 'coral-token', expires_in: 300, session_expires_in: 3000 }),
  )
const csrfCookieName = `${config.cookieName}_csrf`

/** Runs the boundary under `runtime` for `path` with `cookie`, then `next`. */
async function protect(
  runtime: CoralWebRuntimeConfig,
  next: () => Promise<Response>,
  { path = '/activity?tab=mine', cookie = '', method = 'GET' } = {},
) {
  const url = new URL(path, 'https://coral-web.example.test')
  const context = runtimeTestContext(runtime)
  const request = new Request(url, { method, headers: cookie ? { cookie } : {} })
  const nextSpy = vi.fn(next)
  try {
    const response = (await middleware[0]!(
      { context, request, url, params: {}, pattern: url.pathname } as never,
      nextSpy as never,
    )) as Response
    return { response, thrown: false, context, next: nextSpy }
  } catch (error) {
    return { response: error as Response, thrown: true, context, next: nextSpy }
  }
}

/** A real session cookie, optionally with its current CSRF cookie. */
async function signedIn({ withCsrf = true } = {}) {
  const sessionCookie = (await commitCoralWebSession(started, config)).split(';', 1)[0]
  if (!withCsrf) return sessionCookie
  const { setCookie } = await csrfTokenForRequest(
    new Request('https://coral-web.example.test/', { headers: { cookie: sessionCookie } }),
    config,
    started,
  )
  return `${sessionCookie}; ${setCookie!.split(';', 1)[0]}`
}

const cookieNames = (response: Response) =>
  response.headers.getSetCookie().map((cookie) => cookie.slice(0, cookie.indexOf('=')))

describe('protected route boundary', () => {
  beforeEach(() => {
    coral = minted
    vi.stubGlobal(
      'fetch',
      vi.fn(() => coral()),
    )
  })

  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('sets disabled request auth in local mode', async () => {
    const { response, thrown, context } = await protect(localRuntimeConfig, async () =>
      Response.json({}),
    )

    expect(thrown).toBe(false)
    expect(context.get(requestAuthContext)).toEqual({ accessToken: null, mode: 'disabled' })
    expect(response.headers.get('Cache-Control')).toBe('private, no-store')
    expect(response.headers.get('Vary')).toBe('Cookie')
  })

  it.each(['GET', 'POST'] as const)(
    'sends a %s without a session to sign in, carrying the destination',
    async (method) => {
      const { response, thrown, next } = await protect(
        hostedRuntimeConfig,
        async () => Response.json({}),
        { method },
      )

      expect(thrown).toBe(true)
      expect(response.status).toBe(method === 'POST' ? 303 : 302)
      expect(response.headers.get('Location')).toBe('/login?returnTo=%2Factivity%3Ftab%3Dmine')
      expect(response.headers.get('Cache-Control')).toBe('private, no-store')
      expect(next).not.toHaveBeenCalled()
    },
  )

  it('sets the session for a signed-in request', async () => {
    const { response, thrown, context } = await protect(
      hostedRuntimeConfig,
      async () => Response.json({}),
      { cookie: await signedIn() },
    )

    expect(thrown).toBe(false)
    expect(context.get(requestAuthContext)).toMatchObject({
      mode: 'required',
      accessToken: 'coral-token',
      session: { accessToken: 'coral-token', handle: started.handle },
    })
    expect(response.headers.get('Cache-Control')).toBe('private, no-store')
  })

  it.each([
    ['returned', async () => new Response(null, { status: 401 })],
    [
      'thrown',
      async () => {
        throw new Response(null, { status: 401 })
      },
    ],
  ])('signs in again when Coral rejects the session in a %s 401', async (_, next) => {
    const { response, thrown } = await protect(hostedRuntimeConfig, next, {
      cookie: await signedIn(),
    })

    expect(thrown).toBe(true)
    expect(response.status).toBe(302)
    expect(response.headers.get('Location')).toBe(
      '/login?returnTo=%2Factivity%3Ftab%3Dmine&sessionExpired=1',
    )
    // Both cookies clear, so the next sign-in starts clean.
    expect(cookieNames(response).toSorted()).toEqual([config.cookieName, csrfCookieName].toSorted())
    for (const cookie of response.headers.getSetCookie()) expect(cookie).toMatch(/Max-Age=0/)
  })

  it('signs in again, clearing the session, once Coral refuses it', async () => {
    coral = () => Promise.resolve(Response.json({ error: 'invalid_grant' }, { status: 400 }))
    const { response, thrown, next } = await protect(
      hostedRuntimeConfig,
      async () => Response.json({}),
      { cookie: await signedIn() },
    )

    expect(thrown).toBe(true)
    expect(response.headers.get('Location')).toBe(
      '/login?returnTo=%2Factivity%3Ftab%3Dmine&sessionExpired=1',
    )
    expect(cookieNames(response)).toContain(config.cookieName)
    expect(next).not.toHaveBeenCalled()
  })

  // A session Coral cannot answer for may still be valid: the visitor keeps it
  // and the request fails, rather than being signed out by an outage.
  it('answers 503 and keeps the session when Coral is unavailable', async () => {
    coral = () => Promise.reject(new TypeError('fetch failed'))
    const { response, thrown, next } = await protect(
      hostedRuntimeConfig,
      async () => Response.json({}),
      { cookie: await signedIn() },
    )

    expect(thrown).toBe(true)
    expect(response.status).toBe(503)
    expect(response.headers.getSetCookie()).toEqual([])
    expect(response.headers.get('Cache-Control')).toBe('private, no-store')
    expect(next).not.toHaveBeenCalled()
  })

  it.each([
    ['a returned response', async () => Response.json({}), false],
    [
      'a thrown response',
      async () => {
        throw new Response('Not found', { status: 404 })
      },
      true,
    ],
  ])('issues a missing CSRF cookie on %s', async (_, next, expectThrown) => {
    const { response, thrown } = await protect(hostedRuntimeConfig, next, {
      cookie: await signedIn({ withCsrf: false }),
    })

    expect(thrown).toBe(expectThrown)
    expect(cookieNames(response)).toEqual([csrfCookieName])
    expect(response.headers.get('Cache-Control')).toBe('private, no-store')
  })

  it('keeps a current CSRF cookie instead of reissuing it', async () => {
    const { response } = await protect(hostedRuntimeConfig, async () => Response.json({}), {
      cookie: await signedIn(),
    })

    expect(response.headers.getSetCookie()).toEqual([])
  })
})
