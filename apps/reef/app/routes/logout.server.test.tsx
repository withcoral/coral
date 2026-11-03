import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { csrfTokenForRequest } from '@/auth/csrf.server'
import { publicRouteTestArgs } from '@/auth/server-context.test-helper'
import { commitCoralWebSession, oauthCookieName } from '@/auth/session.server'
import { hostedRuntimeConfig, requiredAuthConfig } from '@/auth/test-config'

import { action } from './logout'

const config = requiredAuthConfig
const started = {
  endpoint: `${config.issuer}/oauth/session`,
  expiresIn: 3600,
  handle: 'coral_ws_handle',
}

/** Coral's answers to session requests, by URL. */
const coral = vi.fn<typeof globalThis.fetch>()

describe('logout route', () => {
  beforeEach(() => {
    coral.mockReset()
    coral.mockImplementation(async () => new Response(null, { status: 200 }))
    vi.stubGlobal('fetch', coral)
  })

  afterEach(() => vi.unstubAllGlobals())

  it('keeps disabled local and desktop logout inert', async () => {
    const { cookie, csrf } = await signedIn()

    const response = await action(publicRouteTestArgs(logoutRequest(csrf, cookie)))

    expect(response.headers.get('Location')).toBe('/')
    expect(response.headers.getSetCookie()).toEqual([])
  })

  it('rejects non-POST mutations', async () => {
    const request = new Request('https://coral-web.example.test/logout', { method: 'DELETE' })

    await expect(action(publicRouteTestArgs(request, hostedRuntimeConfig))).rejects.toMatchObject({
      status: 405,
    })
  })

  it.each([
    ['an invalid CSRF token', async () => ({ ...(await signedIn()), csrf: 'invalid-token' })],
    [
      'no active session',
      async () => {
        const { cookie, csrf } = await signedIn()
        // Keep only the CSRF cookie: its token is valid but no session backs it.
        return { cookie: cookie.split('; ')[1], csrf }
      },
    ],
  ])('rejects a hosted logout with %s and clears nothing', async (_, credentials) => {
    const { cookie, csrf } = await credentials()

    const thrown = await action(
      publicRouteTestArgs(logoutRequest(csrf, cookie), hostedRuntimeConfig),
    ).catch((error: unknown) => error)

    expect(thrown).toBeInstanceOf(Response)
    expect((thrown as Response).status).toBe(403)
    expect((thrown as Response).headers.getSetCookie()).toEqual([])
    expect(coral).not.toHaveBeenCalled()
  })

  it('ends the session at Coral, clears hosted auth state, and lands on the signed-out screen', async () => {
    const { cookie, csrf } = await signedIn()

    const response = await action(
      publicRouteTestArgs(logoutRequest(csrf, cookie), hostedRuntimeConfig),
    )

    expect(coral).toHaveBeenCalledTimes(1)
    expect(coral.mock.calls[0]?.[0]).toBe(`${started.endpoint}/end`)
    expect(response.status).toBe(302)
    expect(response.headers.get('Location')).toBe('/login?signedOut=1')
    expect(response.headers.get('Cache-Control')).toBe('private, no-store')
    expect(response.headers.get('Vary')).toBe('Cookie')
    const cleared = response.headers.getSetCookie()
    expect(cleared.map(cookieName).toSorted()).toEqual(
      [
        `${config.cookieName}_csrf`,
        `${config.cookieName}_signout`,
        oauthCookieName(config),
        config.cookieName,
      ].toSorted(),
    )
    for (const setCookie of cleared) expect(setCookie).toMatch(/(?:^|; )Max-Age=0(?:;|$)/)
  })

  // The browser loses the session even when Coral does not answer; a pending
  // sign-out cookie lets the next visit finish ending it at Coral.
  it('signs the browser out and keeps the sign-out to finish when Coral does not answer', async () => {
    coral.mockImplementation(async () => Response.json({ error: 'server_error' }, { status: 503 }))
    const { cookie, csrf } = await signedIn()

    const response = await action(
      publicRouteTestArgs(logoutRequest(csrf, cookie), hostedRuntimeConfig),
    )

    expect(response.headers.get('Location')).toBe('/login?signedOut=1')
    const set = response.headers.getSetCookie()
    const pending = set.find((value) => cookieName(value) === `${config.cookieName}_signout`)
    expect(pending).toBeDefined()
    expect(pending).not.toMatch(/Max-Age=0(?:;|$)/)
    const session = set.find((value) => cookieName(value) === config.cookieName)
    expect(session).toMatch(/Max-Age=0(?:;|$)/)
  })
})

/** A real signed session and its CSRF token, as the browser would hold them. */
async function signedIn(): Promise<{ cookie: string; csrf: string }> {
  const sessionCookie = firstPair(await commitCoralWebSession(started, config))
  const { setCookie, token } = await csrfTokenForRequest(
    new Request('https://coral-web.example.test/', { headers: { cookie: sessionCookie } }),
    config,
    started,
  )
  return { cookie: `${sessionCookie}; ${firstPair(setCookie!)}`, csrf: token }
}

function logoutRequest(csrf: string, cookie: string): Request {
  return new Request('https://coral-web.example.test/logout', {
    body: new URLSearchParams({ csrf }),
    headers: { cookie },
    method: 'POST',
  })
}

function firstPair(setCookie: string): string {
  return setCookie.split(';', 1)[0]
}

function cookieName(cookie: string): string {
  return cookie.slice(0, cookie.indexOf('='))
}
