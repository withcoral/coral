import { describe, expect, it } from 'vitest'

import { csrfTokenForRequest } from '@/auth/csrf.server'
import { publicRouteTestArgs } from '@/auth/server-context.test-helper'
import { commitCoralWebSession, oauthCookieName } from '@/auth/session.server'
import { hostedRuntimeConfig, requiredAuthConfig } from '@/auth/test-config'

import { action } from './logout'

const session = {
  accessToken: 'server-only-token',
  expiresAt: 4_102_444_800,
  tokenType: 'Bearer',
}

describe('logout route', () => {
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
  })

  it('clears hosted auth state and lands on the signed-out screen', async () => {
    const { cookie, csrf } = await signedIn()

    const response = await action(
      publicRouteTestArgs(logoutRequest(csrf, cookie), hostedRuntimeConfig),
    )

    expect(response.status).toBe(302)
    expect(response.headers.get('Location')).toBe('/login?signedOut=1')
    expect(response.headers.get('Cache-Control')).toBe('private, no-store')
    expect(response.headers.get('Vary')).toBe('Cookie')
    const cleared = response.headers.getSetCookie()
    expect(cleared.map(cookieName).toSorted()).toEqual(
      [
        `${requiredAuthConfig.cookieName}_csrf`,
        oauthCookieName(requiredAuthConfig),
        requiredAuthConfig.cookieName,
      ].toSorted(),
    )
    for (const setCookie of cleared) expect(setCookie).toMatch(/(?:^|; )Max-Age=0(?:;|$)/)
  })
})

/** A real signed session and its CSRF token, as the browser would hold them. */
async function signedIn(): Promise<{ cookie: string; csrf: string }> {
  const sessionCookie = firstPair(await commitCoralWebSession(session, requiredAuthConfig))
  const { setCookie, token } = await csrfTokenForRequest(
    new Request('https://coral-web.example.test/', { headers: { cookie: sessionCookie } }),
    requiredAuthConfig,
    session,
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
