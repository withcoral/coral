import { renderToStaticMarkup } from 'react-dom/server'
import { createStaticHandler, createStaticRouter, StaticRouterProvider } from 'react-router'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { startCoralOAuthLogin } from '@/auth/coral-oauth.server'
import { localRuntimeConfig, publicRouteTestArgs } from '@/auth/server-context.test-helper'
import {
  commitCoralWebSession,
  coralWebSessionHandle,
  oauthCookieName,
} from '@/auth/session.server'
import { hostedRuntimeConfig, requiredAuthConfig } from '@/auth/test-config'

import AuthCallback, { ErrorBoundary, loader } from './auth.callback'

const config = requiredAuthConfig
const metadata = {
  authorization_endpoint: `${config.issuer}/authorize`,
  coral_web_session_endpoint: `${config.issuer}/oauth/session`,
  issuer: config.issuer,
  token_endpoint: `${config.issuer}/token`,
}
const endEndpoint = `${metadata.coral_web_session_endpoint}/end`

// Serves discovery, then answers the token exchange with `token`.
const fetch = vi.fn<typeof globalThis.fetch>()
let token: () => Promise<Response>
/** How Coral answers ending a session. */
let endAnswer: number

describe('auth callback route', () => {
  beforeEach(() => {
    token = async () =>
      Response.json({ access_token: 'opaque', expires_in: 3600, refresh_token: 'coral_rt_r' })
    endAnswer = 200
    fetch.mockReset()
    fetch.mockImplementation(async (input) => {
      if (String(input) === metadata.token_endpoint) return token()
      if (String(input) === metadata.coral_web_session_endpoint) {
        return Response.json({ expires_in: 3600, session: 'coral_ws_handle' }, { status: 201 })
      }
      if (String(input) === endEndpoint) return new Response(null, { status: endAnswer })
      return Response.json(metadata)
    })
    vi.stubGlobal('fetch', fetch)
  })

  afterEach(() => vi.unstubAllGlobals())

  it('is unavailable when local or desktop auth is disabled', async () => {
    const request = await callbackRequest()
    fetch.mockClear()

    const thrown = await loader(publicRouteTestArgs(request, localRuntimeConfig)).catch(
      (error: unknown) => error,
    )

    expect(thrown).toBeInstanceOf(Response)
    expect((thrown as Response).status).toBe(404)
    expect(fetch).not.toHaveBeenCalled()
  })

  it('stores the session and returns to the page that started sign-in', async () => {
    const response = await loader(
      hostedArgs(await callbackRequest({ returnTo: '/workspaces/analytics/traces' })),
    )

    expect(response.status).toBe(302)
    expect(response.headers.get('location')).toBe('/workspaces/analytics/traces')
    expect(response.headers.get('Cache-Control')).toBe('private, no-store')
    const cookies = response.headers.getSetCookie()
    expect(cookies.find((cookie) => cookie.startsWith(`${oauthCookieName(config)}=`))).toMatch(
      /Max-Age=0/,
    )
    const session = cookies.find((cookie) => cookie.startsWith(`${config.cookieName}=`))
    await expect(
      coralWebSessionHandle(
        new Request('https://coral-web.example.test/', {
          headers: { cookie: session!.split(';', 1)[0] },
        }),
        config,
      ),
    ).resolves.toBe('coral_ws_handle')
    expect(endedHandles()).toEqual([])
  })

  // One rule for every sign-in over a session, also when the same person signs in.
  it.each([
    [200, false],
    [503, true],
  ])(
    'ends the session a sign-in replaces at Coral; Coral answering %i keeps it to finish: %s',
    async (status, pending) => {
      endAnswer = status
      const response = await loader(hostedArgs(await callbackRequest({ replaced: true })))

      expect(response.status).toBe(302)
      expect(endedHandles()).toEqual(['coral_ws_replaced'])
      const cookies = response.headers.getSetCookie()
      const session = cookies.find((cookie) => cookie.startsWith(`${config.cookieName}=`))
      await expect(
        coralWebSessionHandle(
          new Request('https://coral-web.example.test/', {
            headers: { cookie: session!.split(';', 1)[0] },
          }),
          config,
        ),
      ).resolves.toBe('coral_ws_handle')
      const signOut = cookies.find((cookie) => cookie.startsWith(`${config.cookieName}_signout=`))
      expect(signOut).toBeDefined()
      expect(/Max-Age=0(?:;|$)/.test(signOut!)).toBe(!pending)
    },
  )

  it('keeps the session when the sign-in over it fails', async () => {
    const thrown = await loader(
      hostedArgs(await callbackRequest({ error: 'access_denied', replaced: true })),
    ).catch((error: unknown) => error)

    expect((thrown as Response).status).toBe(400)
    expect(endedHandles()).toEqual([])
    expect(
      (thrown as Response).headers
        .getSetCookie()
        .filter((cookie) => cookie.startsWith(config.cookieName)),
    ).toEqual([])
  })

  const code = 'sign_in_refused'
  const time = '2026-11-25T10:15:42Z'
  it.each([
    ['access_denied', `${code} ${time}`, true],
    ['access_denied', `${'a'.repeat(64)} ${time}`, true],
    ['server_error', `${code} ${time}`, false],
    ['access_denied', `${code}  ${time}`, false],
    ['access_denied', `${code} ${time} extra-detail`, false],
    ['access_denied', `Sign_in_refused ${time}`, false],
    ['access_denied', `${'a'.repeat(65)} ${time}`, false],
    ['access_denied', `${code} 2026-02-30T10:15:42Z`, false],
    ['access_denied', `${code} 2026-11-25T10:15:42.123Z`, false],
    ['access_denied', `${code} 2026-11-25T10:15:42+01:00`, false],
    ['access_denied', code, false],
  ])('shows the code and the time for %s "%s": %s', async (error, description, shown) => {
    const { markup } = await renderCallbackPage(await callbackRequest({ error, description }))

    expect(markup).toContain('Sign-in failed')
    if (shown) {
      const [shownCode, shownTime] = description.split(' ')
      expect(markup).toContain(`>${shownCode}<`)
      expect(markup).toContain(`>${shownTime}<`)
    } else {
      for (const part of description.split(' ').filter(Boolean)) {
        expect(markup).not.toContain(part)
      }
    }
  })

  it('marks expected callback failures private before the error boundary handles them', async () => {
    const thrown = await loader(hostedArgs(await callbackRequest({ state: 'forged-state' }))).catch(
      (error: unknown) => error,
    )

    expect(thrown).toBeInstanceOf(Response)
    expect((thrown as Response).status).toBe(400)
    expect((thrown as Response).headers.get('Cache-Control')).toBe('private, no-store')
    expect((thrown as Response).headers.get('Vary')).toContain('Cookie')
  })

  it.each(['response', 'exception'] as const)(
    'renders an accessible generic error without details from a callback %s',
    async (kind) => {
      const detail = 'provider detail access_token=private-token-canary'
      if (kind === 'exception') token = () => Promise.reject(new Error(detail))
      const request = await callbackRequest(
        kind === 'response'
          ? { error: 'provider detail', description: 'access_token=private-token-canary' }
          : {},
      )
      const { error, markup } = await renderCallbackPage(request)
      expect(error).toMatchObject(
        kind === 'response'
          ? { status: 400, data: 'provider detail: access_token=private-token-canary' }
          : { message: detail },
      )

      expect(markup).toContain('aria-labelledby="auth-error-title"')
      expect(markup).toContain('Sign-in failed')
      expect(markup).toContain('href="/login"')
      expect(markup).toContain('Try again')
      expect(markup).not.toContain('provider detail')
      expect(markup).not.toContain('access_token')
      expect(markup).not.toContain('private-token-canary')
    },
  )
})

function hostedArgs(request: Request) {
  return publicRouteTestArgs(request, hostedRuntimeConfig)
}

/** The handles of the sessions Reef asked Coral to end. */
function endedHandles(): (string | null)[] {
  return fetch.mock.calls
    .filter(([input]) => String(input) === endEndpoint)
    .map(([, init]) => new URLSearchParams(String(init?.body)).get('session'))
}

/** Runs the callback through the router and renders the page it ends on. */
async function renderCallbackPage(request: Request) {
  const handler = createStaticHandler([
    {
      id: 'callback',
      path: '/auth/callback',
      loader: () => loader(hostedArgs(request)),
      Component: AuthCallback,
      ErrorBoundary,
    },
  ])
  const context = await handler.query(request)
  if (context instanceof Response) throw new Error('Expected the callback error boundary')
  const router = createStaticRouter(handler.dataRoutes, context)
  const markup = renderToStaticMarkup(
    <StaticRouterProvider router={router} context={context} hydrate={false} />,
  )
  return { error: context.errors?.callback, markup }
}

/**
 * Starts a real sign-in, then builds the issuer's redirect back to Reef. With
 * `replaced`, the browser also holds a session the sign-in replaces.
 */
async function callbackRequest({
  returnTo = '/',
  state,
  error,
  description,
  replaced = false,
}: {
  returnTo?: string
  state?: string
  error?: string
  description?: string
  replaced?: boolean
} = {}) {
  const login = await startCoralOAuthLogin(
    new Request(`https://coral-web.example.test/login?returnTo=${encodeURIComponent(returnTo)}`),
    config,
  )
  const issued = new URL(login.headers.get('location')!).searchParams.get('state')!
  const query = new URLSearchParams({ state: state ?? issued })
  if (error) query.set('error', error)
  if (description) query.set('error_description', description)
  if (!error) query.set('code', 'authorization-code')
  const cookies = [login.headers.getSetCookie()[0].split(';', 1)[0]]
  if (replaced) {
    const session = await commitCoralWebSession(
      {
        endpoint: metadata.coral_web_session_endpoint,
        expiresIn: 3600,
        handle: 'coral_ws_replaced',
      },
      config,
    )
    cookies.push(session.split(';', 1)[0])
  }
  return new Request(`https://coral-web.example.test/auth/callback?${query}`, {
    headers: { cookie: cookies.join('; ') },
  })
}
