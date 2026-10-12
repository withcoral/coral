import { renderToStaticMarkup } from 'react-dom/server'
import { createStaticHandler, createStaticRouter, StaticRouterProvider } from 'react-router'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { startCoralOAuthLogin } from '@/auth/coral-oauth.server'
import { localRuntimeConfig, publicRouteTestArgs } from '@/auth/server-context.test-helper'
import { oauthCookieName, readCoralWebSession } from '@/auth/session.server'
import { hostedRuntimeConfig, requiredAuthConfig } from '@/auth/test-config'

import AuthCallback, { ErrorBoundary, loader } from './auth.callback'

const config = requiredAuthConfig
const metadata = {
  authorization_endpoint: `${config.issuer}/authorize`,
  issuer: config.issuer,
  token_endpoint: `${config.issuer}/token`,
}

// Serves discovery, then answers the token exchange with `token`.
const fetch = vi.fn<typeof globalThis.fetch>()
let token: () => Promise<Response>

describe('auth callback route', () => {
  beforeEach(() => {
    token = async () => Response.json({ access_token: 'opaque-coral-token', expires_in: 3600 })
    fetch.mockReset()
    fetch.mockImplementation(async (input) =>
      String(input) === metadata.token_endpoint ? token() : Response.json(metadata),
    )
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
      readCoralWebSession(
        new Request('https://coral-web.example.test/', {
          headers: { cookie: session!.split(';', 1)[0] },
        }),
        config,
      ),
    ).resolves.toMatchObject({ accessToken: 'opaque-coral-token' })
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
      expect(context.errors?.callback).toMatchObject(
        kind === 'response'
          ? { status: 400, data: 'provider detail: access_token=private-token-canary' }
          : { message: detail },
      )
      const router = createStaticRouter(handler.dataRoutes, context)
      const markup = renderToStaticMarkup(
        <StaticRouterProvider router={router} context={context} hydrate={false} />,
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

/** Starts a real sign-in, then builds the issuer's redirect back to Reef. */
async function callbackRequest({
  returnTo = '/',
  state,
  error,
  description,
}: { returnTo?: string; state?: string; error?: string; description?: string } = {}) {
  const login = await startCoralOAuthLogin(
    new Request(`https://coral-web.example.test/login?returnTo=${encodeURIComponent(returnTo)}`),
    config,
  )
  const issued = new URL(login.headers.get('location')!).searchParams.get('state')!
  const query = new URLSearchParams({ state: state ?? issued })
  if (error) query.set('error', error)
  if (description) query.set('error_description', description)
  if (!error) query.set('code', 'authorization-code')
  return new Request(`https://coral-web.example.test/auth/callback?${query}`, {
    headers: { cookie: login.headers.getSetCookie()[0].split(';', 1)[0] },
  })
}
