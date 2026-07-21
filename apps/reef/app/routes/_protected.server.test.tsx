import { createRequestHandler, RouterContextProvider, type ServerBuild } from 'react-router'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import type { RequiredAuthConfig } from '@/auth/types'

const authMocks = vi.hoisted(() => ({
  clearReefSession: vi.fn(async () => 'reef_session=; Max-Age=0; Path=/; HttpOnly'),
  csrfTokenForRequest: vi.fn(),
  readReefSession: vi.fn(),
  reefAuthConfig: vi.fn(),
}))

vi.mock('@/auth/config.server', () => ({ reefAuthConfig: authMocks.reefAuthConfig }))
vi.mock('@/auth/csrf.server', () => ({ csrfTokenForRequest: authMocks.csrfTokenForRequest }))
vi.mock('@/auth/session.server', () => ({
  clearReefSession: authMocks.clearReefSession,
  readReefSession: authMocks.readReefSession,
}))

import { requestAuthContext } from '@/auth/server-context'

import { middleware } from './_protected'

const requiredConfig: RequiredAuthConfig = {
  clientId: 'coral-cloud-ui',
  cookieName: 'reef_session',
  cookieSecure: true,
  issuer: 'https://login.example.test',
  mode: 'required',
  redirectUri: 'https://reef.example.test/auth/callback',
  scope: 'coral:mcp',
  sessionMaxAgeSeconds: 3600,
  sessionSecret: '0123456789abcdef0123456789abcdef',
}
const session = {
  accessToken: 'server-only-token',
  expiresAt: 4_102_444_800,
  tokenType: 'Bearer',
}
const descendantLoader = vi.fn(() => ({ ok: true }))
const descendantAction = vi.fn(() => ({ ok: true }))
const routeComponent = () => null

const handlerBuild = {
  assets: {
    entry: { imports: [], module: '' },
    routes: {},
    url: '',
    version: '',
  },
  assetsBuildDirectory: '',
  basename: '/',
  entry: {
    module: {
      default: async () => new Response('document'),
    },
  },
  future: {
    v8_middleware: true,
    v8_passThroughRequests: false,
    v8_trailingSlashAwareDataRequests: false,
  },
  isSpaMode: false,
  prerender: [],
  publicPath: '/',
  routeDiscovery: { manifestPath: '/__manifest', mode: 'lazy' },
  routes: {
    root: {
      id: 'root',
      module: { default: routeComponent },
      path: '',
    },
    'routes/_protected': {
      id: 'routes/_protected',
      module: { default: routeComponent, middleware },
      parentId: 'root',
    },
    'routes/protected': {
      id: 'routes/protected',
      module: {
        action: descendantAction,
        default: routeComponent,
        loader: descendantLoader,
      },
      parentId: 'routes/_protected',
      path: 'protected',
    },
  },
  ssr: true,
} as unknown as ServerBuild

describe('optional auth boundary', () => {
  beforeEach(() => {
    authMocks.clearReefSession.mockClear()
    authMocks.csrfTokenForRequest.mockReset()
    authMocks.csrfTokenForRequest.mockResolvedValue({ setCookie: null, token: 'csrf-token' })
    authMocks.readReefSession.mockReset()
    authMocks.reefAuthConfig.mockReset()
    descendantAction.mockClear()
    descendantLoader.mockClear()
  })

  it('is a true no-op for disabled local and desktop requests', async () => {
    authMocks.reefAuthConfig.mockReturnValue({ mode: 'disabled' })
    const context = new RouterContextProvider()
    const response = new Response('local app')
    const next = vi.fn(async () => response)

    await expect(runMiddleware(new Request('http://localhost:5173/'), next, context)).resolves.toBe(
      response,
    )
    expect(next).toHaveBeenCalledOnce()
    expect(authMocks.readReefSession).not.toHaveBeenCalled()
    expect(authMocks.clearReefSession).not.toHaveBeenCalled()
    expect(authMocks.csrfTokenForRequest).not.toHaveBeenCalled()
    expect(context.get(requestAuthContext)).toEqual({ accessToken: null, mode: 'disabled' })
    expect(response.headers.has('Cache-Control')).toBe(false)
  })

  it.each([
    ['document', 'GET', 'https://reef.example.test/workspaces/analytics/sources'],
    ['data', 'GET', 'https://reef.example.test/workspaces/analytics/sources.data?index'],
    ['action', 'POST', 'https://reef.example.test/workspaces'],
    ['resource action', 'POST', 'https://reef.example.test/sources/github/oauth-install'],
  ])(
    'redirects an anonymous required-auth %s request before descendants',
    async (_, method, url) => {
      authMocks.reefAuthConfig.mockReturnValue(requiredConfig)
      authMocks.readReefSession.mockResolvedValue(null)
      const next = vi.fn()

      const thrown = await runMiddleware(new Request(url, { method }), next).catch(
        (error: unknown) => error,
      )

      expect(thrown).toBeInstanceOf(Response)
      expect((thrown as Response).status).toBe(302)
      const requested = new URL(url)
      expect((thrown as Response).headers.get('location')).toBe(
        `/login?returnTo=${encodeURIComponent(`${requested.pathname}${requested.search}`)}`,
      )
      expectPrivate(thrown as Response)
      expect(next).not.toHaveBeenCalled()
    },
  )

  it.each([
    ['document', 'GET', '/protected'],
    ['data loader', 'GET', '/protected.data'],
    ['data action', 'POST', '/protected.data'],
  ])(
    'blocks anonymous required-auth %s requests in the React Router handler',
    async (_, method, pathname) => {
      authMocks.reefAuthConfig.mockReturnValue(requiredConfig)
      authMocks.readReefSession.mockResolvedValue(null)
      const handleRequest = createRequestHandler(handlerBuild, 'test')

      const response = await handleRequest(
        new Request(`https://reef.example.test${pathname}`, { method }),
      )

      const routedPathname = pathname.replace(/\.data$/, '')
      const loginLocation = `/login?returnTo=${encodeURIComponent(routedPathname)}`
      if (pathname.endsWith('.data')) {
        expect(response.status).toBe(202)
        expect(await response.text()).toContain(loginLocation)
      } else {
        expect(response.status).toBe(302)
        expect(response.headers.get('location')).toBe(loginLocation)
      }
      expect(descendantLoader).not.toHaveBeenCalled()
      expect(descendantAction).not.toHaveBeenCalled()
    },
  )

  it('clears an unreadable session cookie on the login redirect', async () => {
    authMocks.reefAuthConfig.mockReturnValue(requiredConfig)
    authMocks.readReefSession.mockResolvedValue(null)

    const thrown = await runMiddleware(
      new Request('https://reef.example.test/', {
        headers: { cookie: 'other=value; reef_session=unreadable' },
      }),
      vi.fn(),
    ).catch((error: unknown) => error)

    expect(authMocks.clearReefSession).toHaveBeenCalledWith(requiredConfig)
    expect((thrown as Response).headers.get('Set-Cookie')).toContain('Max-Age=0')
  })

  it('keeps the token only in server request context', async () => {
    authMocks.reefAuthConfig.mockReturnValue(requiredConfig)
    authMocks.readReefSession.mockResolvedValue(session)
    const context = new RouterContextProvider()

    await runMiddleware(
      new Request('https://reef.example.test/workspaces/analytics/sources'),
      async () => new Response('hosted app'),
      context,
    )

    expect(context.get(requestAuthContext)).toEqual({
      accessToken: 'server-only-token',
      csrfToken: 'csrf-token',
      mode: 'required',
      session,
    })
  })

  it('commits a fresh CSRF cookie on the protected response', async () => {
    authMocks.reefAuthConfig.mockReturnValue(requiredConfig)
    authMocks.readReefSession.mockResolvedValue(session)
    authMocks.csrfTokenForRequest.mockResolvedValue({
      setCookie: 'reef_csrf=signed; Path=/; HttpOnly; SameSite=Lax',
      token: 'fresh-csrf-token',
    })

    const response = await runMiddleware(
      new Request('https://reef.example.test/'),
      async () => new Response('ok'),
    )

    expect(response.headers.get('Set-Cookie')).toContain('reef_csrf=signed')

    const childError = new Response('failed', { status: 500 })
    const thrown = await runMiddleware(new Request('https://reef.example.test/'), async () => {
      throw childError
    }).catch((error: unknown) => error)
    expect((thrown as Response).headers.get('Set-Cookie')).toContain('reef_csrf=signed')
  })

  it('marks normal and thrown hosted responses private and non-cacheable', async () => {
    authMocks.reefAuthConfig.mockReturnValue(requiredConfig)
    authMocks.readReefSession.mockResolvedValue(session)

    const response = await runMiddleware(
      new Request('https://reef.example.test/'),
      async () => new Response('ok'),
    )
    expectPrivate(response)

    const childRedirect = new Response(null, { headers: { location: '/elsewhere' }, status: 302 })
    const thrown = await runMiddleware(new Request('https://reef.example.test/'), async () => {
      throw childRedirect
    }).catch((error: unknown) => error)
    expect(thrown).toBe(childRedirect)
    expectPrivate(thrown as Response)
  })
})

async function runMiddleware(
  request: Request,
  next: () => Promise<Response>,
  context = new RouterContextProvider(),
): Promise<Response> {
  return (await middleware[0]({ context, params: {}, request } as never, next)) as Response
}

function expectPrivate(response: Response): void {
  expect(response.headers.get('Cache-Control')).toBe('private, no-store')
  expect(response.headers.get('Vary')).toContain('Cookie')
}
