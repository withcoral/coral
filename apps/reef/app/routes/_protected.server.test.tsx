import { create } from '@bufbuild/protobuf'
import { createRequestHandler, RouterContextProvider, type ServerBuild } from 'react-router'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import type { RequiredAuthConfig } from '@/auth/types'
import {
  OAuthCredentialMethodSchema,
  SourceCredentialMethodSchema,
  SourceCredentialSchema,
  SourceInfoSchema,
  SourceInputSpecSchema,
  SourceOrigin,
  SourceSecretInputSchema,
  type CreateBundledSourceWithOAuthResponse,
} from '@/generated/coral/v1/sources_pb'
import { EXPIRED_SESSION_RESPONSE_HEADER } from '@/auth/response.server'
import { expiredSessionRedirect } from '@/auth/response.server'
import {
  AUTH_STREAM_REQUEST_HEADER,
  AUTH_STREAM_RETURN_TO_HEADER,
  EXPIRED_SESSION_LOGIN_HEADER,
} from '@/auth/response'

const authMocks = vi.hoisted(() => ({
  clearReefSession: vi.fn(async () => 'reef_session=; Max-Age=0; Path=/; HttpOnly'),
  csrfTokenForRequest: vi.fn(),
  readReefSession: vi.fn(),
  reefAuthConfig: vi.fn(),
}))
const appShellMocks = vi.hoisted(() => ({ listWorkspacesForRequest: vi.fn() }))
const coralMocks = vi.hoisted(() => {
  const getSource = vi.fn()
  const getSourceInfo = vi.fn()
  return {
    getSource,
    getSourceInfo,
    sourceClientForRequest: vi.fn((_request: Request, _accessToken: string | null) => ({
      getSource,
      getSourceInfo,
    })),
  }
})

vi.mock('@/auth/config.server', () => ({ reefAuthConfig: authMocks.reefAuthConfig }))
vi.mock('@/auth/csrf.server', () => ({ csrfTokenForRequest: authMocks.csrfTokenForRequest }))
vi.mock('@/auth/session.server', () => ({
  clearReefSession: authMocks.clearReefSession,
  readReefSession: authMocks.readReefSession,
}))
vi.mock('@/lib/coral-request.server', () => ({
  sourceClientForRequest: coralMocks.sourceClientForRequest,
}))
vi.mock('@/lib/workspaces.server', () => ({
  listWorkspacesForRequest: appShellMocks.listWorkspacesForRequest,
}))

import { requestAuthContext } from '@/auth/server-context'
import { runOAuthInstallFlow } from '@/lib/source-oauth-install-flow'

import { middleware } from './_protected'
import { loader as appShellLoader } from './app-shell'
import { loader as sourceDetailLoader } from './source-detail'
import { action as oauthInstallAction } from './source-oauth-install'

const requiredConfig: RequiredAuthConfig = {
  cookieName: 'reef_session',
  issuer: 'https://coral.example.test',
  mode: 'required',
  publicUrl: 'https://reef.example.test',
  sessionMaxAgeSeconds: 3600,
  sessionSecret: '0123456789abcdef0123456789abcdef',
}
const session = {
  accessToken: 'server-only-token',
  expiresAt: 4_102_444_800,
  tokenType: 'Bearer',
}
const oauthSourceInfo = create(SourceInfoSchema, {
  inputs: [
    create(SourceInputSpecSchema, {
      input: {
        case: 'secret',
        value: create(SourceSecretInputSchema, {
          credential: create(SourceCredentialSchema, {
            methods: [
              create(SourceCredentialMethodSchema, {
                method: { case: 'oauth', value: create(OAuthCredentialMethodSchema) },
              }),
            ],
          }),
        }),
      },
      key: 'GITHUB_TOKEN',
      required: true,
    }),
  ],
  name: 'github',
  origin: SourceOrigin.BUNDLED,
})
const descendantLoader = vi.fn((_args: { request: Request }) => ({ ok: true }))
const descendantAction = vi.fn((_args: { request: Request }) => ({ ok: true }))
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
    'routes/app-shell': {
      id: 'routes/app-shell',
      module: { default: routeComponent, loader: appShellLoader },
      parentId: 'routes/_protected',
    },
    'routes/source-detail': {
      id: 'routes/source-detail',
      module: { default: routeComponent, loader: sourceDetailLoader },
      parentId: 'routes/app-shell',
      path: 'workspaces/:workspaceId/sources/:sourceName',
    },
    'routes/source-oauth-install': {
      id: 'routes/source-oauth-install',
      module: { action: oauthInstallAction },
      parentId: 'routes/_protected',
      path: 'workspaces/:workspaceId/sources/:sourceName/oauth-install',
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
    appShellMocks.listWorkspacesForRequest.mockReset()
    appShellMocks.listWorkspacesForRequest.mockResolvedValue([])
    descendantAction.mockClear()
    descendantLoader.mockClear()
    coralMocks.getSource.mockReset()
    coralMocks.getSource.mockResolvedValue({ source: undefined })
    coralMocks.getSourceInfo.mockReset()
    coralMocks.getSourceInfo.mockResolvedValue({
      sourceInfo: create(SourceInfoSchema, {
        installed: false,
        name: 'github',
        origin: SourceOrigin.BUNDLED,
      }),
    })
    coralMocks.sourceClientForRequest.mockReset()
    coralMocks.sourceClientForRequest.mockImplementation(() => ({
      getSource: coralMocks.getSource,
      getSourceInfo: coralMocks.getSourceInfo,
    }))
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

  it('clears a Coral-rejected session through the real React Router middleware pipeline', async () => {
    authMocks.reefAuthConfig.mockReturnValue(requiredConfig)
    authMocks.readReefSession.mockResolvedValue(session)
    descendantLoader.mockImplementationOnce(({ request }: { request: Request }) => {
      throw expiredSessionRedirect(request)
    })
    const handleRequest = createRequestHandler(handlerBuild, 'test')

    const response = await handleRequest(
      new Request('https://reef.example.test/protected?tab=data'),
    )

    expect(response.status).toBe(302)
    expect(response.headers.get('location')).toBe('/login?returnTo=%2Fprotected%3Ftab%3Ddata')
    expect(response.headers.has(EXPIRED_SESSION_RESPONSE_HEADER)).toBe(false)
    expect(response.headers.get('Set-Cookie')).toContain('Max-Age=0')
    expect(authMocks.clearReefSession).toHaveBeenCalledOnce()
    expectPrivate(response)
  })

  it('converts a Coral-rejected stream action in the real middleware pipeline', async () => {
    authMocks.reefAuthConfig.mockReturnValue(requiredConfig)
    authMocks.readReefSession.mockResolvedValue(session)
    descendantAction.mockImplementationOnce(({ request }: { request: Request }) => {
      throw expiredSessionRedirect(request)
    })
    const handleRequest = createRequestHandler(handlerBuild, 'test')

    const response = await handleRequest(
      new Request('https://reef.example.test/protected', {
        headers: {
          [AUTH_STREAM_REQUEST_HEADER]: '1',
          [AUTH_STREAM_RETURN_TO_HEADER]: '/workspaces/analytics/sources/new',
        },
        method: 'POST',
      }),
    )

    expect(response.status).toBe(401)
    expect(response.headers.get(EXPIRED_SESSION_LOGIN_HEADER)).toBe(
      '/login?returnTo=%2Fworkspaces%2Fanalytics%2Fsources%2Fnew',
    )
    expect(response.headers.get('Set-Cookie')).toContain('Max-Age=0')
    expect(authMocks.clearReefSession).toHaveBeenCalledOnce()
    expectPrivate(response)
  })

  it('navigates when the OAuth action expires inside the protected route pipeline', async () => {
    authMocks.reefAuthConfig.mockReturnValue(requiredConfig)
    authMocks.readReefSession.mockResolvedValue(session)
    coralMocks.getSourceInfo.mockResolvedValue({ sourceInfo: oauthSourceInfo })
    coralMocks.sourceClientForRequest.mockImplementation(
      (request: Request, _accessToken: string | null) => ({
        createBundledSourceWithOAuth: () => rejectedResponses(expiredSessionRedirect(request)),
        getSource: coralMocks.getSource,
        getSourceInfo: coralMocks.getSourceInfo,
      }),
    )
    const handleRequest = createRequestHandler(handlerBuild, 'test')
    let serverResponse: Response | undefined
    const fetchOAuthInstall = vi.fn<typeof fetch>(async (input, init) => {
      const inputUrl =
        typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      serverResponse = await handleRequest(
        new Request(new URL(inputUrl, 'https://reef.example.test'), init),
      )
      return serverResponse
    })
    const navigateToLogin = vi.fn()
    const formData = new FormData()
    formData.set('method:GITHUB_TOKEN', '0')
    formData.set('name', 'github')

    await runOAuthInstallFlow({
      endpoint: '/workspaces/analytics/sources/github/oauth-install',
      fetchOAuthInstall,
      formData,
      navigateToLogin,
      onComplete: vi.fn(),
      openAuthorization: vi.fn(),
      setError: vi.fn(),
      setProgress: vi.fn(),
      signal: new AbortController().signal,
      visibleLocation: '/workspaces/analytics/sources/new?step=oauth',
    })

    expect(coralMocks.sourceClientForRequest).toHaveBeenCalledWith(
      expect.any(Request),
      session.accessToken,
    )
    expect(authMocks.clearReefSession).toHaveBeenCalledOnce()
    expect(serverResponse?.status).toBe(401)
    expect(serverResponse?.headers.get(EXPIRED_SESSION_LOGIN_HEADER)).toBe(
      '/login?returnTo=%2Fworkspaces%2Fanalytics%2Fsources%2Fnew%3Fstep%3Doauth',
    )
    expect(serverResponse?.headers.has('Location')).toBe(false)
    expect(serverResponse?.headers.get('Set-Cookie')).toContain('Max-Age=0')
    expect(serverResponse?.bodyUsed).toBe(false)
    expect(navigateToLogin).toHaveBeenCalledWith(
      '/login?returnTo=%2Fworkspaces%2Fanalytics%2Fsources%2Fnew%3Fstep%3Doauth',
    )
  })

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

  it('keeps the server-held access token out of the serialized route response', async () => {
    authMocks.reefAuthConfig.mockReturnValue(requiredConfig)
    authMocks.readReefSession.mockResolvedValue(session)
    const handleRequest = createRequestHandler(handlerBuild, 'test')

    const response = await handleRequest(
      new Request('https://reef.example.test/workspaces/analytics/sources/github.data'),
    )

    expect(response.status).toBe(200)
    expect(coralMocks.sourceClientForRequest).toHaveBeenCalledWith(
      expect.any(Request),
      session.accessToken,
    )
    expect(appShellMocks.listWorkspacesForRequest).toHaveBeenCalledWith(
      expect.any(Request),
      session.accessToken,
    )
    const serializedResponse = [
      await response.text(),
      ...Array.from(response.headers.entries(), ([name, value]) => `${name}: ${value}`),
    ].join('\n')
    expect(serializedResponse).toContain('github')
    expect(serializedResponse).not.toContain(session.accessToken)
  })

  it('commits a fresh CSRF cookie on the protected response', async () => {
    authMocks.reefAuthConfig.mockReturnValue(requiredConfig)
    authMocks.readReefSession.mockResolvedValue(session)
    authMocks.csrfTokenForRequest.mockResolvedValue({
      setCookie: 'reef_session_csrf=signed; Path=/; HttpOnly; SameSite=Lax',
      token: 'fresh-csrf-token',
    })

    const response = await runMiddleware(
      new Request('https://reef.example.test/'),
      async () => new Response('ok'),
    )

    expect(response.headers.get('Set-Cookie')).toContain('reef_session_csrf=signed')

    const childError = new Response('failed', { status: 500 })
    const thrown = await runMiddleware(new Request('https://reef.example.test/'), async () => {
      throw childError
    }).catch((error: unknown) => error)
    expect((thrown as Response).headers.get('Set-Cookie')).toContain('reef_session_csrf=signed')
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

  it('uses the same client-visible signal when the encrypted session is already expired', async () => {
    authMocks.reefAuthConfig.mockReturnValue(requiredConfig)
    authMocks.readReefSession.mockResolvedValue(null)
    const next = vi.fn()

    const thrown = await runMiddleware(
      new Request('https://reef.example.test/workspaces/analytics/sources/oauth-import', {
        headers: {
          cookie: 'reef_session=expired',
          [AUTH_STREAM_REQUEST_HEADER]: '1',
          [AUTH_STREAM_RETURN_TO_HEADER]: '/workspaces/analytics/sources/new',
        },
        method: 'POST',
      }),
      next,
    ).catch((error: unknown) => error)

    expect((thrown as Response).status).toBe(401)
    expect((thrown as Response).headers.get(EXPIRED_SESSION_LOGIN_HEADER)).toBe(
      '/login?returnTo=%2Fworkspaces%2Fanalytics%2Fsources%2Fnew',
    )
    expect((thrown as Response).headers.get('Set-Cookie')).toContain('Max-Age=0')
    expect(next).not.toHaveBeenCalled()
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

function rejectedResponses(error: unknown): AsyncIterable<CreateBundledSourceWithOAuthResponse> {
  return {
    [Symbol.asyncIterator]: () => ({ next: () => Promise.reject(error) }),
  }
}
