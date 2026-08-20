import {
  Code,
  ConnectError,
  createClient,
  type Interceptor,
  type Transport,
} from '@connectrpc/connect'
import { createGrpcTransport, Http2SessionManager } from '@connectrpc/connect-node'

import { coralUIAuthConfig } from '@/auth/config.server'
import { CatalogService } from '@/generated/coral/v1/catalog_pb'
import { FeatureService } from '@/generated/coral/v1/features_pb'
import { FunctionService } from '@/generated/coral/v1/functions_pb'
import { GuiOnboardingService } from '@/generated/coral/v1/gui_onboarding_pb'
import { QueryService } from '@/generated/coral/v1/query_pb'
import { SourceService } from '@/generated/coral/v1/sources_pb'
import { TraceService } from '@/generated/coral/v1/traces_pb'
import { WorkspaceService } from '@/generated/coral/v1/workspaces_pb'
import { Health } from '@/generated/grpc/health/v1/health_pb'
import { expiredSessionRedirect } from '@/auth/response.server'

import { resolveCoralEndpoint } from './coral-endpoint.server'

export function sourceClientForRequest(request: Request, accessToken: string | null) {
  return createClient(SourceService, coralTransportForRequest(request, accessToken))
}

export function workspaceClientForRequest(request: Request, accessToken: string | null) {
  return createClient(WorkspaceService, coralTransportForRequest(request, accessToken))
}

export function catalogClientForRequest(request: Request, accessToken: string | null) {
  return createClient(CatalogService, coralTransportForRequest(request, accessToken))
}

export function featureClientForRequest(request: Request, accessToken: string | null) {
  return createClient(FeatureService, coralTransportForRequest(request, accessToken))
}

export function functionClientForRequest(request: Request, accessToken: string | null) {
  return createClient(FunctionService, coralTransportForRequest(request, accessToken))
}

export function guiOnboardingClientForRequest(request: Request, accessToken: string | null) {
  return createClient(GuiOnboardingService, coralTransportForRequest(request, accessToken))
}

export function queryClientForRequest(request: Request, accessToken: string | null) {
  return createClient(QueryService, coralTransportForRequest(request, accessToken))
}

export function traceClientForRequest(request: Request, accessToken: string | null) {
  return createClient(TraceService, coralTransportForRequest(request, accessToken))
}

/** Public readiness uses Coral's unauthenticated health service, never a user's bearer token. */
export function healthClientForRequest(request: Request) {
  return createClient(Health, coralHealthTransportForRequest(request))
}

function coralHealthTransportForRequest(request: Request) {
  const authMode = coralUIAuthConfig().mode
  const endpoint = resolveCoralEndpoint({
    authenticated: authMode !== 'disabled',
    request,
  })
  const { baseUrl } = endpoint
  if (authMode !== 'disabled') warnAuthenticatedCleartext(endpoint.authenticatedCleartextOrigin)
  return createGrpcTransport({
    baseUrl,
    sessionManager: coralSessionManager(new URL(baseUrl)),
  })
}

function coralTransportForRequest(request: Request, accessToken: string | null) {
  const authMode = coralUIAuthConfig().mode
  const endpoint = resolveCoralEndpoint({
    authenticated: authMode !== 'disabled' || accessToken !== null,
    request,
  })
  const { baseUrl } = endpoint
  if (!accessToken) {
    // Hosted Coral UI never reaches it: `_protected` proves a session before any
    // loader runs, so a null token in `required` mode is a caller that dropped
    // it. Falling through would send anonymous RPCs to a hosted Coral and
    // surface the result as an opaque transport error rather than an auth one.
    if (authMode !== 'disabled') {
      throw new Error('Coral authentication is required but this request carried no access token')
    }

    return createGrpcTransport({
      baseUrl,
      sessionManager: coralSessionManager(new URL(baseUrl)),
    })
  }

  warnAuthenticatedCleartext(endpoint.authenticatedCleartextOrigin)

  return redirectUnauthenticatedTransport(
    request,
    createGrpcTransport({
      baseUrl,
      interceptors: [bearerAuthInterceptor(accessToken)],
      sessionManager: coralSessionManager(new URL(baseUrl)),
    }),
  )
}

const warnedCleartextOrigins = new Set<string>()

function warnAuthenticatedCleartext(origin: string | null): void {
  if (!origin || warnedCleartextOrigins.has(origin)) return
  warnedCleartextOrigins.add(origin)
  console.warn(
    `CORAL_UI_ALLOW_INSECURE_CORAL_ENDPOINT sends Coral bearer tokens over cleartext HTTP to ${origin}; trust the entire Coral UI-to-Coral network path.`,
  )
}

// One HTTP/2 connection per Coral origin, for the lifetime of the process.
//
// `createGrpcTransport` builds its own `Http2SessionManager` when it is not
// given one, and a transport is built per client per request — so a single page
// navigation running loaders in parallel used to open a connection for each of
// them, and each lingered for the manager's fifteen-minute idle timeout. Coral UI
// talks to exactly one Coral in production, which is precisely the case HTTP/2
// multiplexing exists for.
//
// Keyed by origin because that is the granularity the manager itself binds to:
// it stores `new URL(url).origin` as its authority and refuses any request that
// does not match.
//
// One consequence worth knowing: concurrent Coral UI→Coral calls now share a
// session's stream limit and flow-control window instead of each holding their
// own. That is the normal shape for a service talking to one backend, but it
// means a future limit is reached globally rather than per request.
const coralSessionManagers = new Map<string, Http2SessionManager>()

function coralSessionManager(endpoint: URL): Http2SessionManager {
  const existing = coralSessionManagers.get(endpoint.origin)
  if (existing) return existing

  const created = new Http2SessionManager(endpoint.origin)
  coralSessionManagers.set(endpoint.origin, created)
  return created
}

function bearerAuthInterceptor(accessToken: string): Interceptor {
  return (next) => async (rpcRequest) => {
    rpcRequest.header.set('Authorization', `Bearer ${accessToken}`)
    return next(rpcRequest)
  }
}

function redirectUnauthenticatedTransport(request: Request, transport: Transport): Transport {
  return {
    ...transport,
    async unary(method, signal, timeoutMs, header, input, contextValues) {
      try {
        return await transport.unary(method, signal, timeoutMs, header, input, contextValues)
      } catch (error) {
        throwCoralError(request, error)
      }
    },
    async stream(method, signal, timeoutMs, header, input, contextValues) {
      let response
      try {
        response = await transport.stream(method, signal, timeoutMs, header, input, contextValues)
      } catch (error) {
        throwCoralError(request, error)
      }

      return {
        ...response,
        message: redirectUnauthenticatedMessages(request, response.message),
      }
    },
  }
}

async function* redirectUnauthenticatedMessages<T>(
  request: Request,
  messages: AsyncIterable<T>,
): AsyncIterable<T> {
  try {
    yield* messages
  } catch (error) {
    throwCoralError(request, error)
  }
}

function throwCoralError(request: Request, error: unknown): never {
  if (error instanceof ConnectError && error.code === Code.Unauthenticated) {
    throw expiredSessionRedirect(request)
  }
  throw error
}
