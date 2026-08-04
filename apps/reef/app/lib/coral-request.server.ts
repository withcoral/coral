import { createClient, type Interceptor } from '@connectrpc/connect'
import { createGrpcTransport, Http2SessionManager } from '@connectrpc/connect-node'
import { createGrpcWebTransport } from '@connectrpc/connect-web'

import { reefAuthConfig } from '@/auth/config.server'
import { CatalogService } from '@/generated/coral/v1/catalog_pb'
import { FunctionService } from '@/generated/coral/v1/functions_pb'
import { QueryService } from '@/generated/coral/v1/query_pb'
import { SourceService } from '@/generated/coral/v1/sources_pb'
import { TraceService } from '@/generated/coral/v1/traces_pb'
import { WorkspaceService } from '@/generated/coral/v1/workspaces_pb'

import { DEFAULT_DEV_CORAL_ENDPOINT } from './constants'
import { isExplicitLoopbackUrl } from './loopback.server'
import { isLocalDevOrigin, trimTrailingSlash } from './utils'

export function sourceClientForRequest(request: Request, accessToken: string | null) {
  return createClient(SourceService, coralTransportForRequest(request, accessToken))
}

export function workspaceClientForRequest(request: Request, accessToken: string | null) {
  return createClient(WorkspaceService, coralTransportForRequest(request, accessToken))
}

export function catalogClientForRequest(request: Request, accessToken: string | null) {
  return createClient(CatalogService, coralTransportForRequest(request, accessToken))
}

export function functionClientForRequest(request: Request, accessToken: string | null) {
  return createClient(FunctionService, coralTransportForRequest(request, accessToken))
}

export function queryClientForRequest(request: Request, accessToken: string | null) {
  return createClient(QueryService, coralTransportForRequest(request, accessToken))
}

export function traceClientForRequest(request: Request, accessToken: string | null) {
  return createClient(TraceService, coralTransportForRequest(request, accessToken))
}

export function coralEndpointForRequest(request: Request): string {
  const configured = process.env.CORAL_ENDPOINT?.trim()
  if (configured) return trimTrailingSlash(configured)

  if (process.env.NODE_ENV === 'production') {
    throw new Error('CORAL_ENDPOINT must be set in production')
  }

  const url = new URL(request.url)
  if (isLocalDevOrigin(url)) return DEFAULT_DEV_CORAL_ENDPOINT
  return url.origin
}

function coralTransportForRequest(request: Request, accessToken: string | null) {
  const baseUrl = coralEndpointForRequest(request)
  if (!accessToken) {
    // The transport follows the deployment topology, not whether a token
    // happened to be threaded here. `coral ui` — the Desktop sidecar, and the
    // local dev default — serves gRPC-Web only and answers `application/grpc`
    // with HTTP 415, so the unauthenticated local topology needs its own
    // transport and this branch cannot simply go away.
    //
    // Hosted Reef never reaches it: `_protected` proves a session before any
    // loader runs, so a null token in `required` mode is a caller that dropped
    // it. Falling through would send anonymous RPCs to a hosted Coral and
    // surface the result as an opaque transport error rather than an auth one.
    if (reefAuthConfig().mode !== 'disabled') {
      throw new Error('Coral authentication is required but this request carried no access token')
    }

    return createGrpcWebTransport({ baseUrl })
  }

  const endpoint = new URL(baseUrl)
  if (
    endpoint.protocol !== 'https:' &&
    !(endpoint.protocol === 'http:' && isExplicitLoopbackUrl(endpoint))
  ) {
    throw new Error(
      'CORAL_ENDPOINT must use HTTPS or explicit-loopback HTTP when Coral authentication is enabled',
    )
  }

  return createGrpcTransport({
    baseUrl,
    interceptors: [bearerAuthInterceptor(accessToken)],
    sessionManager: coralSessionManager(endpoint),
  })
}

// One HTTP/2 connection per Coral origin, for the lifetime of the process.
//
// `createGrpcTransport` builds its own `Http2SessionManager` when it is not
// given one, and a transport is built per client per request — so a single page
// navigation running loaders in parallel used to open a connection for each of
// them, and each lingered for the manager's fifteen-minute idle timeout. Reef
// talks to exactly one Coral in production, which is precisely the case HTTP/2
// multiplexing exists for.
//
// Keyed by origin because that is the granularity the manager itself binds to:
// it stores `new URL(url).origin` as its authority and refuses any request that
// does not match.
//
// One consequence worth knowing: concurrent Reef→Coral calls now share a
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
