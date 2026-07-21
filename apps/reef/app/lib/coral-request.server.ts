import { createClient, type Interceptor } from '@connectrpc/connect'
import { createGrpcWebTransport } from '@connectrpc/connect-web'

import { CatalogService } from '@/generated/coral/v1/catalog_pb'
import { QueryService } from '@/generated/coral/v1/query_pb'
import { SourceService } from '@/generated/coral/v1/sources_pb'
import { TraceService } from '@/generated/coral/v1/traces_pb'
import { WorkspaceService } from '@/generated/coral/v1/workspaces_pb'

import { DEFAULT_DEV_CORAL_ENDPOINT } from './constants'
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
  if (accessToken && new URL(baseUrl).protocol !== 'https:') {
    throw new Error('CORAL_ENDPOINT must use HTTPS when Coral authentication is enabled')
  }

  return createGrpcWebTransport({
    baseUrl,
    interceptors: accessToken ? [bearerAuthInterceptor(accessToken)] : undefined,
  })
}

function bearerAuthInterceptor(accessToken: string): Interceptor {
  return (next) => async (rpcRequest) => {
    rpcRequest.header.set('Authorization', `Bearer ${accessToken}`)
    return next(rpcRequest)
  }
}
