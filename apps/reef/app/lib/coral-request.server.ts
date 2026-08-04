import { createClient, type Interceptor } from '@connectrpc/connect'
import { createGrpcTransport } from '@connectrpc/connect-node'
import { createGrpcWebTransport } from '@connectrpc/connect-web'

import { CatalogService } from '@/generated/coral/v1/catalog_pb'
import { FunctionService } from '@/generated/coral/v1/functions_pb'
import { QueryService } from '@/generated/coral/v1/query_pb'
import { SourceService } from '@/generated/coral/v1/sources_pb'
import { TraceService } from '@/generated/coral/v1/traces_pb'
import { WorkspaceService } from '@/generated/coral/v1/workspaces_pb'

import { DEFAULT_DEV_CORAL_ENDPOINT } from './constants'
import { isExplicitLoopbackUrl } from './loopback.server'
import { isLocalDevOrigin, trimTrailingSlash } from './utils'

export function sourceClientForRequest(request: Request, accessToken: string | null = null) {
  return createClient(SourceService, coralTransportForRequest(request, accessToken))
}

export function workspaceClientForRequest(request: Request, accessToken: string | null = null) {
  return createClient(WorkspaceService, coralTransportForRequest(request, accessToken))
}

export function catalogClientForRequest(request: Request, accessToken: string | null = null) {
  return createClient(CatalogService, coralTransportForRequest(request, accessToken))
}

export function functionClientForRequest(request: Request, accessToken: string | null = null) {
  return createClient(FunctionService, coralTransportForRequest(request, accessToken))
}

export function queryClientForRequest(request: Request, accessToken: string | null = null) {
  return createClient(QueryService, coralTransportForRequest(request, accessToken))
}

export function traceClientForRequest(request: Request, accessToken: string | null = null) {
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
  if (!accessToken) return createGrpcWebTransport({ baseUrl })

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
  })
}

function bearerAuthInterceptor(accessToken: string): Interceptor {
  return (next) => async (rpcRequest) => {
    rpcRequest.header.set('Authorization', `Bearer ${accessToken}`)
    return next(rpcRequest)
  }
}
