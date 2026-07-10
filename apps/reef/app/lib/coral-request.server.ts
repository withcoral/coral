import { createClient } from '@connectrpc/connect'
import { createGrpcWebTransport } from '@connectrpc/connect-web'

import { CatalogService } from '@/generated/coral/v1/catalog_pb'
import { SourceService } from '@/generated/coral/v1/sources_pb'

import { DEFAULT_DEV_CORAL_ENDPOINT } from './constants'
import { isLocalDevOrigin, trimTrailingSlash } from './utils'

export function sourceClientForRequest(request: Request) {
  return createClient(
    SourceService,
    createGrpcWebTransport({ baseUrl: coralEndpointForRequest(request) }),
  )
}

export function catalogClientForRequest(request: Request) {
  return createClient(
    CatalogService,
    createGrpcWebTransport({ baseUrl: coralEndpointForRequest(request) }),
  )
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
