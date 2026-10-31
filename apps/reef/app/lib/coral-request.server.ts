import type { RequestAuth } from '@/auth/types'

import { createClient } from '@connectrpc/connect'
import { createGrpcTransport, Http2SessionManager } from '@connectrpc/connect-node'

import { Health } from '@/generated/grpc/health/v1/health_pb'

import { traceCoralCall } from '../../server-tracing.js'

import type { CoralEndpointPolicy } from './coral-endpoint.server'

/** Public readiness uses Coral's unauthenticated health service. */
export function healthClient(endpoint: CoralEndpointPolicy) {
  warnAuthenticatedCleartext(endpoint.authenticatedCleartextOrigin)
  return createClient(
    Health,
    createGrpcTransport({
      baseUrl: endpoint.baseUrl,
      sessionManager: coralSessionManager(new URL(endpoint.baseUrl)),
    }),
  )
}

/** Private first-party transport; used only by resource-specific server paths. */
export function managementTransportForRequest(
  auth: RequestAuth,
  endpoint: CoralEndpointPolicy,
  defaultTimeoutMs = 10_000,
) {
  warnAuthenticatedCleartext(endpoint.authenticatedCleartextOrigin)
  return createGrpcTransport({
    baseUrl: endpoint.baseUrl,
    sessionManager: coralSessionManager(new URL(endpoint.baseUrl)),
    defaultTimeoutMs,
    interceptors: [
      traceCoralCall,
      (next) => (call) => {
        call.header.set('coral-audience', 'reef')
        if (auth.accessToken) call.header.set('authorization', `Bearer ${auth.accessToken}`)
        return next(call)
      },
    ],
  })
}

const warnedCleartextOrigins = new Set<string>()

function warnAuthenticatedCleartext(origin: string | null): void {
  if (!origin || warnedCleartextOrigins.has(origin)) return
  warnedCleartextOrigins.add(origin)
  console.warn(
    `CORAL_WEB_ALLOW_INSECURE_CORAL_ENDPOINT uses cleartext HTTP to ${origin}; trust the entire Coral web-to-Coral network path.`,
  )
}

// One HTTP/2 connection per Coral origin for the lifetime of the process.
const coralSessionManagers = new Map<string, Http2SessionManager>()

function coralSessionManager(endpoint: URL): Http2SessionManager {
  const existing = coralSessionManagers.get(endpoint.origin)
  if (existing) return existing

  const created = new Http2SessionManager(endpoint.origin)
  coralSessionManagers.set(endpoint.origin, created)
  return created
}
