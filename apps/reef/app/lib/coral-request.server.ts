import type { RequestAuth } from '@/auth/types'

import { createClient } from '@connectrpc/connect'
import { createGrpcTransport, Http2SessionManager } from '@connectrpc/connect-node'
import { context, propagation, type TextMapSetter } from '@opentelemetry/api'

import { Health } from '@/generated/grpc/health/v1/health_pb'

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
      (next) => (call) => {
        call.header.set('coral-audience', 'reef')
        // The request's span is Coral's parent; without one, Coral starts the trace.
        propagation.inject(context.active(), call.header, headerSetter)
        if (auth.accessToken) call.header.set('authorization', `Bearer ${auth.accessToken}`)
        return next(call)
      },
    ],
  })
}

const headerSetter: TextMapSetter<Headers> = {
  set: (headers, name, value) => headers.set(name, value),
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
