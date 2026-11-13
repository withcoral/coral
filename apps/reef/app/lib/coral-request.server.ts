import type { RequestAuth } from '@/auth/types'

import { MethodOptions_IdempotencyLevel } from '@bufbuild/protobuf/wkt'
import { ConnectError, createClient, type Interceptor } from '@connectrpc/connect'
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

/** How long Reef waits for a Coral call by default. */
export const CORAL_CALL_TIMEOUT_MS = 10_000

/** Private first-party transport; used only by resource-specific server paths. */
export function managementTransportForRequest(
  auth: RequestAuth,
  endpoint: CoralEndpointPolicy,
  defaultTimeoutMs = CORAL_CALL_TIMEOUT_MS,
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
      resendAfterLostSession,
    ],
  })
}

/**
 * Coral can end the shared HTTP/2 session under any call: on a restart, or
 * when h2's rapid-reset guard sees a burst of cancelled calls. Node then fails
 * every call on that session with the same error, so Reef cannot tell which
 * calls Coral applied. Send a call once more only when its proto method
 * declares an `idempotency_level`, which says repeating it is safe.
 */
const resendAfterLostSession: Interceptor = (next) => async (call) => {
  try {
    return await next(call)
  } catch (error) {
    if (
      call.stream ||
      call.signal.aborted ||
      call.method.idempotency === MethodOptions_IdempotencyLevel.IDEMPOTENCY_UNKNOWN ||
      !sessionLost(error)
    ) {
      throw error
    }
    return next(call)
  }
}

function sessionLost(error: unknown): boolean {
  const cause = error instanceof ConnectError ? error.cause : undefined
  return cause instanceof Error && 'code' in cause && cause.code === 'ERR_HTTP2_SESSION_ERROR'
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
