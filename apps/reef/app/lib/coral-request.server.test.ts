import type { Interceptor, UnaryRequest } from '@connectrpc/connect'
import type { GrpcTransportOptions } from '@connectrpc/connect-node'
import { context, propagation, ROOT_CONTEXT, trace, TraceFlags } from '@opentelemetry/api'
import { AsyncLocalStorageContextManager } from '@opentelemetry/context-async-hooks'
import { W3CTraceContextPropagator } from '@opentelemetry/core'
import { afterEach, beforeAll, afterAll, describe, expect, it, vi } from 'vitest'

const transportMocks = vi.hoisted(() => ({
  createClient: vi.fn((_service, transport) => transport),
  createGrpcTransport: vi.fn((options) => options),
  Http2SessionManager: vi.fn(function (this: { authority: string }, authority: string) {
    this.authority = authority
  }),
}))

vi.mock('@connectrpc/connect', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@connectrpc/connect')>()),
  createClient: transportMocks.createClient,
}))
vi.mock('@connectrpc/connect-node', () => ({
  createGrpcTransport: transportMocks.createGrpcTransport,
  Http2SessionManager: transportMocks.Http2SessionManager,
}))

import { hostedRuntimeConfig } from '@/auth/test-config'

import { healthClient, managementTransportForRequest } from './coral-request.server'

/** The headers one call carries through the management transport. */
async function callHeaders(): Promise<Headers> {
  const transport = managementTransportForRequest(
    { mode: 'disabled', accessToken: null },
    hostedRuntimeConfig.endpoint,
  ) as unknown as GrpcTransportOptions
  const [interceptor] = transport.interceptors as Interceptor[]
  const call = { header: new Headers() } as UnaryRequest
  const next = vi.fn(async () => undefined as never)
  await interceptor!(next)(call)
  return call.header
}

describe('Coral transport', () => {
  afterEach(() => vi.restoreAllMocks())

  it('management uses the configured endpoint', () => {
    const transport = managementTransportForRequest(
      { mode: 'disabled', accessToken: null },
      hostedRuntimeConfig.endpoint,
    ) as unknown as GrpcTransportOptions
    expect(transport.baseUrl).toBe(hostedRuntimeConfig.endpoint.baseUrl)
  })

  it('warns once when authenticated topology opts into cleartext', () => {
    const origin = 'http://coral.internal:50051'
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => undefined)

    healthClient({ authenticatedCleartextOrigin: origin, baseUrl: origin })
    healthClient({ authenticatedCleartextOrigin: origin, baseUrl: origin })

    expect(warn).toHaveBeenCalledOnce()
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('cleartext HTTP'))
  })

  describe('trace context', () => {
    beforeAll(() => {
      context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable())
      propagation.setGlobalPropagator(new W3CTraceContextPropagator())
    })
    afterAll(() => {
      context.disable()
      propagation.disable()
    })

    it('carries the active span as the parent of each Coral call', async () => {
      const traceId = '0af7651916cd43dd8448eb211c80319c'
      const spanId = 'b7ad6b7169203331'
      const active = trace.setSpanContext(ROOT_CONTEXT, {
        spanId,
        traceFlags: TraceFlags.SAMPLED,
        traceId,
      })

      const headers = await context.with(active, callHeaders)

      expect(headers.get('traceparent')).toBe(`00-${traceId}-${spanId}-01`)
    })

    it('sends no parent when no span is active', async () => {
      expect((await callHeaders()).get('traceparent')).toBeNull()
    })
  })
})
