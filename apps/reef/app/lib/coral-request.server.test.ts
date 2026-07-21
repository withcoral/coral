import type { Interceptor } from '@connectrpc/connect'
import type { GrpcWebTransportOptions } from '@connectrpc/connect-web'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const transportMocks = vi.hoisted(() => ({
  createClient: vi.fn((_service, transport) => transport),
  createGrpcWebTransport: vi.fn((options) => options),
}))

vi.mock('@connectrpc/connect', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@connectrpc/connect')>()),
  createClient: transportMocks.createClient,
}))
vi.mock('@connectrpc/connect-web', () => ({
  createGrpcWebTransport: transportMocks.createGrpcWebTransport,
}))

import {
  catalogClientForRequest,
  queryClientForRequest,
  sourceClientForRequest,
  traceClientForRequest,
  workspaceClientForRequest,
} from './coral-request.server'

const request = new Request('http://localhost:5173/workspaces/analytics/sources')
const clientFactories = [
  catalogClientForRequest,
  queryClientForRequest,
  sourceClientForRequest,
  traceClientForRequest,
  workspaceClientForRequest,
]

describe('request-scoped Coral transport authentication', () => {
  beforeEach(() => {
    vi.stubEnv('CORAL_ENDPOINT', 'https://coral.example.test')
    transportMocks.createClient.mockClear()
    transportMocks.createGrpcWebTransport.mockClear()
  })

  afterEach(() => {
    vi.unstubAllEnvs()
  })

  it('adds the server-held bearer token to every Coral RPC', async () => {
    for (const clientFactory of clientFactories) {
      const transport = clientFactory(
        request,
        'coral-access-token',
      ) as unknown as GrpcWebTransportOptions
      const [interceptor] = transport.interceptors ?? []

      expect(interceptor).toBeTypeOf('function')
      expect(await authorizationHeader(interceptor)).toBe('Bearer coral-access-token')
    }
  })

  it('keeps local and desktop Coral RPCs unauthenticated', () => {
    vi.stubEnv('CORAL_ENDPOINT', 'http://127.0.0.1:50051')

    for (const clientFactory of clientFactories) {
      const transport = clientFactory(request, null) as unknown as GrpcWebTransportOptions

      expect(transport.interceptors).toBeUndefined()
    }
  })

  it('rejects cleartext Coral endpoints before attaching a bearer token', () => {
    vi.stubEnv('CORAL_ENDPOINT', 'http://coral.example.test')

    expect(() => sourceClientForRequest(request, 'coral-access-token')).toThrow(
      'CORAL_ENDPOINT must use HTTPS when Coral authentication is enabled',
    )
    expect(transportMocks.createGrpcWebTransport).not.toHaveBeenCalled()
  })
})

async function authorizationHeader(interceptor: Interceptor): Promise<string | null> {
  const next = vi.fn(async (rpcRequest) => rpcRequest)
  const rpcRequest = { header: new Headers() }

  await interceptor(next as never)(rpcRequest as never)
  return rpcRequest.header.get('Authorization')
}
