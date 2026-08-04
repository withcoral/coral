import type { Interceptor } from '@connectrpc/connect'
import type { GrpcTransportOptions } from '@connectrpc/connect-node'
import type { GrpcWebTransportOptions } from '@connectrpc/connect-web'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const transportMocks = vi.hoisted(() => ({
  createClient: vi.fn((_service, transport) => transport),
  createGrpcTransport: vi.fn((options) => options),
  createGrpcWebTransport: vi.fn((options) => options),
}))

vi.mock('@connectrpc/connect', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@connectrpc/connect')>()),
  createClient: transportMocks.createClient,
}))
vi.mock('@connectrpc/connect-node', () => ({
  createGrpcTransport: transportMocks.createGrpcTransport,
}))
vi.mock('@connectrpc/connect-web', () => ({
  createGrpcWebTransport: transportMocks.createGrpcWebTransport,
}))

import {
  catalogClientForRequest,
  functionClientForRequest,
  queryClientForRequest,
  sourceClientForRequest,
  traceClientForRequest,
  workspaceClientForRequest,
} from './coral-request.server'

const request = new Request('http://localhost:5173/workspaces/analytics/sources')
const clientFactories = [
  catalogClientForRequest,
  functionClientForRequest,
  queryClientForRequest,
  sourceClientForRequest,
  traceClientForRequest,
  workspaceClientForRequest,
]

describe('request-scoped Coral transport authentication', () => {
  beforeEach(() => {
    vi.stubEnv('CORAL_ENDPOINT', 'https://coral.example.test')
    transportMocks.createClient.mockClear()
    transportMocks.createGrpcTransport.mockClear()
    transportMocks.createGrpcWebTransport.mockClear()
  })

  afterEach(() => {
    vi.unstubAllEnvs()
  })

  it('uses native gRPC and adds the server-held bearer token to every Coral RPC', async () => {
    for (const clientFactory of clientFactories) {
      const transport = clientFactory(
        request,
        'coral-access-token',
      ) as unknown as GrpcTransportOptions
      const [interceptor] = transport.interceptors ?? []

      expect(transport.baseUrl).toBe('https://coral.example.test')
      expect(interceptor).toBeTypeOf('function')
      expect(await authorizationHeader(interceptor)).toBe('Bearer coral-access-token')
    }
    expect(transportMocks.createGrpcTransport).toHaveBeenCalledTimes(clientFactories.length)
    expect(transportMocks.createGrpcWebTransport).not.toHaveBeenCalled()
  })

  it('keeps local and desktop calls on the existing unauthenticated gRPC-Web transport', () => {
    vi.stubEnv('CORAL_ENDPOINT', 'http://127.0.0.1:50051')

    for (const clientFactory of clientFactories) {
      const transport = clientFactory(request, null) as unknown as GrpcWebTransportOptions

      expect(transport).toEqual({ baseUrl: 'http://127.0.0.1:50051' })
    }
    expect(transportMocks.createGrpcWebTransport).toHaveBeenCalledTimes(clientFactories.length)
    expect(transportMocks.createGrpcTransport).not.toHaveBeenCalled()
  })

  it.each([
    'http://localhost:50051',
    'http://127.42.0.1:50051',
    'http://[::1]:50051',
    'http://[::ffff:127.0.0.1]:50051',
  ])('allows authenticated native gRPC to an explicit-loopback HTTP endpoint: %s', (endpoint) => {
    vi.stubEnv('CORAL_ENDPOINT', endpoint)

    const transport = sourceClientForRequest(
      request,
      'coral-access-token',
    ) as unknown as GrpcTransportOptions

    expect(transport.baseUrl).toBe(endpoint)
    expect(transportMocks.createGrpcTransport).toHaveBeenCalledOnce()
    expect(transportMocks.createGrpcWebTransport).not.toHaveBeenCalled()
  })

  it.each([
    'http://coral.example.test',
    'http://192.168.1.10:50051',
    'http://10.0.0.10:50051',
    'http://preview.localhost:50051',
  ])(
    'rejects a non-loopback cleartext endpoint before attaching a bearer token: %s',
    (endpoint) => {
      vi.stubEnv('CORAL_ENDPOINT', endpoint)

      expect(() => sourceClientForRequest(request, 'coral-access-token')).toThrow(
        'CORAL_ENDPOINT must use HTTPS or explicit-loopback HTTP when Coral authentication is enabled',
      )
      expect(transportMocks.createGrpcTransport).not.toHaveBeenCalled()
      expect(transportMocks.createGrpcWebTransport).not.toHaveBeenCalled()
    },
  )
})

async function authorizationHeader(interceptor: Interceptor): Promise<string | null> {
  const next = vi.fn(async (rpcRequest) => rpcRequest)
  const rpcRequest = { header: new Headers() }

  await interceptor(next as never)(rpcRequest as never)
  return rpcRequest.header.get('Authorization')
}
