import type { Interceptor } from '@connectrpc/connect'
import type { GrpcTransportOptions } from '@connectrpc/connect-node'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

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

import {
  catalogClientForRequest,
  functionClientForRequest,
  healthClientForRequest,
  queryClientForRequest,
  sourceClientForRequest,
  traceClientForRequest,
  workspaceClientForRequest,
} from './coral-request.server'

const request = new Request('http://localhost:5173/workspaces/analytics/sources')
const attackerRequest = new Request('https://attacker.example.test/workspaces/analytics/sources', {
  headers: { Host: 'attacker.example.test', 'X-Forwarded-Host': 'attacker.example.test' },
})
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
    transportMocks.Http2SessionManager.mockClear()
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.unstubAllEnvs()
  })

  it('uses native gRPC and adds the server-held bearer token to every Coral RPC', async () => {
    for (const clientFactory of clientFactories) {
      const transport = clientFactory(
        attackerRequest,
        'coral-access-token',
      ) as unknown as GrpcTransportOptions
      const [interceptor] = transport.interceptors ?? []

      expect(transport.baseUrl).toBe('https://coral.example.test')
      expect(interceptor).toBeTypeOf('function')
      expect(await authorizationHeader(interceptor)).toBe('Bearer coral-access-token')
    }
    expect(transportMocks.createGrpcTransport).toHaveBeenCalledTimes(clientFactories.length)
  })

  it('requires CORAL_ENDPOINT under auth instead of trusting request or forwarded hosts', () => {
    vi.stubEnv('CORAL_ENDPOINT', '')
    vi.stubEnv('CORAL_UI_AUTH_MODE', 'required')
    vi.stubEnv('CORAL_UI_AUTH_ISSUER', 'https://auth.example.test')
    vi.stubEnv('CORAL_UI_PUBLIC_URL', 'https://coral-ui.example.test')
    vi.stubEnv('CORAL_UI_SESSION_SECRET', '0123456789abcdef0123456789abcdef')

    expect(() => sourceClientForRequest(attackerRequest, 'coral-access-token')).toThrow(
      'CORAL_ENDPOINT must be set when Coral authentication is enabled',
    )
    expect(transportMocks.createGrpcTransport).not.toHaveBeenCalled()
  })

  it('uses native gRPC for unauthenticated local and Desktop calls', () => {
    // Pinned rather than inherited: this case passes on an empty environment by
    // luck, and would flip on any machine that exports CORAL_UI_AUTH_MODE=required.
    vi.stubEnv('CORAL_UI_AUTH_MODE', 'disabled')
    vi.stubEnv('CORAL_ENDPOINT', 'http://127.0.0.1:50051')

    for (const clientFactory of clientFactories) {
      const transport = clientFactory(request, null) as unknown as GrpcTransportOptions

      expect(transport.baseUrl).toBe('http://127.0.0.1:50051')
      expect(transport.interceptors).toBeUndefined()
    }
    expect(transportMocks.createGrpcTransport).toHaveBeenCalledTimes(clientFactories.length)
  })

  it('uses native gRPC for unauthenticated health checks in local and Desktop mode', () => {
    vi.stubEnv('CORAL_UI_AUTH_MODE', 'disabled')
    vi.stubEnv('CORAL_ENDPOINT', 'http://127.0.0.1:50051')

    const transport = healthClientForRequest(request) as unknown as GrpcTransportOptions

    expect(transport.baseUrl).toBe('http://127.0.0.1:50051')
    expect(transport.interceptors).toBeUndefined()
    expect(transportMocks.createGrpcTransport).toHaveBeenCalledOnce()
  })

  it('reuses one native HTTP/2 session for hosted health checks without a bearer token', () => {
    vi.stubEnv('CORAL_UI_AUTH_MODE', 'required')
    vi.stubEnv('CORAL_UI_AUTH_ISSUER', 'https://auth.example.test')
    vi.stubEnv('CORAL_UI_PUBLIC_URL', 'https://coral-ui.example.test')
    vi.stubEnv('CORAL_UI_SESSION_SECRET', '0123456789abcdef0123456789abcdef')
    vi.stubEnv('CORAL_ENDPOINT', 'https://health.coral.example.test')

    const first = healthClientForRequest(request) as unknown as GrpcTransportOptions
    const second = healthClientForRequest(request) as unknown as GrpcTransportOptions

    expect(first.baseUrl).toBe('https://health.coral.example.test')
    expect(first.interceptors).toBeUndefined()
    expect(first.sessionManager).toBe(second.sessionManager)
    expect(transportMocks.Http2SessionManager).toHaveBeenCalledOnce()
    expect(transportMocks.createGrpcTransport).toHaveBeenCalledTimes(2)
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
  })

  it('allows opted-in h2c with native bearer transport and warns once per origin', () => {
    vi.stubEnv('CORAL_ENDPOINT', 'http://coral.internal:50051/rpc')
    vi.stubEnv('CORAL_UI_ALLOW_INSECURE_CORAL_ENDPOINT', ' TrUe ')
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => undefined)

    for (const clientFactory of clientFactories) {
      const transport = clientFactory(
        request,
        'coral-access-token',
      ) as unknown as GrpcTransportOptions
      expect(transport.baseUrl).toBe('http://coral.internal:50051/rpc')
    }

    expect(transportMocks.createGrpcTransport).toHaveBeenCalledTimes(clientFactories.length)
    expect(warn).toHaveBeenCalledOnce()
    expect(warn).toHaveBeenCalledWith(
      expect.stringContaining('bearer tokens over cleartext HTTP to http://coral.internal:50051'),
    )
  })

  // The transport follows the deployment topology, not whether a token reached
  // this call. In hosted mode `_protected` has already proven a session, so a
  // null token is a caller that dropped it — and the old code answered that by
  // quietly building an unauthenticated transport and letting Coral reject the
  // RPC, which reads as a transport fault rather than the threading bug it is.
  it('refuses to build an unauthenticated transport when auth is required', () => {
    vi.stubEnv('CORAL_UI_AUTH_MODE', 'required')
    vi.stubEnv('CORAL_UI_AUTH_ISSUER', 'https://coral.example.test')
    vi.stubEnv('CORAL_UI_PUBLIC_URL', 'https://coral-ui.example.test')
    vi.stubEnv('CORAL_UI_SESSION_SECRET', '0123456789abcdef0123456789abcdef')

    for (const clientFactory of clientFactories) {
      expect(() => clientFactory(request, null)).toThrow(
        'Coral authentication is required but this request carried no access token',
      )
    }
    expect(transportMocks.createGrpcTransport).not.toHaveBeenCalled()
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
    },
  )
})

async function authorizationHeader(interceptor: Interceptor): Promise<string | null> {
  const next = vi.fn(async (rpcRequest) => rpcRequest)
  const rpcRequest = { header: new Headers() }

  await interceptor(next as never)(rpcRequest as never)
  return rpcRequest.header.get('Authorization')
}
