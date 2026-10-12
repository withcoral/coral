import type { GrpcTransportOptions } from '@connectrpc/connect-node'
import { afterEach, describe, expect, it, vi } from 'vitest'

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

const attackerRequest = new Request('https://attacker.example.test/readyz', {
  headers: { Host: 'attacker.example.test', 'X-Forwarded-Host': 'attacker.example.test' },
})

describe('Coral transport', () => {
  afterEach(() => vi.restoreAllMocks())

  it('management uses the configured endpoint, not request hosts', () => {
    const transport = managementTransportForRequest(
      attackerRequest,
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
})
