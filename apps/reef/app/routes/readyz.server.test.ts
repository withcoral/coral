import { ConnectError, Code } from '@connectrpc/connect'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const transportMocks = vi.hoisted(() => ({
  createClient: vi.fn(),
  createGrpcTransport: vi.fn((options) => options),
  listCatalog: vi.fn(),
}))

vi.mock('@connectrpc/connect', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@connectrpc/connect')>()),
  createClient: transportMocks.createClient,
}))
vi.mock('@connectrpc/connect-node', () => ({
  createGrpcTransport: transportMocks.createGrpcTransport,
}))

import { assertCoralReady } from '@/lib/readiness.server'

import { loader } from './readyz'

describe('readiness route', () => {
  beforeEach(() => {
    vi.stubEnv('CORAL_ENDPOINT', 'https://coral.example.test')
    transportMocks.createClient.mockReturnValue({ listCatalog: transportMocks.listCatalog })
    transportMocks.createGrpcTransport.mockClear()
    transportMocks.listCatalog.mockReset()
  })

  afterEach(() => vi.unstubAllEnvs())

  it('returns ready only after ListCatalog succeeds against the configured Coral endpoint', async () => {
    transportMocks.listCatalog.mockResolvedValue({ items: [] })
    const request = new Request('http://attacker.example.test/readyz')

    const response = await loader({ request } as never)

    expect(transportMocks.createGrpcTransport).toHaveBeenCalledWith({
      baseUrl: 'https://coral.example.test',
    })
    expect(transportMocks.listCatalog).toHaveBeenCalledWith(
      expect.objectContaining({ workspace: expect.objectContaining({ name: 'default' }) }),
      { signal: request.signal },
    )
    await expect(response.json()).resolves.toEqual({ coral: 'reachable', status: 'ok' })
  })

  it('propagates an upstream ListCatalog failure instead of reporting ready', async () => {
    const upstream = new ConnectError('Coral unavailable', Code.Unavailable)
    transportMocks.listCatalog.mockRejectedValue(upstream)

    await expect(loader({ request: new Request('http://reef.test/readyz') } as never)).rejects.toBe(
      upstream,
    )
  })

  it('keeps the route adapter wired to the server-only readiness implementation', () => {
    expect(assertCoralReady).toBeTypeOf('function')
  })
})
