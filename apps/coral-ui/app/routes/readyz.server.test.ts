import { ConnectError, Code } from '@connectrpc/connect'
import { HealthCheckResponse_ServingStatus } from '@/generated/grpc/health/v1/health_pb'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const healthMocks = vi.hoisted(() => ({
  check: vi.fn(),
  healthClientForRequest: vi.fn(),
}))

vi.mock('@/lib/coral-request.server', () => ({
  healthClientForRequest: healthMocks.healthClientForRequest,
}))

import { assertCoralReady } from '@/lib/readiness.server'

import { loader } from './readyz'

describe('readiness route', () => {
  beforeEach(() => {
    healthMocks.healthClientForRequest.mockReturnValue({ check: healthMocks.check })
    healthMocks.healthClientForRequest.mockClear()
    healthMocks.check.mockReset()
  })

  afterEach(() => vi.unstubAllEnvs())

  it('returns ready only when Coral reports its engine health as serving', async () => {
    healthMocks.check.mockResolvedValue({ status: HealthCheckResponse_ServingStatus.SERVING })
    const request = new Request('http://attacker.example.test/readyz')

    const response = await loader({ request } as never)

    expect(healthMocks.healthClientForRequest).toHaveBeenCalledWith(request)
    expect(healthMocks.check).toHaveBeenCalledWith(
      expect.objectContaining({ service: 'coral.readiness' }),
      { signal: request.signal },
    )
    await expect(response.json()).resolves.toEqual({ coral: 'reachable', status: 'ok' })
  })

  it('rejects a non-serving engine status', async () => {
    healthMocks.check.mockResolvedValue({ status: HealthCheckResponse_ServingStatus.NOT_SERVING })

    await expect(
      loader({ request: new Request('http://coral-ui.test/readyz') } as never),
    ).rejects.toEqual(expect.objectContaining({ code: Code.Unavailable }))
  })

  it('propagates an upstream health-check failure instead of reporting ready', async () => {
    const upstream = new ConnectError('Coral unavailable', Code.Unavailable)
    healthMocks.check.mockRejectedValue(upstream)

    await expect(
      loader({ request: new Request('http://coral-ui.test/readyz') } as never),
    ).rejects.toBe(upstream)
  })

  it('keeps the server-only readiness implementation importable', () => {
    expect(assertCoralReady).toBeTypeOf('function')
  })
})
