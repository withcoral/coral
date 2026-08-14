import { Code, ConnectError, type Transport } from '@connectrpc/connect'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const transportMocks = vi.hoisted(() => ({
  createGrpcTransport: vi.fn(),
  // The transport shares one session per Coral origin, so the module imports
  // this by value. A mock factory that omits an export makes vitest throw on
  // first access rather than return undefined.
  Http2SessionManager: vi.fn(),
}))

vi.mock('@connectrpc/connect-node', () => ({
  createGrpcTransport: transportMocks.createGrpcTransport,
  Http2SessionManager: transportMocks.Http2SessionManager,
}))

import { sourceClientForRequest, workspaceClientForRequest } from './coral-request.server'

const request = new Request('https://coral-ui.example.test/workspaces/analytics/sources')

describe('authenticated Coral client expiry boundary', () => {
  beforeEach(() => {
    vi.stubEnv('CORAL_ENDPOINT', 'https://coral.example.test')
    transportMocks.createGrpcTransport.mockReset()
  })

  afterEach(() => {
    vi.unstubAllEnvs()
  })

  it('maps a normalized unary authentication failure after the generated client boundary', async () => {
    const unary = vi.fn().mockRejectedValue(new ConnectError('expired', Code.Unauthenticated))
    transportMocks.createGrpcTransport.mockReturnValue(fakeTransport({ unary }))

    const thrown = await workspaceClientForRequest(request, 'access-token')
      .listWorkspaces({})
      .catch((error: unknown) => error)

    expectExpiredSessionRedirect(thrown)
    expect(unary).toHaveBeenCalledOnce()
  })

  it('maps a normalized trailers-only stream failure after the generated client boundary', async () => {
    const stream = vi.fn().mockResolvedValue({
      header: new Headers(),
      message: rejectedMessages(new ConnectError('expired', Code.Unauthenticated)),
      trailer: new Headers(),
    })
    transportMocks.createGrpcTransport.mockReturnValue(fakeTransport({ stream }))

    const messages = sourceClientForRequest(request, 'access-token').createBundledSourceWithOAuth(
      {} as never,
    )
    const thrown = await collect(messages).catch((error: unknown) => error)

    expectExpiredSessionRedirect(thrown)
    expect(stream).toHaveBeenCalledOnce()
  })

  it('preserves normalized non-authentication failures without retrying', async () => {
    const failure = new ConnectError('unavailable', Code.Unavailable)
    const unary = vi.fn().mockRejectedValue(failure)
    transportMocks.createGrpcTransport.mockReturnValue(fakeTransport({ unary }))

    await expect(
      workspaceClientForRequest(request, 'access-token').listWorkspaces({}),
    ).rejects.toBe(failure)
    expect(unary).toHaveBeenCalledOnce()
  })
})

function fakeTransport(overrides: {
  stream?: ReturnType<typeof vi.fn>
  unary?: ReturnType<typeof vi.fn>
}): Transport {
  return {
    stream: overrides.stream ?? vi.fn(),
    unary: overrides.unary ?? vi.fn(),
  } as unknown as Transport
}

function rejectedMessages(error: unknown): AsyncIterable<never> {
  return {
    [Symbol.asyncIterator]: () => ({ next: () => Promise.reject(error) }),
  }
}

async function collect(messages: AsyncIterable<unknown>): Promise<unknown[]> {
  const collected = []
  for await (const message of messages) collected.push(message)
  return collected
}

function expectExpiredSessionRedirect(error: unknown): void {
  expect(error).toBeInstanceOf(Response)
  expect((error as Response).status).toBe(302)
  expect((error as Response).headers.get('location')).toBe(
    '/login?returnTo=%2Fworkspaces%2Fanalytics%2Fsources',
  )
  expect((error as Response).headers.get('X-Coral-UI-Expired-Session')).toBe('1')
}
