import { Code, ConnectError, createRouterTransport } from '@connectrpc/connect'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { ActivityService } from '@/generated/coral/management/v1/activity_pb'
import {
  AccountConnectionState,
  ProviderAccountService,
} from '@/generated/coral/management/v1/account_pb'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { hostedRuntimeConfig, requiredAuthConfig } from '@/auth/test-config'
import { action } from './activity-resolve'

const decideApproval = vi.hoisted(() => vi.fn(async () => ({ state: 'declined' })))
const cancelActivityRun = vi.hoisted(() => vi.fn(async () => ({ state: 3 })))
vi.mock('@/lib/coral-request.server', () => ({
  managementTransportForRequest: () => transport,
}))
const resolveAccountConnectionRequest = vi.hoisted(() =>
  vi.fn(async (_request: unknown) => ({ state: 3 })),
)
const transport = createRouterTransport(({ service }) => {
  service(ActivityService, { decideApproval, cancelActivityRun })
  service(ProviderAccountService, { resolveAccountConnectionRequest })
})
afterEach(() => {
  vi.clearAllMocks()
})
function request(intent = 'approve', origin: string | null = 'http://reef.test') {
  return new Request('http://reef.test/activity/resolve', {
    method: 'POST',
    headers: origin ? { origin } : {},
    body: new URLSearchParams({
      intent,
      name: 'workspaces/w/approvals/a',
      comment: 'Reviewed exact arguments',
    }),
  })
}
it.each([null, 'https://other.test'])(
  'rejects untrusted origins before decisions (%s)',
  async (origin) => {
    await expect(action(authRouteTestArgs(request('approve', origin), {}))).rejects.toMatchObject({
      status: 403,
    })
    expect(decideApproval).not.toHaveBeenCalled()
  },
)
it('returns the winning decision rather than asserting that a stale requested approval won', async () => {
  const response = await action(authRouteTestArgs(request(), {}))
  expect(response.data).toEqual({ state: 'declined' })
  expect(decideApproval).toHaveBeenCalledWith(
    expect.objectContaining({
      name: 'workspaces/w/approvals/a',
      decision: 'approve',
      comment: 'Reviewed exact arguments',
    }),
    expect.anything(),
  )
  expect(JSON.stringify(response.data)).not.toContain('Reviewed exact arguments')
})
it('decides from the pending queue, which sends no comment field', async () => {
  const response = await action(
    authRouteTestArgs(
      new Request('http://reef.test/activity/resolve', {
        method: 'POST',
        headers: { origin: 'http://reef.test' },
        body: new URLSearchParams({ intent: 'decline', name: 'workspaces/w/approvals/a' }),
      }),
      {},
    ),
  )
  expect(response.data).toEqual({ state: 'declined' })
  const [decided] = decideApproval.mock.calls[0] as unknown as [Record<string, unknown>]
  expect(decided).toMatchObject({ name: 'workspaces/w/approvals/a', decision: 'decline' })
  expect(decided.comment).toBeUndefined()
})
it('reports an already completed Run instead of claiming cancellation won', async () => {
  cancelActivityRun.mockResolvedValueOnce({ state: 2 })
  const response = await action(authRouteTestArgs(request('cancel'), {}))
  expect(response.data).toEqual({ message: 'The run already succeeded.' })
})
// Coral answers this while its Run Owner has not recorded the request yet.
const notYetRecorded = () => new ConnectError('RUN_REQUEST_NOT_YET_RECORDED', Code.Unavailable)
describe.each([
  ['cancellation', 'cancel', cancelActivityRun, { message: 'The run already failed.' }],
  ['decision', 'approve', decideApproval, { state: 'declined' }],
] as const)('a %s that Coral has not recorded yet', (_, intent, send, result) => {
  it('is sent once more and answers what Coral recorded', async () => {
    send.mockRejectedValueOnce(notYetRecorded())
    const response = await action(authRouteTestArgs(request(intent), {}))
    expect(response.data).toEqual(result)
    expect(send).toHaveBeenCalledTimes(2)
  })
  it('is sent only once more, then answers that Coral is recording it', async () => {
    send.mockRejectedValueOnce(notYetRecorded()).mockRejectedValueOnce(notYetRecorded())
    const response = await action(authRouteTestArgs(request(intent), {}))
    expect(response.init?.status).toBe(202)
    expect(response.data).toEqual({ recording: true })
    expect(send).toHaveBeenCalledTimes(2)
  })
  // Catches: every UNAVAILABLE read as a request that Coral is recording.
  it('is not sent again when Coral is not available', async () => {
    send.mockRejectedValueOnce(new ConnectError('connection refused', Code.Unavailable))
    const response = await action(authRouteTestArgs(request(intent), {}))
    expect(response.init?.status).toBe(503)
    expect(response.data).toMatchObject({
      error: expect.stringContaining('Coral is not available.'),
    })
    expect(send).toHaveBeenCalledOnce()
  })
})
it('uses only Run cancellation and rejects unrelated or bulk decision intents', async () => {
  await action(authRouteTestArgs(request('cancel'), {}))
  expect(cancelActivityRun).toHaveBeenCalledOnce()
  await expect(action(authRouteTestArgs(request('approve-all'), {}))).rejects.toMatchObject({
    status: 400,
  })
  expect(decideApproval).not.toHaveBeenCalled()
})
it('requires CSRF in authenticated mode even for a same-origin stale browser', async () => {
  const { publicUrl } = requiredAuthConfig
  const args = authRouteTestArgs(request('approve', publicUrl), {}, 'dummy', hostedRuntimeConfig)
  await expect(action(args)).rejects.toMatchObject({ status: 403 })
  expect(decideApproval).not.toHaveBeenCalled()
})
it('declines a waiting account connection and reports the winning state', async () => {
  resolveAccountConnectionRequest.mockResolvedValueOnce({
    state: AccountConnectionState.CONNECTED,
  })
  const response = await action(
    authRouteTestArgs(
      new Request('http://reef.test/activity/resolve', {
        method: 'POST',
        headers: { origin: 'http://reef.test' },
        body: new URLSearchParams({
          intent: 'decline_connection',
          name: 'workspaces/w/accountConnections/c',
        }),
      }),
      {},
    ),
  )
  // Connecting elsewhere can win the race; the view words the actual state.
  expect(response.data).toEqual({ connection: 'connected' })
  expect(resolveAccountConnectionRequest).toHaveBeenCalledWith(
    expect.objectContaining({
      name: 'workspaces/w/accountConnections/c',
      decision: expect.objectContaining({ case: 'decline' }),
    }),
    expect.anything(),
  )
  expect(decideApproval).not.toHaveBeenCalled()
})
