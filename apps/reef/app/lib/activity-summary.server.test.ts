import { createClient, createRouterTransport, Code, ConnectError } from '@connectrpc/connect'
import { expect, it } from 'vitest'
import { ActivityService } from '@/generated/coral/management/v1/activity_pb'
import { glimpseActivitySummary, loadActivitySummary } from './activity-summary.server'

const workspace = 'workspaces/00000000-0000-4000-8000-000000000001'

function activity(summarizeActivity: () => object) {
  return createClient(
    ActivityService,
    createRouterTransport(({ service }) => {
      service(ActivityService, { summarizeActivity })
    }),
  )
}

it('keeps exact context bytes and turns the median wait into seconds', async () => {
  const api = {
    activity: activity(() => ({
      calls: {
        answered: 5n,
        blocked: 1n,
        notAllowed: 2n,
        needsSetup: 3n,
        failed: 0n,
        canceled: 4n,
      },
      approvals: { approved: 3n, declined: 2n, medianWaitNanos: 1_500_000_000n },
      contextImpactBytes: -9007199254740993n,
      contextImpactPartial: true,
      actors: [{ key: 'u1', label: 'You', bytes: -9007199254740993n }],
    })),
  }
  expect(await loadActivitySummary(api, workspace, 7)).toMatchObject({
    calls: { answered: 5, blocked: 1, notAllowed: 2, needsSetup: 3, failed: 0, canceled: 4 },
    approvals: { approved: 3, declined: 2, medianWaitSeconds: 1.5 },
    contextImpactBytes: '-9007199254740993',
    contextImpactPartial: true,
    contextActors: [{ key: 'u1', label: 'You', bytes: '-9007199254740993' }],
  })
})

// Catches: an agent's calls that do not open its page, or your own calls that link nowhere.
it('links each agent caller to its page and leaves your own calls unlinked', async () => {
  const agent = `${workspace}/agents/agent:00000000-0000-4000-8000-000000000002`
  const callers = [
    { key: 'a1', label: 'Nightly sync', agent, calls: 7n },
    { key: 'u1', label: 'You', agent: '', calls: 2n },
  ]
  const api = {
    activity: activity(() => ({
      callers,
      tools: [{ callPath: 'coral.providers.github.getIssue', calls: 9n, callers }],
    })),
  }
  const summary = await loadActivitySummary(api, workspace, 7)
  const expected = [
    {
      key: 'a1',
      label: 'Nightly sync',
      agentPath: `/${workspace}/agents/agent%3A00000000-0000-4000-8000-000000000002`,
      calls: 7,
    },
    { key: 'u1', label: 'You', calls: 2 },
  ]
  expect(summary.callers).toEqual(expected)
  expect(summary.tools[0].callers).toEqual(expected)
})

it('reports no decisions and no measured context as absent, not zero', async () => {
  const summary = await loadActivitySummary({ activity: activity(() => ({})) }, workspace, 1)
  expect(summary.approvals.medianWaitSeconds).toBeNull()
  expect(summary.contextImpactBytes).toBeUndefined()
})

// Catches: zeros shown for an unavailable summary, or a rejected session or a defect
// hidden as one.
it.each([
  [Code.NotFound, 'hidden'],
  [Code.PermissionDenied, 'hidden'],
  [Code.Unavailable, 'hidden'],
  [Code.Unauthenticated, 'rejected'],
  [Code.InvalidArgument, 'rejected'],
  [Code.Internal, 'rejected'],
] as const)('a glimpse that fails with code %s is %s', async (code, expected) => {
  const api = {
    activity: activity(() => {
      throw new ConnectError('private error', code)
    }),
  }
  const glimpse = glimpseActivitySummary(api, workspace, 7)
  if (expected === 'hidden') expect(await glimpse).toBeNull()
  else await expect(glimpse).rejects.toMatchObject({ code })
})
