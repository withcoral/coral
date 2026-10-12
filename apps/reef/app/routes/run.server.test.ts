import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import { ActivityRunState } from '@/generated/coral/management/v1/activity_pb'
import { fakeManagementTransport } from '@/lib/management.test-support'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { loader } from './run'
import { loader as activityLoader } from './activity'

const transportState = vi.hoisted(() => ({ count: 1 }))
const authorizedWorkspace = vi.hoisted(() => vi.fn())
vi.mock('@/lib/workspace-route.server', () => ({ authorizedWorkspace }))
vi.mock('@/lib/coral-request.server', () => ({ managementTransportForRequest: () => transport }))
const workspace = 'workspaces/00000000-0000-4000-8000-000000000001'
const run = `${workspace}/runs/00000000-0000-4000-8000-000000000002`
const approval = `${workspace}/approvals/00000000-0000-4000-8000-000000000003`
const transport = fakeManagementTransport({
  workspaces: { listWorkspaces: () => ({ workspaces: [] }) },
  activity: {
    inspectRun: () => ({
      run: {
        name: run,
        workspace,
        state: ActivityRunState.RUNNING,
        actionPage: {
          total: BigInt(transportState.count),
          actions: Array.from({ length: transportState.count }, (_, i) => ({
            request: {
              request: { case: 'approval' as const, value: i ? `${approval}-second` : approval },
            },
            callPath: 'mail.send',
          })),
        },
      },
    }),
    inspectApproval: ({ name }) => ({
      name,
      run,
      state: 'pending',
      actionable: true,
      argumentsJson: '{"to":"exact-recipient"}',
    }),
    browseActivityRoots: () => ({ roots: [] }),
    browseRequiredActions: () => ({ actions: [] }),
  },
})
afterEach(() => {
  transportState.count = 1
  authorizedWorkspace.mockReset()
})
beforeEach(() => {
  authorizedWorkspace.mockRejectedValue(new Error('no workspace access'))
})
it.each([1, 2])(
  'opens Run context without treating the Run as an approval target or redirecting (%s)',
  async (count) => {
    transportState.count = count
    const runResponse = await loader(
      authRouteTestArgs(new Request(`http://reef.test/${run}`), {
        workspace: workspace.split('/')[1],
        run: run.split('/').at(-1)!,
      }),
    )
    // The Activity list requires its Workspace.
    authorizedWorkspace.mockResolvedValue({ workspace: { name: workspace } })
    const responses = [
      runResponse,
      await activityLoader(
        authRouteTestArgs(new Request(`http://reef.test/${workspace}/activity?record=${run}`), {
          workspace: workspace.split('/')[1],
        }),
      ),
    ]
    for (const response of responses) {
      expect(response).not.toBeInstanceOf(Response)
      if (response instanceof Response) throw new Error('Review must not require another request')
      expect(response.data.detail).toMatchObject({
        kind: 'run',
        request: null,
        run: { actionPage: { total: String(count) } },
      })
      expect(response.data.initialRequest).toBeUndefined()
    }
  },
)

it('names the Run workspace for its breadcrumb, and still opens the Run without it', async () => {
  const args = () =>
    authRouteTestArgs(new Request(`http://reef.test/${run}`), {
      workspace: workspace.split('/')[1],
      run: run.split('/').at(-1)!,
    })
  authorizedWorkspace.mockResolvedValue({
    workspace: { name: workspace, displayName: 'Demo', path: `/${workspace}` },
  })
  const named = await loader(args())
  if (named instanceof Response) throw new Error('The Run page renders')
  expect(named.data.workspace).toMatchObject({ displayName: 'Demo', path: `/${workspace}` })
  authorizedWorkspace.mockRejectedValue(new Error('no workspace access'))
  const unnamed = await loader(args())
  if (unnamed instanceof Response) throw new Error('The Run page renders')
  expect(unnamed.data.workspace).toBeUndefined()
  expect(unnamed.data.detail).toMatchObject({ kind: 'run' })
})
