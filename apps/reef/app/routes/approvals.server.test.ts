import { Code, ConnectError } from '@connectrpc/connect'
import { expect, it, vi } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import { ActivityRequiredActionKind } from '@/generated/coral/management/v1/activity_pb'
import { WorkspaceRole, WorkspaceState } from '@/generated/coral/management/v1/workspace_pb'
import { fakeManagementTransport } from '@/lib/management.test-support'
import { loader } from './approvals'

const coral = vi.hoisted(() => ({ transport: undefined as unknown }))
vi.mock('@/lib/coral-request.server', () => ({
  managementTransportForRequest: () => coral.transport,
}))
const id = '00000000-0000-4000-8000-000000000001'
const workspace = `workspaces/${id}`

const queued = (request: string) => ({
  action: {
    request: { request: { case: 'approval' as const, value: `${workspace}/approvals/${request}` } },
    callPath: `mail.${request}`,
  },
  run: { name: `${workspace}/runs/${request}`, workspace },
})

function fakeCoral(
  state: WorkspaceState,
  browse: (input: { after: string; kind: ActivityRequiredActionKind }) => object,
  count: () => object = () => ({}),
) {
  coral.transport = fakeManagementTransport({
    workspaces: {
      getWorkspace: ({ name }) => ({
        name,
        displayName: 'Finance',
        effectiveRole: WorkspaceRole.MEMBER,
        state,
      }),
    },
    activity: { browseRequiredActions: browse, countRequiredActions: count },
  })
}

function load() {
  const request = new Request(`http://reef.test/workspaces/${id}/approvals`)
  return loader({ ...authRouteTestArgs(request, {}), params: { workspace: id } } as never)
}

async function loaderData() {
  const result = await load()
  return (
    result as unknown as {
      data: { approvals: { action: { name: string } }[]; revision?: string }
    }
  ).data
}

it('lists every pending approval in the order Coral returns them, across pages', async () => {
  const kinds: ActivityRequiredActionKind[] = []
  fakeCoral(WorkspaceState.ACTIVE, ({ after, kind }) => {
    kinds.push(kind)
    return after
      ? { actions: [queued('second')], actionCount: 2n }
      : { actions: [queued('first')], actionCount: 2n, nextAfter: 'next' }
  })
  const { approvals } = await loaderData()
  expect(approvals.map(({ action }) => action.name)).toEqual([
    `${workspace}/approvals/first`,
    `${workspace}/approvals/second`,
  ])
  expect(new Set(kinds)).toEqual(new Set([ActivityRequiredActionKind.APPROVAL]))
})

// The rows must be at least as new as the revision, or a change between them is never seen.
it('reads the approval revision before the rows', async () => {
  const reads: string[] = []
  fakeCoral(
    WorkspaceState.ACTIVE,
    () => {
      reads.push('rows')
      return { actions: [] }
    },
    () => {
      reads.push('revision')
      return { approvalRevision: 'approvals', connectionRevision: 'connections' }
    },
  )
  expect((await loaderData()).revision).toBe('approvals')
  expect(reads).toEqual(['revision', 'rows'])
})

it('still lists the rows when the revision cannot be read', async () => {
  fakeCoral(
    WorkspaceState.ACTIVE,
    () => ({ actions: [queued('first')], actionCount: 1n }),
    () => {
      throw new ConnectError('down', Code.Unavailable)
    },
  )
  const { approvals, revision } = await loaderData()
  expect(approvals).toHaveLength(1)
  expect(revision).toBeUndefined()
})

it('has no page for an archived workspace', async () => {
  const browse = vi.fn(() => ({ actions: [] }))
  fakeCoral(WorkspaceState.ARCHIVED, browse)
  await expect(load()).rejects.toMatchObject({ status: 404 })
  expect(browse).not.toHaveBeenCalled()
})

// An empty list says that nothing waits, so a failed read must not produce one.
it('fails the page when the requests cannot be read', async () => {
  fakeCoral(WorkspaceState.ACTIVE, () => {
    throw new ConnectError('down', Code.Unavailable)
  })
  await expect(load()).rejects.toMatchObject({ status: 503 })
})
