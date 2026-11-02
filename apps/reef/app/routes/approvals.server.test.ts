import { Code, ConnectError } from '@connectrpc/connect'
import { expect, it, vi } from 'vitest'
import { authRouteTestArgs } from '@/auth/server-context.test-helper'
import {
  ActivityApprovalOutcome,
  ActivityRequiredActionKind,
} from '@/generated/coral/management/v1/activity_pb'
import { WorkspaceRole, WorkspaceState } from '@/generated/coral/management/v1/workspace_pb'
import { fakeManagementTransport, type ManagementHandlers } from '@/lib/management.test-support'
import type { ApprovalReading, SettledPage } from '@/views/activity/types'
import { loader } from './approvals'
import { loader as historyLoader } from './approvals-history'

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
  review: ManagementHandlers['activity'] = {},
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
    activity: {
      browseRequiredActions: browse,
      countRequiredActions: count,
      browseSettledApprovals: () => ({ approvals: [] }),
      ...review,
    },
    providers: { listProviders: () => ({ providers: [] }) },
  })
}

function load(search = '') {
  const request = new Request(`http://reef.test/workspaces/${id}/approvals${search}`)
  return loader({ ...authRouteTestArgs(request, {}), params: { workspace: id } } as never)
}

async function loaderData(search = '') {
  const result = await load(search)
  return (
    result as unknown as {
      data: {
        approvals: { action: { name: string } }[]
        settled: SettledPage | null
        revision?: string
        selected: string | null
        review: ApprovalReading | null
      }
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

const selected = `${workspace}/approvals/open`
const selectedSearch = `?${new URLSearchParams({ request: selected })}`
const invocation = `${workspace}/invocations/open`
const openRun = `${workspace}/runs/open`
const nanos = (iso: string) => BigInt(Date.parse(iso)) * 1_000_000n

/** Coral's answers for the exact request, its call and its Run. `failure` conceals or fails. */
function exactReads(failure?: ConnectError) {
  const read = <Reply>(reply: Reply) =>
    vi.fn(() => {
      if (failure) throw failure
      return reply
    })
  return {
    inspectApproval: read({
      name: selected,
      run: openRun,
      invocation,
      callPath: 'mail.send',
      argumentsJson: '{"to":"exact-recipient"}',
      state: 'pending',
      actionable: true,
      deadlineAtUnixNanos: nanos('2026-09-22T10:00:00Z'),
    }),
    inspectInvocation: read({
      invocation: {
        name: invocation,
        run: openRun,
        startedAtUnixNanos: nanos('2026-09-22T09:30:00Z'),
      },
    }),
    inspectRun: read({
      run: { name: openRun, workspace, intent: 'Send reminders', task: 'task' },
      submittedSource: 'return await coral.providers.mail.send({})',
    }),
    inspectTask: read({ task: { name: 'task', intent: 'Collect invoices' } }),
  }
}

it('reads the exact arguments only for the request the address opens', async () => {
  const reads = exactReads()
  fakeCoral(WorkspaceState.ACTIVE, () => ({ actions: [] }), undefined, reads)

  expect((await loaderData()).review).toBeNull()
  expect(reads.inspectApproval).not.toHaveBeenCalled()

  const { review } = await loaderData(selectedSearch)
  expect(review).toMatchObject({
    detail: {
      name: selected,
      arguments: '{"to":"exact-recipient"}',
      taskIntent: 'Collect invoices',
      actionable: true,
    },
    context: {
      run: { name: openRun, intent: 'Send reminders' },
      source: 'return await coral.providers.mail.send({})',
    },
    call: { kind: 'invocation', name: invocation, startedAt: '2026-09-22T09:30:00.000Z' },
  })
})

// A concealed request leaves the list. `approvalReading` owns what counts as concealed.
it('keeps the list when Coral shows no review', async () => {
  const reads = exactReads(new ConnectError('hidden', Code.NotFound))
  fakeCoral(WorkspaceState.ACTIVE, () => ({ actions: [queued('first')] }), undefined, reads)
  const { approvals, review, selected: opened } = await loaderData(selectedSearch)
  expect(review).toBeNull()
  expect(opened).toBe(selected)
  expect(approvals).toHaveLength(1)
})

it('fails the page when Coral cannot answer for the open request', async () => {
  const reads = exactReads(new ConnectError('down', Code.Unavailable))
  fakeCoral(WorkspaceState.ACTIVE, () => ({ actions: [] }), undefined, reads)
  await expect(load(selectedSearch)).rejects.toMatchObject({ status: 503 })
})

// The panel follows the same revision as the rows, or a change between them is never seen.
it('reads the open request after the revision', async () => {
  const order: string[] = []
  const reads = exactReads()
  fakeCoral(
    WorkspaceState.ACTIVE,
    () => ({ actions: [] }),
    () => {
      order.push('revision')
      return {}
    },
    {
      ...reads,
      inspectApproval: () => {
        order.push('request')
        return reads.inspectApproval()
      },
    },
  )
  await loaderData(selectedSearch)
  expect(order).toEqual(['revision', 'request'])
})

const settledRow = (request: string, outcome: ActivityApprovalOutcome, approverLabel = '') => ({
  name: `${workspace}/approvals/${request}`,
  outcome,
  settledAtUnixNanos: nanos('2026-09-22T11:00:00Z'),
  approverLabel,
  callPath: `mail.${request}`,
  run: { name: `${workspace}/runs/${request}`, workspace, intent: 'Send reminders' },
})

// History follows the same revision as the pending rows: a request settles by leaving them.
it('reads the newest settled page after the revision, in the order Coral returns it', async () => {
  const order: string[] = []
  const settled = vi.fn(({ after }: { after: string }) => {
    order.push(`settled:${after}`)
    return {
      approvals: [
        settledRow('expired', ActivityApprovalOutcome.EXPIRED),
        settledRow('approved', ActivityApprovalOutcome.APPROVED, 'Ada Lovelace'),
        // An outcome this build does not know is no row, not a wrong one.
        settledRow('newer', 99 as ActivityApprovalOutcome),
      ],
      nextAfter: 'older',
    }
  })
  fakeCoral(
    WorkspaceState.ACTIVE,
    () => ({ actions: [] }),
    () => {
      order.push('revision')
      return {}
    },
    { browseSettledApprovals: settled },
  )
  const page = (await loaderData()).settled!
  expect(page.approvals).toMatchObject([
    { name: `${workspace}/approvals/expired`, outcome: 'expired', approverLabel: '' },
    {
      name: `${workspace}/approvals/approved`,
      outcome: 'approved',
      approverLabel: 'Ada Lovelace',
      settledAt: '2026-09-22T11:00:00.000Z',
      run: { intent: 'Send reminders' },
    },
  ])
  expect(page.next).toBe('older')
  // Older pages load on request, never with the page.
  expect(order).toEqual(['revision', 'settled:'])
})

function loadHistory(search: string) {
  const request = new Request(`http://reef.test/workspaces/${id}/approvals/history${search}`)
  return historyLoader({ ...authRouteTestArgs(request, {}), params: { workspace: id } } as never)
}

it('loads the settled page after the given position', async () => {
  const settled = vi.fn(() => ({
    approvals: [settledRow('older', ActivityApprovalOutcome.DECLINED, 'You')],
  }))
  fakeCoral(WorkspaceState.ACTIVE, () => ({ actions: [] }), undefined, {
    browseSettledApprovals: settled,
  })
  const result = (await loadHistory('?after=older')) as unknown as { data: SettledPage }
  expect(result.data).toMatchObject({
    approvals: [{ name: `${workspace}/approvals/older`, outcome: 'declined' }],
    next: '',
  })
  expect(settled).toHaveBeenCalledWith(
    expect.objectContaining({ workspace, after: 'older' }),
    expect.anything(),
  )
})

// The page keeps its rows through an outage, so the failure is an answer, not a new page.
it('answers an outage as unavailable, and refuses a missing position or an archived workspace', async () => {
  fakeCoral(WorkspaceState.ACTIVE, () => ({ actions: [] }), undefined, {
    browseSettledApprovals: () => {
      throw new ConnectError('down', Code.Unavailable)
    },
  })
  const outage = (await loadHistory('?after=older')) as unknown as {
    data: unknown
    init: { status: number }
  }
  expect(outage.data).toEqual({ unavailable: true })
  expect(outage.init.status).toBe(503)
  await expect(loadHistory('')).rejects.toMatchObject({ status: 400 })
  const settled = vi.fn(() => ({ approvals: [] }))
  fakeCoral(WorkspaceState.ARCHIVED, () => ({ actions: [] }), undefined, {
    browseSettledApprovals: settled,
  })
  await expect(loadHistory('?after=older')).rejects.toMatchObject({ status: 404 })
  expect(settled).not.toHaveBeenCalled()
})

// Pending requests have deadlines: a history outage must not hide them.
it('keeps the pending rows when the history cannot be read', async () => {
  fakeCoral(WorkspaceState.ACTIVE, () => ({ actions: [queued('first')] }), undefined, {
    browseSettledApprovals: () => {
      throw new ConnectError('down', Code.Unavailable)
    },
  })
  const { approvals, settled } = await loaderData()
  expect(approvals).toHaveLength(1)
  expect(settled).toBeNull()
})
