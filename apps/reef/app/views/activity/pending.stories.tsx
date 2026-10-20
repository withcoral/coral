import type { Meta, StoryObj } from '@storybook/react'
import { createRoutesStub, useSearchParams } from 'react-router'
import { expect, waitFor, within } from 'storybook/test'
import { PageLayout } from '@/wax/components/page-layout'
import { trail } from '@/views/management/layout'
import { Activity } from './activity'
import { ActionReviewSession } from '../actions/review'
import type { ActivityPage, ActivityRow, RequestDetail } from './types'

const workspace = 'workspaces/finance'
const run: Extract<ActivityRow, { kind: 'run' }> = {
  kind: 'run',
  name: `${workspace}/runs/reminders`,
  workspace,
  agent: `${workspace}/agents/collections`,
  actor: 'actor-id',
  actorLabel: 'Collections agent',
  workspaceLabel: 'Finance',
  intent: 'Send invoice reminders',
  task: '',
  state: 'running',
  startedAt: '2026-09-22T09:00:00Z',
  endedAt: null,
  durationNanos: '3000000000',
  requiresAction: true,
  actionPage: {
    total: '2',
    next: '',
    actions: [
      {
        kind: 'approval',
        name: `${workspace}/approvals/mail`,
        callPath: 'coral.providers.mail.send',
        providerLabel: 'Mail',
        deadline: '2030-09-22T09:15:00Z',
      },
      {
        kind: 'approval',
        name: `${workspace}/approvals/update`,
        callPath: 'coral.providers.billing.updateInvoice',
        providerLabel: 'Billing',
        deadline: '2030-09-22T09:15:00Z',
      },
    ],
  },
}
const request: Extract<RequestDetail, { kind: 'approval' }> = {
  kind: 'approval',
  name: run.actionPage!.actions[0].name,
  run: run.name,
  invocation: `${workspace}/invocations/mail`,
  provider: `${workspace}/providers/mail`,
  providerLabel: 'Mail',
  callPath: 'coral.providers.mail.send',
  arguments:
    '{"to":"owner@example.com","subject":"Invoice INV-4171 is overdue","body":"Your invoice is ready for review."}',
  state: 'pending',
  actionable: true,
  deadline: '2030-09-22T09:15:00Z',
  approver: '',
  decidedAt: null,
  comment: null,
}
/** Queue entries for each action of `runs`, in order. */
function queued(...runs: (typeof run)[]) {
  return runs.flatMap((item) =>
    (item.actionPage?.actions ?? []).map((action) => ({ action, run: item })),
  )
}
const accountRun: typeof run = {
  ...run,
  name: `${workspace}/runs/account`,
  intent: 'Read my open issues',
  agent: '',
  actorLabel: 'You',
  actionPage: {
    total: '1',
    next: '',
    actions: [
      {
        kind: 'account_connection',
        name: `${workspace}/accountConnections/linear`,
        callPath: '',
        providerLabel: 'Linear',
        deadline: '2030-09-22T09:15:00Z',
      },
    ],
  },
}
const page: ActivityPage = {
  rows: [
    {
      ...run,
      name: `${workspace}/runs/personal`,
      actorLabel: 'You',
      actor: 'you',
      agent: '',
      intent: 'Read my invoices',
      requiresAction: false,
      actionPage: undefined,
    },
  ],
  branches: {},
  expanded: [],
  hasBefore: false,
  hasAfter: false,
  anchorUnavailable: false,
  listUnavailable: false,
  agent: null,
  detail: null,
  unavailable: false,
  selected: null,
  selectedRow: null,
  listPath: '/workspaces/finance/activity',
  pending: {
    actionCount: '3',
    next: '',
    unavailable: false,
    actions: queued(run, accountRun),
  },
}
function StoryActivity({ page: initial }: { page: ActivityPage }) {
  const [query] = useSearchParams()
  const selected = query.get('record') ?? initial.selected
  const selectedRun =
    initial.pending?.actions.find((item) => item.run.name === selected)?.run ?? initial.selectedRow
  const action =
    selectedRun?.kind === 'run'
      ? selectedRun.actionPage?.actions.find((item) => item.name === query.get('request'))
      : null
  const exact: RequestDetail | null =
    action?.kind === 'approval'
      ? {
          ...request,
          name: action.name,
          callPath: action.callPath,
          providerLabel: action.providerLabel,
          arguments:
            action.name === request.name
              ? request.arguments
              : '{"invoice":"INV-4171","reminderSent":true}',
        }
      : action
        ? {
            kind: 'account_connection',
            name: action.name,
            provider: 'linear',
            state: 'pending',
            expiresAt: action.deadline,
          }
        : null
  const value: ActivityPage = {
    ...initial,
    selected,
    selectedRow: selectedRun ?? null,
    detail:
      initial.detail ??
      (selectedRun?.kind === 'run'
        ? {
            kind: 'run',
            run: selectedRun,
            request: exact,
            source: 'return await coral.providers.mail.send({to: "owner@example.com"});',
            linkedFunctions: [],
            terminal: { kind: 'unavailable' },
          }
        : null),
  }
  if (value.detail?.kind === 'run' && value.detail.request?.kind === 'approval') {
    const context = value.detail
    const approval = context.request as Extract<RequestDetail, { kind: 'approval' }>
    const invocation = {
      ...context.run,
      name: approval.invocation,
      run: context.run.name,
      provider: approval.provider,
      operation: approval.callPath,
      callPath: approval.callPath,
      state: 'pending' as const,
      outcome: 'unknown' as const,
      dispatchedAt: null,
      waitingNanos: '1',
    }
    value.detail = {
      kind: 'invocation',
      invocation,
      arguments: approval.arguments,
      result: { kind: 'unknown' },
      requests: [{ kind: 'approval', name: approval.name }],
      request: approval,
      requestUnavailable: false,
      context: { ...context, request: null },
    }
    value.selected = invocation.name
    value.selectedRow = { ...invocation, kind: 'invocation' }
    value.rows = [
      { ...context.run, kind: 'run' },
      ...value.rows.filter((row) => row.name !== context.run.name),
    ]
    value.expanded = [context.run.name]
    value.branches = {
      [context.run.name]: { rows: [value.selectedRow], next: '', unavailable: false },
    }
  }
  return (
    <ActionReviewSession
      request={
        value.detail?.kind === 'run' || value.detail?.kind === 'invocation'
          ? value.detail.request
          : null
      }
    >
      <Activity page={value} />
    </ActionReviewSession>
  )
}
const meta = {
  title: 'Activity/Pending actions',
  component: Activity,
  args: { page },
  parameters: { layout: 'fullscreen', chromatic: { viewports: [390, 1440] } },
  render: (args) => <StoryActivity page={args.page} />,
  decorators: [
    (Story, context) => {
      const Stub = createRoutesStub([{ path: '*', Component: Story }])
      return (
        <div style={{ height: '100vh', display: 'flex', flexDirection: 'column' }}>
          <PageLayout variant="split" breadcrumbs={trail([], 'Activity')}>
            <Stub
              initialEntries={[context.parameters.initialEntry ?? '/workspaces/finance/activity']}
            />
          </PageLayout>
        </div>
      )
    },
  ],
} satisfies Meta<typeof Activity>
export default meta
type Story = StoryObj<typeof meta>
export const Combined: Story = {}
export const ExactReview: Story = {
  parameters: {
    initialEntry: `/workspaces/finance/activity?record=${run.name}&request=${request.name}`,
  },
}
const singleRun: typeof run = {
  ...run,
  actorLabel: 'You',
  agent: '',
  intent: 'Read issues assigned to the current user, most recently updated first.',
  actionPage: {
    total: '1',
    next: '',
    actions: [
      {
        ...run.actionPage!.actions[0],
        providerLabel: 'Linear',
        callPath: 'coral.providers.linear.listIssues',
      },
    ],
  },
}
export const SingleAction: Story = {
  args: {
    page: {
      ...page,
      selected: run.name,
      selectedRow: singleRun,
      pending: { actions: queued(singleRun), actionCount: '1', next: '', unavailable: false },
      detail: {
        kind: 'run',
        run: singleRun,
        request: {
          ...request,
          providerLabel: 'Linear',
          provider: `${workspace}/providers/linear`,
          callPath: 'coral.providers.linear.listIssues',
          arguments: JSON.stringify({
            assignee: 'me',
            includeArchived: false,
            limit: 100,
            orderBy: 'updatedAt',
            fields: [
              'id',
              'title',
              'url',
              'status',
              'statusType',
              'priority',
              'dueDate',
              'updatedAt',
              'project',
            ],
          }),
        },
        source: 'return await coral.providers.linear.listIssues({ assignee: "me" });',
        linkedFunctions: [],
        terminal: { kind: 'unavailable' },
      },
    },
  },
}
export const EmptyPending: Story = {
  args: {
    page: { ...page, pending: { actions: [], actionCount: '0', next: '', unavailable: false } },
  },
}
export const ManyPending: Story = {
  args: {
    page: {
      ...page,
      pending: {
        actionCount: '200',
        next: 'next-page',
        unavailable: false,
        actions: Array.from({ length: 10 }, (_, index) => ({
          action: {
            ...run.actionPage!.actions[0],
            name: `${workspace}/approvals/reminder-${index}`,
            deadline: `2030-09-22T09:${String(index + 10).padStart(2, '0')}:00Z`,
          },
          run: {
            ...run,
            name: `${run.name}-${index}`,
            intent: `Send invoice reminders ${index + 1}`,
          },
        })),
      },
    },
  },
}
export const LargeArguments: Story = {
  args: {
    page: {
      ...page,
      selected: run.name,
      selectedRow: run,
      detail: {
        kind: 'run',
        run,
        source: 'return null;',
        linkedFunctions: [],
        terminal: { kind: 'unavailable' },
        request: {
          ...request,
          arguments: JSON.stringify({
            to: 'owner@example.com',
            lines: Array.from({ length: 80 }, (_, i) => `Invoice ${i + 1}`),
          }),
        },
      },
    },
  },
}
export const ResolvedSelection: Story = {
  args: {
    page: {
      ...page,
      selected: run.name,
      selectedRow: run,
      pending: { actions: [], actionCount: '0', next: '', unavailable: false },
      detail: {
        kind: 'run',
        run: { ...run, actionPage: { actions: [], total: '0', next: '' } },
        source: 'return null;',
        linkedFunctions: [],
        terminal: { kind: 'unavailable' },
        request: {
          ...request,
          actionable: false,
          state: 'approved',
          approver: 'owner',
          approverLabel: 'You',
          decidedAt: '2026-09-22T09:01:00Z',
          comment: 'Checked the recipient.',
        },
      },
    },
  },
}
/** Parallel calls to one Operation: one Run, three separate decisions. */
export const ParallelCalls: Story = {
  args: {
    page: {
      ...page,
      pending: {
        actionCount: '3',
        next: '',
        unavailable: false,
        actions: ['open', 'progress', 'review'].map((state) => ({
          action: {
            ...singleRun.actionPage!.actions[0],
            name: `${workspace}/approvals/${state}`,
          },
          run: {
            ...singleRun,
            intent: 'Rerun the three parallel listIssues calls for my Linear issues',
          },
        })),
      },
    },
  },
}
/**
 * Scrolled arguments pass under the sticky review header. The play function
 * scrolls the heading behind the header so Chromatic records that the header
 * hides it in each theme.
 */
const reviewHeaderOverArguments: Story = {
  args: {
    page: {
      ...page,
      selected: run.name,
      selectedRow: run,
      pending: { actions: [], actionCount: '0', next: '', unavailable: false },
      detail: {
        kind: 'run',
        run: { ...run, actionPage: { actions: [], total: '0', next: '' } },
        source: 'return null;',
        linkedFunctions: [],
        terminal: { kind: 'unavailable' },
        request: {
          ...request,
          // Taller than the drawer at every Chromatic viewport, so the review scrolls.
          arguments: JSON.stringify({
            lines: Array.from({ length: 300 }, (_, i) => `Invoice ${i + 1}`),
          }),
          actionable: false,
          state: 'approved',
          approver: 'owner',
          approverLabel: 'You',
          decidedAt: '2026-09-22T09:01:00Z',
        },
      },
    },
  },
  play: async ({ canvasElement }) => {
    const body = within(canvasElement.ownerDocument.body)
    const review = await body.findByRole('region', { name: `Review ${request.callPath}` })
    // The drawer slides in from off-screen, so measure only once it rests.
    const drawer = body.getByRole('dialog')
    await Promise.all(drawer.getAnimations().map((animation) => animation.finished))
    const heading = within(review).getByRole('heading', { name: 'Arguments to review' })
    const header = review.querySelector('header')!
    let port = heading.parentElement
    while (
      port &&
      !(
        port.scrollHeight > port.clientHeight &&
        ['auto', 'scroll'].includes(getComputedStyle(port).overflowY)
      )
    )
      port = port.parentElement
    if (!port) throw new Error('The review needs a scroll viewport')
    const scrollPort = port
    // The header sticks only once the review scrolls, so measure again after each move.
    await waitFor(() => {
      const before = heading.getBoundingClientRect()
      const stuck = header.getBoundingClientRect()
      scrollPort.scrollTop += before.top - stuck.top - (stuck.height - before.height) / 2
      const behind = heading.getBoundingClientRect()
      const pinned = header.getBoundingClientRect()
      expect(behind.top).toBeGreaterThanOrEqual(pinned.top)
      expect(behind.bottom).toBeLessThanOrEqual(pinned.bottom)
    })
    const { left, top, width, height } = heading.getBoundingClientRect()
    const topmost = canvasElement.ownerDocument.elementFromPoint(left + width / 2, top + height / 2)
    await expect(header.contains(topmost)).toBe(true)
  },
}
export const ReviewHeaderOverArgumentsLight: Story = {
  ...reviewHeaderOverArguments,
  globals: { backgrounds: { value: 'light' } },
}
export const ReviewHeaderOverArgumentsDark: Story = {
  ...reviewHeaderOverArguments,
  globals: { backgrounds: { value: 'dark' } },
}
