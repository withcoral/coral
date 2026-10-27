import type { Meta, StoryObj } from '@storybook/react-vite'
import { createRoutesStub, useSearchParams } from 'react-router'
import { expect, waitFor, within } from 'storybook/test'
import { PageLayout } from '@/wax/components/page-layout'
import { trail } from '@/views/management/layout'
import { Activity } from './activity'
import { ActionReviewSession } from '../actions/review'
import { freezeStoryClock, minutesAfterStoryNow } from './story-clock'
import { programAt } from './side-panel-fixtures'
import type { ActivityPage, ActivityRow, RequestDetail } from './types'

const workspace = 'workspaces/finance'
/** The reminders Run's program: each approval it waits on sits at its own call here. */
const remindersProgram = [
  'const invoices = await coral.providers.billing.listOverdue({ limit: 10 })',
  'for (const invoice of invoices) {',
  '  await coral.providers.mail.send({',
  '    to: invoice.owner,',
  '    subject: `Invoice ${invoice.id} is overdue`,',
  "    body: 'Your invoice is ready for review.',",
  '  })',
  '  await coral.providers.billing.updateInvoice({ invoice: invoice.id, reminderSent: true })',
  '}',
].join('\n')
/** The account Run's program, whose one call waits on your Linear connection. */
const linearProgram = "return await coral.providers.linear.listIssues({ assignee: 'me' })"
/** 15 minutes after the story loads, so each deadline reads "Expires in 15 minutes". */
const soon = minutesAfterStoryNow(15)
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
        deadline: soon,
      },
      {
        kind: 'approval',
        name: `${workspace}/approvals/update`,
        callPath: 'coral.providers.billing.updateInvoice',
        providerLabel: 'Billing',
        deadline: soon,
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
  taskIntent: 'Chase overdue invoices for the Finance workspace',
  state: 'pending',
  actionable: true,
  deadline: soon,
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
        deadline: soon,
      },
    ],
  },
}
const accountCall = {
  ...accountRun,
  name: `${workspace}/invocations/linear`,
  run: accountRun.name,
  provider: `${workspace}/providers/linear`,
  tool: 'listIssues',
  callPath: 'coral.providers.linear.listIssues',
  state: 'pending' as const,
  outcome: 'unknown' as const,
  dispatchedAt: null,
  waitingNanos: '1',
  sourceLocation: programAt('coral.providers.linear.listIssues', linearProgram),
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
            source: selectedRun.name === accountRun.name ? linearProgram : remindersProgram,
            linkedFunctions: [],
            terminal: { kind: 'unavailable' },
          }
        : null),
  }
  // A call waiting on the shared Linear connection, reached on its own.
  if (selected === accountCall.name) {
    const connection = accountRun.actionPage!.actions[0]
    value.selectedRow = { ...accountCall, kind: 'invocation' }
    value.detail = {
      kind: 'invocation',
      invocation: accountCall,
      arguments: '{"assignee":"me","limit":50}',
      result: { kind: 'unknown' },
      requests: [{ kind: 'account_connection', name: connection.name }],
      request:
        query.get('request') === connection.name
          ? {
              kind: 'account_connection',
              name: connection.name,
              provider: 'linear',
              state: 'pending',
              expiresAt: connection.deadline,
            }
          : null,
      requestUnavailable: false,
      context: {
        kind: 'run',
        run: accountRun,
        source: linearProgram,
        linkedFunctions: [],
        terminal: { kind: 'unavailable' },
      },
    }
  }
  if (value.detail?.kind === 'run' && value.detail.request?.kind === 'approval') {
    const context = value.detail
    const approval = context.request as Extract<RequestDetail, { kind: 'approval' }>
    const invocation = {
      ...context.run,
      name: approval.invocation,
      run: context.run.name,
      provider: approval.provider,
      tool: approval.callPath,
      callPath: approval.callPath,
      state: 'pending' as const,
      outcome: 'unknown' as const,
      dispatchedAt: null,
      waitingNanos: '1',
      // The approved call, where the Run's program makes it.
      sourceLocation: programAt(approval.callPath, context.source),
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
  beforeEach: freezeStoryClock,
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
export const Combined: Story = {
  play: async ({ canvasElement, userEvent }) => {
    const canvas = within(canvasElement)
    const pending = await canvas.findByRole('heading', { name: 'Needs your action' })
    const heading = canvas.getByRole('heading', { name: 'Your recent activity' })
    const live = canvas.getByRole('button', { name: 'Follow latest activity' })
    // The page's live toggle sits right after the list heading, below the actions.
    await expect(heading.parentElement).toContainElement(live)
    await expect(heading.compareDocumentPosition(live)).toBe(Node.DOCUMENT_POSITION_FOLLOWING)
    await expect(pending.compareDocumentPosition(live)).toBe(Node.DOCUMENT_POSITION_FOLLOWING)
    await expect(live).toHaveAttribute('aria-pressed', 'false')
    // Tab reaches it after the actions, and before the table.
    live.focus()
    await userEvent.tab({ shift: true })
    await expect(pending.closest('section')).toContainElement(
      canvasElement.ownerDocument.activeElement as HTMLElement,
    )
    await userEvent.tab()
    await expect(live).toHaveFocus()
    await userEvent.tab()
    await expect(canvas.getByRole('table', { name: 'Activity records' })).toHaveFocus()
  },
}
export const ExactReview: Story = {
  parameters: {
    initialEntry: `/workspaces/finance/activity?record=${run.name}&request=${request.name}`,
  },
  play: async ({ canvasElement }) => {
    const body = within(canvasElement.ownerDocument.body)
    await body.findByRole('region', { name: `Review ${request.callPath}` })
    // The reviewed call shows beside its Run's program, at the line that makes it.
    const source = body.getByRole('region', { name: 'Accepted source' })
    await waitFor(() =>
      expect(
        [...source.querySelectorAll<HTMLElement>('[data-highlighted]')].map(
          (line) => line.dataset.line,
        ),
      ).toEqual(['3']),
    )
    await expect(within(source).getByRole('status')).toHaveTextContent('Line 3')
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
        source: remindersProgram,
        linkedFunctions: [],
        terminal: { kind: 'unavailable' },
        request: {
          ...request,
          arguments: JSON.stringify({
            to: 'owner@example.com',
            options: { trackOpens: false, replyTo: 'billing@example.com' },
            lines: Array.from({ length: 80 }, (_, i) => `Invoice ${i + 1}`),
          }),
        },
      },
    },
  },
}
/** Arguments that are not a JSON object stay one exact JSON block. */
export const NonObjectArguments: Story = {
  args: {
    page: {
      ...page,
      selected: run.name,
      selectedRow: run,
      detail: {
        kind: 'run',
        run,
        source: remindersProgram,
        linkedFunctions: [],
        terminal: { kind: 'unavailable' },
        request: { ...request, arguments: '["owner@example.com","INV-4171"]' },
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
        source: remindersProgram,
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
/** Parallel calls to one Tool: one Run, three separate decisions. */
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
 * Scrolled review content passes under the sticky review header. The play function
 * scrolls an argument label behind the header so Chromatic records that the header
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
        source: remindersProgram,
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
    // The field label sits above its tall value, so it scrolls fully under the header.
    const heading = within(review).getByText('lines', { selector: 'dt' })
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
    // The header sticks only once the review scrolls, and the drawer animates in,
    // so measure and probe again after each move.
    await waitFor(() => {
      const before = heading.getBoundingClientRect()
      const stuck = header.getBoundingClientRect()
      scrollPort.scrollTop += before.top - stuck.top - (stuck.height - before.height) / 2
      const behind = heading.getBoundingClientRect()
      const pinned = header.getBoundingClientRect()
      expect(behind.top).toBeGreaterThanOrEqual(pinned.top)
      expect(behind.bottom).toBeLessThanOrEqual(pinned.bottom)
      const topmost = canvasElement.ownerDocument.elementFromPoint(
        behind.left + behind.width / 2,
        behind.top + behind.height / 2,
      )
      expect(header.contains(topmost)).toBe(true)
    })
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
/** From a Run's action list, the keyboard reaches each card's review in order, and Enter opens
 * that exact review. */
export const ActionListKeyboard: Story = {
  parameters: { initialEntry: `/workspaces/finance/activity?record=${run.name}` },
  play: async ({ canvasElement, userEvent }) => {
    const body = within(canvasElement.ownerDocument.body)
    const nav = await body.findByRole('navigation', { name: 'Actions in this run' })
    const [mail, update] = within(nav).getAllByRole('link', { name: /^Review / })
    await expect(mail).toHaveAccessibleName('Review coral.providers.mail.send')
    await expect(update).toHaveAccessibleName('Review coral.providers.billing.updateInvoice')
    for (const time of nav.querySelectorAll('time'))
      await expect(time).toHaveTextContent('Expires in 15 minutes')
    mail.focus()
    await userEvent.tab()
    await expect(update).toHaveFocus()
    await userEvent.keyboard('{Enter}')
    // The second action's own review opens, with its own arguments.
    await waitFor(() =>
      expect(body.getByRole('button', { name: 'Approve this call' })).toBeVisible(),
    )
    await expect(body.getAllByText(/reminderSent/).length).toBeGreaterThan(0)
    await expect(body.queryByRole('navigation', { name: 'Actions in this run' })).toBeNull()
  },
}
/** A call waiting on a connection links to that connection on its Run, where Connect and
 * Decline are; the whole card is the one link, reached by keyboard. */
export const ConnectionFromCall: Story = {
  parameters: {
    initialEntry: `/workspaces/finance/activity?record=${accountCall.name}&request=${accountRun.actionPage!.actions[0].name}`,
  },
  globals: { viewport: { value: 'mobile1', isRotated: false } },
  play: async ({ canvasElement, userEvent }) => {
    const body = within(canvasElement.ownerDocument.body)
    const card = await body.findByRole('region', { name: 'Account connection' })
    await expect(card).toHaveTextContent('connection request: pending')
    // One link to the request: the whole card, with no icon and no second "Inspect" row.
    await expect(body.queryByRole('link', { name: 'Inspect account connection' })).toBeNull()
    const open = within(card).getByRole('link', {
      name: 'Open account connection request: pending',
    })
    await expect(card.firstElementChild).toBe(open)
    await expect(open.querySelector('svg, button')).toBeNull()
    const dialog = card.closest('[role="dialog"]') as HTMLElement
    await expect(dialog.scrollWidth).toBeLessThanOrEqual(dialog.clientWidth)
    // Tab reaches the card, and it shows that it has focus.
    open.focus()
    await expect(open).toHaveFocus()
    await expect(getComputedStyle(open).outlineStyle).not.toBe('none')
    await userEvent.keyboard('{Enter}')
    // The connection opens on its Run, with the controls to act on it.
    await waitFor(() =>
      expect(
        body.getByRole('heading', { level: 3, name: 'Connect your Linear account' }),
      ).toBeVisible(),
    )
    await expect(body.getByRole('button', { name: 'Connect Linear' })).toBeVisible()
    await expect(body.getByRole('button', { name: 'Decline' })).toBeVisible()
    await expect(body.queryByRole('region', { name: 'Account connection' })).toBeNull()
  },
}
