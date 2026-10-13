import type { Meta, StoryObj } from '@storybook/react'
import { useEffect, useState } from 'react'
import { createRoutesStub, useLocation, useSearchParams } from 'react-router'
import { expect, fn, waitFor, within } from 'storybook/test'
import { PageLayout } from '@/wax/components/page-layout'
import { trail } from '@/views/management/layout'
import { Activity } from './activity'
import { ActionReviewSession } from '../actions/review'
import { stored } from './discovery/fixtures'
import { discoveryView, functionTarget, operationTarget } from './discovery/response'
import type {
  ActivityPage,
  ActivityRow,
  DiscoveryContext,
  InvocationDetail,
  RunDetail,
} from './types'

/** Highlighted code splits its text across token spans, so match the whole block. */
const codeContaining = (text: string) => (_: string, element: Element | null) =>
  element?.textContent?.includes(text) ?? false

const workspace = 'workspaces/finance'
const task = `${workspace}/tasks/invoices`
const run = `${workspace}/runs/8f3c`
const invocation = `${workspace}/invocations/mail`
const at = '2026-09-18T14:02:00.000Z'
const goal: ActivityRow & { kind: 'task' } = {
  kind: 'task',
  name: task,
  workspace,
  actor: 'Collections agent',
  intent: 'Find overdue invoices and notify their owners',
  outcome: 'open',
  startedAt: at,
  endedAt: null,
  agent: '',
}
const execution: ActivityRow & { kind: 'run' } = {
  kind: 'run',
  name: run,
  workspace,
  actor: 'Collections agent',
  agent: '',
  task,
  state: 'running',
  startedAt: at,
  endedAt: null,
  durationNanos: '242000000000',
  requiresAction: true,
  submission: {
    name: `${task}/interactions/accepted`,
    tool: 'exec',
    intent: 'Send invoice reminders',
    outcome: 'succeeded',
    startedAt: at,
    completedAt: at,
    acceptedRun: run,
    agent: '',
  },
}
const call: ActivityRow & { kind: 'invocation' } = {
  kind: 'invocation',
  name: invocation,
  workspace,
  actor: execution.actor,
  agent: '',
  task,
  run,
  provider: `${workspace}/providers/mail`,
  operation: 'send',
  callPath: 'mail.send',
  state: 'pending',
  outcome: 'unknown',
  startedAt: at,
  endedAt: null,
  durationNanos: '221000000000',
  requiresAction: true,
  dispatchedAt: null,
  waitingNanos: '221000000000',
}
const emission: ActivityRow = {
  kind: 'output',
  name: `${run}/events/7`,
  workspace,
  run,
  label: 'emission #7',
  startedAt: at,
  eventKind: 'program.emitted',
  agent: '',
}
const standalone: ActivityRow & { kind: 'run' } = {
  ...execution,
  submission: undefined,
  task: '',
  name: 'workspaces/support/runs/standalone',
  workspace: 'workspaces/support',
  actor: 'you',
  state: 'failed',
  requiresAction: false,
}
const tool = (
  name: string,
  toolName: string,
  outcome: string,
  intent = 'Find the billing operation',
): ActivityRow => ({
  kind: 'tool',
  name: `${task}/interactions/${name}`,
  workspace,
  task,
  tool: toolName,
  intent,
  outcome,
  startedAt: at,
  completedAt: outcome === 'unknown' ? null : at,
  acceptedRun: '',
  agent: '',
})
const detail: InvocationDetail = {
  kind: 'invocation',
  invocation: call,
  arguments:
    '{"to":"owner@example.com","subject":"Invoice INV-4171 is overdue","body":"Your invoice is ready for review."}',
  result: { kind: 'unknown' },
  requests: [{ kind: 'approval', name: `${workspace}/approvals/mail` }],
  request: null,
  requestUnavailable: false,
}
// An approval is always reviewed inside its Run.
const approvalContext: RunDetail = {
  kind: 'run',
  run: execution,
  submission: execution.submission,
  source: 'export default async () => null',
  linkedFunctions: [],
  terminal: { kind: 'unavailable' },
}
const page: ActivityPage = {
  rows: [
    goal,
    {
      ...goal,
      name: 'workspaces/support/tasks/same-intent',
      workspace: 'workspaces/support',
    },
    standalone,
  ],
  branches: {
    [task]: {
      rows: [
        tool('search', 'search', 'succeeded', 'Find billing operations'),
        tool('describe', 'describe', 'succeeded', 'Inspect invoice inputs and response'),
        tool('rejected', 'exec', 'rejected', 'Prepare the reminder program'),
        execution,
      ],
      next: '',
      unavailable: false,
    },
    [run]: {
      rows: [
        {
          ...call,
          name: `${workspace}/invocations/list`,
          callPath: 'billing.listInvoices',
          state: 'ended',
          outcome: 'upstream_outcome',
          requiresAction: false,
        },
        call,
        {
          ...call,
          name: `${workspace}/invocations/uncertain`,
          callPath: 'crm.getOwner',
          state: 'ended',
          outcome: 'indeterminate',
          requiresAction: false,
        },
        emission,
      ],
      next: '',
      unavailable: false,
    },
  },
  expanded: [task, run],
  hasBefore: false,
  hasAfter: false,
  anchorUnavailable: false,
  listUnavailable: false,
  agent: null,
  detail,
  unavailable: false,
  selected: invocation,
  selectedRow: call,
  listPath: '/workspaces/finance/activity',
}
const meta = {
  title: 'Activity/Hierarchy',
  component: Activity,
  args: { page },
  parameters: { layout: 'fullscreen', chromatic: { viewports: [390, 1440] } },
  decorators: [
    (Story, context) => {
      const Stub = createRoutesStub([
        {
          path: '*',
          Component: () => (
            <ActionReviewSession>
              <Story />
            </ActionReviewSession>
          ),
        },
      ])
      return (
        <div style={{ height: '100vh', display: 'flex', flexDirection: 'column' }}>
          <PageLayout variant="split" breadcrumbs={trail([], 'Activity')}>
            <Stub
              initialEntries={[
                context.parameters.initialEntry ?? '/workspaces/finance/activity?tree=1',
              ]}
            />
          </PageLayout>
        </div>
      )
    },
  ],
} satisfies Meta<typeof Activity>
export default meta
type Story = StoryObj<typeof meta>
export const MixedHistory: Story = {
  play: async ({ canvasElement }) => {
    const drawer = within(await within(canvasElement.ownerDocument.body).findByRole('dialog'))
    await expect(drawer.getByText('Exact logical arguments')).toBeVisible()
    await expect(drawer.getByRole('button', { name: 'Close details' })).toBeVisible()
    await expect(drawer.queryByRole('button', { name: 'Approve' })).not.toBeInTheDocument()
    await expect(drawer.queryByText('Copy link')).not.toBeInTheDocument()
  },
}
export const CompletionFeedback: Story = {
  args: {
    page: {
      ...page,
      selected: task,
      selectedRow: { ...goal, outcome: 'failure', endedAt: at },
      detail: {
        kind: 'task',
        task: { ...goal, outcome: 'failure', endedAt: at },
        feedback:
          'I could not find an operation to export invoices.\nSearch returned no matching operations.',
      },
    },
  },
}
export const StreamingHistory: Story = {
  args: {
    page: { ...page, selected: null, selectedRow: null, detail: null },
    live: { connected: true, followRoot: true, atStart() {}, viewport() {} },
  },
}
export const Empty: Story = {
  args: {
    page: { ...page, rows: [], branches: {}, selected: null, selectedRow: null, detail: null },
    connect: { endpoint: 'https://coral.example/mcp/finance' },
  },
}
export const ObservationUnavailable: Story = {
  args: {
    page: {
      ...page,
      rows: [],
      branches: {},
      selectedRow: null,
      detail: null,
      unavailable: true,
      listUnavailable: true,
    },
    live: { connected: false, followRoot: false, atStart() {}, viewport() {} },
  },
}
export const ContextImpact: Story = {
  args: {
    page: {
      ...page,
      rows: [
        { ...goal, contextImpactBytes: '-1970000' },
        { ...standalone, contextImpactBytes: '0' },
        {
          ...goal,
          name: `${task}-running`,
          contextImpactBytes: '55500',
          contextImpactPartial: true,
        },
        { ...goal, name: `${task}-unavailable` },
      ],
      selected: null,
      selectedRow: null,
      detail: null,
      branches: {
        [task]: {
          rows: [
            { ...tool('search', 'search', 'succeeded'), contextImpactBytes: '50200' },
            { ...tool('describe', 'describe', 'succeeded'), contextImpactBytes: '5300' },
            {
              ...execution,
              state: 'succeeded',
              requiresAction: false,
              contextImpactBytes: '-2025500',
              hasFinalValue: true,
            },
          ],
          next: '',
          unavailable: false,
        },
        [run]: {
          rows: [
            {
              ...call,
              state: 'ended',
              outcome: 'upstream_outcome',
              requiresAction: false,
              contextImpactBytes: '-2040000',
            },
            {
              ...emission,
              name: `${run}/final`,
              eventKind: 'run.ended',
              label: 'final value',
              contextImpactBytes: '14500',
            },
          ],
          next: '',
          unavailable: false,
        },
      },
    },
  },
}

export const SingleLineRows: Story = {
  args: {
    page: {
      ...page,
      rows: [
        { ...goal, intent: 'Find overdue invoices and notify their owners. '.repeat(12) },
        ...page.rows.slice(1),
      ],
      detail: null,
      selected: null,
      selectedRow: null,
    },
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const table = canvas.getByRole('table', { name: 'Activity records' })
    for (const row of within(table).getAllByRole('row')) {
      await expect(row.getBoundingClientRect().height).toBeLessThanOrEqual(44)
    }
    const label = within(table).getByRole('link', {
      name: 'Find overdue invoices and notify their owners. '.repeat(12).trim(),
    }).parentElement!
    await expect(label.scrollWidth).toBeGreaterThan(label.clientWidth)
    await expect(getComputedStyle(label).textOverflow).toBe('ellipsis')
    await expect(within(table).queryByRole('img', { name: /^workspace:/ })).not.toBeInTheDocument()
    await expect(within(table).queryByText('Finance')).not.toBeInTheDocument()
    await expect(within(table).getByRole('img', { name: 'search' })).toBeVisible()
    await expect(within(table).getByRole('link', { name: 'Find billing operations' })).toBeVisible()
    await expect(within(table).getByRole('link', { name: 'Send invoice reminders' })).toBeVisible()
  },
}
export const StatusMarkers: Story = {
  args: {
    page: {
      ...page,
      rows: [
        { ...goal, outcome: 'success', endedAt: at },
        {
          ...goal,
          name: `${task}-failed`,
          intent: 'Locate the missing account',
          outcome: 'failure',
          endedAt: at,
        },
        { ...standalone, state: 'succeeded' },
      ],
      branches: {
        [task]: {
          rows: [{ ...execution, state: 'succeeded', requiresAction: false }],
          next: '',
          unavailable: false,
        },
        [run]: {
          rows: [
            {
              ...call,
              state: 'ended',
              outcome: 'upstream_outcome',
              requiresAction: false,
              callPath: 'coral.providers.linear.listIssues',
            },
            {
              ...call,
              name: `${invocation}-failed`,
              state: 'ended',
              outcome: 'coral_failure',
              requiresAction: false,
              callPath: 'coral.providers.linear.updateIssue',
            },
            {
              ...call,
              name: `${invocation}-uncertain`,
              state: 'ended',
              outcome: 'indeterminate',
              requiresAction: false,
              callPath: 'coral.providers.linear.createIssue',
            },
          ],
          next: '',
          unavailable: false,
        },
      },
      detail: null,
      selected: null,
      selectedRow: null,
    },
  },
}
export const Collapsed: Story = {
  args: {
    page: {
      ...page,
      branches: {},
      expanded: [],
      detail: null,
      selected: null,
      selectedRow: null,
    },
  },
}
export const ScrollablePage: Story = {
  args: {
    page: {
      ...page,
      rows: Array.from({ length: 25 }, (_, i) => ({
        ...goal,
        name: `${task}-${i}`,
        intent: `Task ${i + 1}`,
      })),
      branches: {},
      expanded: [],
      detail: null,
      selected: null,
      selectedRow: null,
      hasAfter: true,
    },
  },
  play: async ({ canvasElement }) => {
    const list = within(canvasElement).getByRole('table', { name: 'Activity records' })
    list.scrollTop = 400
    await expect(list.scrollTop).toBeGreaterThan(0)
  },
}
const busyTask = {
  ...page,
  rows: [goal, { ...goal, name: `${task}-next`, intent: 'Reconcile the ledger' }],
  branches: {
    [task]: {
      rows: [
        ...Array.from({ length: 30 }, (_, i) =>
          tool(`search-${i}`, 'search', 'succeeded', `Search ${i + 1}`),
        ),
        { ...execution, state: 'succeeded' as const, requiresAction: false },
        ...Array.from({ length: 30 }, (_, i) =>
          tool(`describe-${i}`, 'describe', 'succeeded', `Describe ${i + 1}`),
        ),
      ],
      next: '',
      unavailable: false,
    },
    [run]: {
      rows: Array.from({ length: 40 }, (_, i) => ({
        ...call,
        name: `${workspace}/invocations/${i}`,
        callPath: `billing.listInvoices.${i}`,
        state: 'ended' as const,
        outcome: 'upstream_outcome',
        requiresAction: false,
      })),
      next: '',
      unavailable: false,
    },
  },
  expanded: [task, run],
  detail: null,
  selected: null,
  selectedRow: null,
} satisfies ActivityPage
const recordRow = (table: HTMLElement, name: RegExp | string) =>
  within(table)
    .getAllByRole('row')
    .find((row) => within(row).queryByRole('link', { name }))!
/**
 * Scroll to see an expanded Task pin under its day heading and an expanded Run pin
 * under its Task. Wax tests the pinning; Vitest tests that Activity nests the groups.
 */
export const PinnedHeadings: Story = {
  args: { page: busyTask },
}
const collapsedRun: ActivityRow & { kind: 'run' } = {
  ...execution,
  name: `${run}-collapsed`,
  submission: { ...execution.submission!, intent: 'Archive the paid invoices' },
}
/**
 * Expanding a record turns its row into a group heading without moving its toggle
 * or label, at the root and inside a Task. Wax owns the heading's cell layout.
 */
export const HeadingAlignment: Story = {
  args: {
    page: {
      ...page,
      rows: [goal, { ...goal, name: `${task}-collapsed`, intent: 'Reconcile the ledger' }],
      branches: {
        [task]: { rows: [execution, collapsedRun], next: '', unavailable: false },
        [run]: { rows: [], next: '', unavailable: false },
      },
      expanded: [task, run],
      detail: null,
      selected: null,
      selectedRow: null,
    },
  },
  play: async ({ canvasElement }) => {
    const row = (name: string) => recordRow(canvasElement, name)
    const left = (element: Element) => element.getBoundingClientRect().left
    for (const [expanded, collapsed] of [
      [goal.intent, 'Reconcile the ledger'],
      ['Send invoice reminders', 'Archive the paid invoices'],
    ] as const) {
      const heading = within(row(expanded))
      const plain = within(row(collapsed))
      await expect(left(heading.getByRole('button', { name: 'Collapse record' }))).toBe(
        left(plain.getByRole('button', { name: 'Expand record' })),
      )
      await expect(left(heading.getByRole('link'))).toBe(left(plain.getByRole('link')))
    }
  },
}
// Enough history to scroll well past 5000px collapsed. Tasks hold 0, 1 or 3
// executions, and executions hold 0, 1 or 30 operations, in rotation.
const largeCounts = { executions: [0, 1, 3], operations: [0, 1, 30] } as const
// One Task every 45 minutes back from now, across several UTC days.
const largeStart = (i: number, minutes = 0) =>
  new Date(Math.floor(Date.now() / 60_000) * 60_000 - (i * 45 + minutes) * 60_000).toISOString()
const largeTasks = Array.from({ length: 120 }, (_, i): ActivityRow & { kind: 'task' } => ({
  ...goal,
  startedAt: largeStart(i),
  name: `${workspace}/tasks/t${i}`,
  intent: `Task ${i + 1}: reconcile invoices for account ${1000 + i}`,
  outcome: i % 7 === 0 ? 'open' : i % 5 === 0 ? 'failure' : 'success',
  endedAt: i % 7 === 0 ? null : at,
}))
const largeRun = (
  name: string,
  taskName: string,
  i: number,
  operations: number,
  startedAt: string,
): ActivityRow & { kind: 'run' } => ({
  ...execution,
  name,
  task: taskName,
  startedAt,
  state: i % 4 === 0 ? 'running' : i % 6 === 0 ? 'failed' : 'succeeded',
  requiresAction: false,
  submission: taskName
    ? {
        ...execution.submission!,
        intent: `Send reminder batch ${i + 1} (${operations} operation${operations === 1 ? '' : 's'})`,
        acceptedRun: name,
      }
    : undefined,
})
const largeBranchOf = (rows: ActivityRow[]) => ({ rows, next: '', unavailable: false })
// The whole fixture, built once: every record and every branch it can expand.
const largeRows = new Map<string, ActivityRow>()
const largeBranches: ActivityPage['branches'] = {}
function addLargeRun(row: ActivityRow & { kind: 'run' }, operations: number) {
  largeRows.set(row.name, row)
  largeBranches[row.name] = largeBranchOf(
    Array.from({ length: operations }, (_, k) => ({
      ...call,
      name: `${row.name}/invocations/${k}`,
      run: row.name,
      startedAt: row.startedAt,
      callPath: `billing.listInvoices.page${k + 1}`,
      state: 'ended' as const,
      outcome: k % 9 === 8 ? 'coral_failure' : 'upstream_outcome',
      requiresAction: false,
    })),
  )
  return row
}
const largeRoots: ActivityRow[] = largeTasks.flatMap((parent, i) => {
  largeRows.set(parent.name, parent)
  const id = `t${i}`
  const lookup = (k: number, intent = `Look up step ${k + 1}`): ActivityRow => ({
    ...(tool(`${id}-${k}`, k % 2 ? 'describe' : 'search', 'succeeded', intent) as ActivityRow & {
      kind: 'tool'
    }),
    task: parent.name,
    startedAt: parent.startedAt,
  })
  // Two lookups before each execution, and two after the last.
  const children: ActivityRow[] = [
    ...Array.from({ length: largeCounts.executions[i % 3]! }, (_, j) => {
      const operations = largeCounts.operations[(i + j) % 3]!
      return [
        lookup(j * 2),
        lookup(j * 2 + 1),
        addLargeRun(
          largeRun(`${workspace}/runs/${id}-r${j}`, parent.name, j, operations, parent.startedAt),
          operations,
        ),
      ]
    }).flat(),
    lookup(98, 'Find the reconciled invoices'),
    lookup(99, 'Check the reconciled totals'),
  ]
  for (const child of children) largeRows.set(child.name, child)
  largeBranches[parent.name] = largeBranchOf(children)
  if (i % 3) return [parent]
  const operations = largeCounts.operations[(i / 3) % 3]!
  const solo = largeRun(`${workspace}/runs/s${i}`, '', i, operations, largeStart(i, 20))
  return [parent, addLargeRun(solo, operations)]
})
/** A long history. Expand and collapse records, and scroll to watch their headings pin. */
export const LargeHistory: Story = {
  parameters: {
    initialEntry: `/workspaces/finance/activity?tree=1&expand=${largeTasks[2]!.name}&expand=${workspace}/runs/t2-r0`,
    chromatic: { disableSnapshot: true },
  },
  render: function LargeHistory() {
    const [query] = useSearchParams()
    const expanded = query.getAll('expand')
    const branches = Object.fromEntries(
      expanded.filter((name) => largeBranches[name]).map((name) => [name, largeBranches[name]!]),
    )
    const selected = query.get('record')
    const selectedRow = (selected && largeRows.get(selected)) || null
    return (
      <Activity
        page={{
          ...page,
          rows: largeRoots,
          branches,
          expanded,
          selected: selectedRow ? selected : null,
          selectedRow,
          detail: selectedRow?.kind === 'task' ? { kind: 'task', task: selectedRow } : null,
          pending: undefined,
        }}
      />
    )
  },
}
/**
 * Press ↓ in the open detail, then Escape before the next record arrives. The
 * selection reaches the page 100 ms after the URL, as a loader's does, so the
 * step lands while the drawer closes. The drawer stays closed. It needs the
 * drawer's real exit animation, so it runs in a browser.
 */
export const EscapeWhileStepping: Story = {
  parameters: {
    initialEntry: `/workspaces/finance/activity?tree=1&record=${largeTasks[0]!.name}`,
    chromatic: { disableSnapshot: true },
  },
  render: function EscapeWhileStepping() {
    const { search } = useLocation()
    const [loaded, setLoaded] = useState(search)
    const [visited, setVisited] = useState<string[]>([])
    useEffect(() => {
      setVisited((before) => [...before, search])
      const timer = setTimeout(() => setLoaded(search), 100)
      return () => clearTimeout(timer)
    }, [search])
    const selected = new URLSearchParams(loaded).get('record')
    const selectedRow = (selected && largeRows.get(selected)) || null
    return (
      <>
        <output data-testid="location">{search}</output>
        <output data-testid="visited">{visited.join(' ')}</output>
        <output data-testid="loaded">{loaded}</output>
        <Activity
          page={{
            ...page,
            rows: largeRoots,
            branches: {},
            expanded: [],
            selected: selectedRow ? selected : null,
            selectedRow,
            detail: selectedRow?.kind === 'task' ? { kind: 'task', task: selectedRow } : null,
            pending: undefined,
          }}
        />
      </>
    )
  },
  play: async ({ canvasElement, userEvent }) => {
    const screen = within(canvasElement.ownerDocument.body)
    const dialog = await screen.findByRole('dialog', { name: 'Activity details' })
    await waitFor(() => expect(dialog).toHaveFocus())
    // The records the URL has named, in order, and the one the page now shows.
    const named = () => [
      ...new Set(
        (screen.getByTestId('visited').textContent ?? '')
          .split(' ')
          .map((search) => new URLSearchParams(search).get('record'))
          .filter(Boolean),
      ),
    ]
    const loaded = () => new URLSearchParams(screen.getByTestId('loaded').textContent ?? '')
    await userEvent.keyboard('{ArrowDown}{Escape}')
    // The step reaches the page while the drawer closes. A closing drawer steps no further.
    await waitFor(() => expect(named()).toHaveLength(2))
    await waitFor(() => expect(loaded().get('record')).toBe(named()[1]))
    await userEvent.keyboard('{ArrowDown}')
    await waitFor(() =>
      expect(screen.queryByRole('dialog', { name: 'Activity details' })).not.toBeInTheDocument(),
    )
    await waitFor(() => expect(screen.getByTestId('location').textContent).not.toContain('record='))
    await expect(named()).toHaveLength(2)
    await expect(screen.getByRole('table', { name: 'Activity records' })).toHaveFocus()
  },
}
/** Whether all of `row` shows: its top and bottom edges are not under a pinned heading. */
const shownInFull = (row: HTMLElement) => {
  const bounds = row.getBoundingClientRect()
  const x = bounds.left + bounds.width / 2
  return [bounds.top + 1, bounds.bottom - 1].every((y) =>
    row.contains(row.ownerDocument.elementFromPoint(x, y)),
  )
}
/**
 * Press ↓ and ↑ to move the keyboard cursor through the tree, Space to toggle
 * expansion, and Enter to open the detail. Its row shows in full below the pinned
 * headings. Vitest covers the traversal rules and focus; this needs a real layout.
 */
export const KeyboardCursor: Story = {
  parameters: LargeHistory.parameters,
  render: LargeHistory.render,
  play: async ({ canvasElement, userEvent }) => {
    const canvas = within(canvasElement)
    const screen = within(canvasElement.ownerDocument.body)
    const list = canvas.getByRole('table', { name: 'Activity records' })
    const cursor = () => list.querySelector<HTMLElement>('[role="row"][data-active]')
    // The table's keys act once the table itself has focus.
    list.focus()
    // Down into the expanded Task and its Run, then back up, each row in full view.
    for (const key of [...Array(40).fill('{ArrowDown}'), ...Array(40).fill('{ArrowUp}')]) {
      await userEvent.keyboard(key)
      await waitFor(() => expect(shownInFull(cursor()!)).toBe(true))
    }
    await expect(list.scrollTop).toBe(0)
    // The cursor, not an outer table ring, shows where keyboard actions apply.
    list.focus()
    await expect(list).toHaveFocus()
    await expect(getComputedStyle(list).outlineStyle).toBe('none')
    await expect(cursor()).not.toBeNull()
    // Past the first Task's solo Run to the next Task; rows in view do not move.
    await userEvent.keyboard('{ArrowDown}{ArrowDown}')
    await expect(list.scrollTop).toBe(0)
    await userEvent.keyboard('{Enter}')
    const dialog = await screen.findByRole('dialog', { name: 'Activity details' })
    await within(dialog).findByRole('heading', { name: largeTasks[1]!.intent })
    dialog.scrollTop = dialog.scrollHeight
    // ↓ reaches the expanded Task after it, in the same drawer, from its top.
    await userEvent.keyboard('{ArrowDown}')
    await within(dialog).findByRole('heading', { name: largeTasks[2]!.intent })
    await expect(screen.getByRole('dialog', { name: 'Activity details' })).toBe(dialog)
    await expect(dialog.scrollTop).toBe(0)
  },
}
/**
 * Records on both sides of midnight UTC sit under their own day headings. This is
 * the one home for the midnight split, so it runs in a real browser.
 */
export const DayBoundary: Story = {
  args: {
    page: {
      ...page,
      rows: [
        {
          ...goal,
          name: `${task}-after`,
          intent: 'Send the morning digest',
          startedAt: '2026-09-29T00:04:00.000Z',
        },
        { ...standalone, startedAt: '2026-09-28T23:58:00.000Z' },
        {
          ...goal,
          name: `${task}-evening`,
          intent: 'Close the day ledger',
          startedAt: '2026-09-28T21:30:00.000Z',
        },
      ],
      branches: {},
      expanded: [],
      detail: null,
      selected: null,
      selectedRow: null,
    },
  },
  play: async ({ canvasElement }) => {
    const day = (name: string) => within(within(canvasElement).getByRole('rowgroup', { name }))
    await expect(day('Tue 29 Sep 2026').getByText('00:04:00')).toBeVisible()
    await expect(day('Mon 28 Sep 2026').getByText('23:58:00')).toBeVisible()
    await expect(day('Mon 28 Sep 2026').getByText('21:30:00')).toBeVisible()
    await expect(day('Tue 29 Sep 2026').queryByText('23:58:00')).toBeNull()
  },
}

const loadOlder = fn()
const loadNewer = fn()
/**
 * The list loads more records at either end. A load in flight shows a quiet
 * spinner; a failed load keeps the records and offers a retry.
 */
export const LoadMoreRecords: Story = {
  args: {
    page: {
      ...page,
      selected: null,
      selectedRow: null,
      detail: null,
      hasBefore: true,
      hasAfter: true,
    },
    list: {
      count: 500,
      newer: { load: loadNewer, loading: false, failed: true, automatic: false },
      older: { load: loadOlder, loading: true, failed: false, automatic: true },
    },
  },
  play: async ({ canvasElement, userEvent }) => {
    const canvas = within(canvasElement)
    await expect(canvas.getByText('The list shows 500 records.')).toBeInTheDocument()
    // The error only states what failed; the button next to it says what to do.
    await expect(canvas.getByRole('alert')).toHaveTextContent(/^The records did not load\.$/)
    await userEvent.click(canvas.getByRole('button', { name: 'Try again' }))
    await expect(loadNewer).toHaveBeenCalledOnce()
    // A load in flight says so quietly; there is nothing to click.
    await expect(
      canvas
        .getAllByRole('status')
        .some((status) => status.textContent?.includes('Loading older records…')),
    ).toBe(true)
    await expect(canvas.queryByRole('button', { name: /older records/ })).toBeNull()
    await expect(loadOlder).not.toHaveBeenCalled()
  },
}

export const DrawerNavigation: Story = {
  render: function DrawerNavigation() {
    const [query] = useSearchParams()
    const rows = Array.from({ length: 40 }, (_, i) => ({
      ...goal,
      name: `${task}-${i}`,
      intent: `Task ${i + 1}`,
    }))
    const selected = query.get('record')
    const selectedRow = rows.find((row) => row.name === selected) ?? null
    return (
      <Activity
        page={{
          ...page,
          rows,
          branches: {},
          expanded: [],
          selected,
          selectedRow,
          detail: selectedRow ? { kind: 'task', task: selectedRow } : null,
        }}
      />
    )
  },
  play: async ({ canvasElement, userEvent }) => {
    const canvas = within(canvasElement)
    const screen = within(canvasElement.ownerDocument.body)
    const list = canvas.getByRole('table', { name: 'Activity records' })
    const heading = within(list).getByRole('columnheader', { name: 'RECORD' })
    const top = heading.getBoundingClientRect().top
    list.scrollTop = list.scrollHeight
    await waitFor(() => expect(list.scrollTop).toBeGreaterThan(0))
    await expect(heading.getBoundingClientRect().top).toBe(top)
    const before = list.scrollTop
    for (const close of ['escape', 'button']) {
      await userEvent.click(within(list).getByRole('link', { name: 'Task 40' }))
      const dialog = await screen.findByRole('dialog', { name: 'Activity details' })
      await expect(within(dialog).getByRole('heading', { name: 'Task 40' })).toBeVisible()
      await waitFor(() =>
        expect(dialog.contains(canvasElement.ownerDocument.activeElement)).toBe(true),
      )
      // The drawer rests against the right edge once it has slid in.
      await waitFor(() =>
        expect(Math.abs(dialog.getBoundingClientRect().right - window.innerWidth)).toBeLessThan(2),
      )
      if (close === 'escape') await userEvent.keyboard('{Escape}')
      else await userEvent.click(within(dialog).getByRole('button', { name: 'Close details' }))
      await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull())
      await expect(list.scrollTop).toBe(before)
      await expect(canvas.getByRole('heading', { name: 'Your recent activity' })).toBeVisible()
    }
  },
}
export const ExpansionLimit: Story = {
  args: {
    page: {
      ...page,
      rows: Array.from({ length: 25 }, (_, i) => ({
        ...goal,
        name: `${task}-${i}`,
        intent: `Task ${i + 1}`,
      })),
      expanded: Array.from({ length: 20 }, (_, i) => `${task}-${i}`),
      branches: {},
      detail: null,
      selected: null,
      selectedRow: null,
    },
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    for (const control of canvas.getAllByRole('button', { name: 'Expand record' }))
      await expect(control).toBeDisabled()
    for (const control of canvas.getAllByRole('button', { name: 'Collapse record' }))
      await expect(control).toBeEnabled()
    await expect(canvas.queryByRole('navigation', { name: 'Activity scope' })).toBeNull()
    await expect(canvas.queryByRole('link', { name: /Scope to / })).toBeNull()
  },
}
export const StandaloneExecution: Story = {
  args: {
    page: {
      ...page,
      selected: standalone.name,
      selectedRow: standalone,
      detail: {
        kind: 'run',
        run: standalone,
        source: 'return await coral.providers.billing.listInvoices({});',
        linkedFunctions: [],
        terminal: {
          kind: 'failed',
          code: 'program_rejected',
          message: 'The program rejected after publishing an emission.',
          error: null,
        },
      },
    },
  },
}
export const RetainedSource: Story = {
  args: {
    page: {
      ...page,
      selected: run,
      selectedRow: execution,
      detail: {
        kind: 'run',
        run: { ...execution, state: 'succeeded', requiresAction: false },
        source: 'return coral.functions.billing.calculate();',
        linkedFunctions: [
          {
            savedFunction: 'old-function-id',
            path: 'billing.calculate',
            source: 'export default () => 41;',
          },
        ],
        terminal: { kind: 'succeeded', finalValue: 'null' },
      },
    },
  },
}
export const FinalValueOnExec: Story = {
  args: {
    page: {
      ...page,
      selected: run,
      selectedRow: { ...execution, hasFinalValue: true },
      branches: {
        [task]: {
          rows: [{ ...execution, state: 'succeeded', requiresAction: false, hasFinalValue: true }],
          next: '',
          unavailable: false,
        },
        [run]: {
          rows: [
            emission,
            {
              ...emission,
              name: `${run}/final`,
              eventKind: 'run.ended',
              label: 'final value',
              contextImpactBytes: '15',
            },
          ],
          next: '',
          unavailable: false,
        },
      },
      detail: {
        kind: 'run',
        run: { ...execution, state: 'succeeded', requiresAction: false },
        source: 'coral.emit({progress: "ready"}); return {invoices: 24};',
        linkedFunctions: [],
        terminal: { kind: 'succeeded', finalValue: '{"invoices":24}' },
      },
    },
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement)
    const table = canvas.queryByRole('table', { name: 'Activity records' })
    if (table) {
      await expect(within(table).getByRole('img', { name: 'final value' })).toBeVisible()
      await expect(within(table).getByRole('link', { name: 'final value' })).toBeVisible()
    }
    const drawer = within(await within(canvasElement.ownerDocument.body).findByRole('dialog'))
    await expect(
      drawer.getByText(codeContaining('"invoices": 24'), { selector: 'pre' }),
    ).toBeVisible()
  },
}
export const NoFinalValue: Story = {
  args: {
    page: {
      ...page,
      selected: run,
      selectedRow: execution,
      detail: {
        kind: 'run',
        run: { ...execution, state: 'succeeded', requiresAction: false },
        source: 'console.info("done");',
        linkedFunctions: [],
        terminal: { kind: 'succeeded', finalValue: null },
      },
    },
  },
}
export const Upstream404: Story = {
  args: {
    page: {
      ...page,
      detail: {
        ...detail,
        result: {
          kind: 'upstream_outcome',
          value: '{"data":{"message":"Not found"},"metadata":{"status":404}}',
        },
      },
    },
  },
}
export const ExactNativeInteger: Story = {
  args: {
    page: {
      ...page,
      detail: {
        ...detail,
        invocation: {
          ...call,
          state: 'ended',
          outcome: 'upstream_outcome',
          endedAt: at,
          dispatchedAt: at,
          waitingNanos: '0',
          requiresAction: false,
        },
        requests: [],
        result: {
          kind: 'upstream_outcome',
          value: '{"data":{"id":9007199254740993},"metadata":{"status":200}}',
        },
      },
    },
  },
  play: async ({ canvasElement }) => {
    const value = await within(canvasElement.ownerDocument.body).findByText(
      codeContaining('9007199254740993'),
      {
        selector: 'pre',
      },
    )
    await expect(value).toBeVisible()
    await expect(value.textContent).toContain('\n  "data": {\n    "id": 9007199254740993')
  },
}
export const CoralFailure: Story = {
  args: {
    page: {
      ...page,
      detail: {
        ...detail,
        result: {
          kind: 'coral_failure',
          code: 'operation_disabled',
          message: 'The operation is disabled.',
        },
      },
    },
  },
}
export const Indeterminate: Story = {
  args: {
    page: {
      ...page,
      detail: {
        ...detail,
        invocation: {
          ...call,
          state: 'ended',
          outcome: 'indeterminate',
          dispatchedAt: at,
          waitingNanos: '0',
          requiresAction: false,
        },
        result: { kind: 'indeterminate' },
      },
    },
  },
}
export const EmissionAfterFailure: Story = {
  args: {
    page: {
      ...page,
      selected: emission.name,
      selectedRow: emission,
      detail: {
        kind: 'output',
        run,
        event: {
          kind: 'program.emitted',
          sequence: '7',
          occurredAt: at,
          value: '{"invoices":24,"groupedBy":"owner"}',
        },
      },
    },
  },
}
const searched: DiscoveryContext = {
  kind: 'search',
  query: 'github pulls',
  provider: null,
  limit: 5,
  cursor: null,
}
/** A retained `search` or `describe` attempt, selected in the list. */
function discovery(context: DiscoveryContext, response: string): Story['args'] {
  const view = discoveryView(context, response)
  // The loader resolves these links against the current catalog.
  const targets =
    view.kind === 'search' ? view.page.hits : view.kind === 'describe' ? [view.description] : []
  for (const target of targets) {
    const operation = operationTarget(target.callPath)
    const path = functionTarget(target.callPath)
    target.currentHref = operation
      ? `/${workspace}/providers/${operation.namespace}/ops/${operation.relativePath}`
      : `/${workspace}/functions/${path}`
  }
  const name = context.kind === 'describe' ? 'describe' : 'search'
  const row = tool(name, name, 'succeeded', 'Find the operation that merges a pull request')
  return {
    page: {
      ...page,
      selected: row.name,
      selectedRow: row,
      detail: {
        kind: 'tool',
        task,
        interaction: {
          name: row.name,
          agent: '',
          tool: name,
          intent: 'Find the operation that merges a pull request',
          outcome: 'succeeded',
          startedAt: at,
          completedAt: at,
          acceptedRun: '',
        },
        evidence: { kind: 'discovery', context, response, view },
      },
    },
  }
}
const openDetail = async (canvasElement: HTMLElement) =>
  within(await within(canvasElement.ownerDocument.body).findByRole('dialog'))

export const HistoricalSearch: Story = {
  args: discovery(searched, stored.complete),
  play: async ({ canvasElement }) => {
    const opened = await openDetail(canvasElement)
    // The intent and the query are different texts, each under its own label.
    await expect(
      opened.getByText('Intent: Find the operation that merges a pull request'),
    ).toBeVisible()
    await expect(opened.getByText('Query').nextElementSibling).toHaveTextContent('“github pulls”')
    const coverage = within(opened.getByRole('group', { name: 'Search coverage' }))
    await expect(coverage.getByText('100 found')).toBeVisible()
    await expect(coverage.getByText('1 of 1 providers searched, more available')).toBeVisible()
    await expect(coverage.getByText('none match')).toBeVisible()
    const results = within(opened.getByRole('list', { name: 'Results in rank order' }))
    await expect(results.getAllByRole('listitem')).toHaveLength(5)
    // The stored response stays available, closed, after the results.
    await expect(opened.getByText('Raw response').closest('details')).not.toHaveAttribute('open')
  },
}
export const HistoricalSearchPartialCoverage: Story = {
  args: discovery(searched, stored.partial),
  play: async ({ canvasElement }) => {
    const coverage = within(
      (await openDetail(canvasElement)).getByRole('group', { name: 'Search coverage' }),
    )
    await expect(coverage.getByText('1 of 3 providers failed')).toBeVisible()
    await expect(coverage.getByText('Results may be missing.')).toBeVisible()
  },
}
export const HistoricalSearchFailedCoverage: Story = {
  args: discovery(searched, stored.error),
  play: async ({ canvasElement }) => {
    const coverage = within(
      (await openDetail(canvasElement)).getByRole('group', { name: 'Search coverage' }),
    )
    await expect(coverage.getByText('could not search')).toBeVisible()
    await expect(coverage.getByText('Results are missing.')).toBeVisible()
  },
}
/** Both kinds of result, with flags, a guidance match, and matched declaration paths. */
export const HistoricalSearchMixedResults: Story = {
  args: discovery({ ...searched, query: 'github issues by label' }, stored.mixed),
  play: async ({ canvasElement }) => {
    const opened = await openDetail(canvasElement)
    const results = opened.getByRole('list', { name: 'Results in rank order' })
    const [savedFunction, merge] = within(results).getAllByRole('listitem')
    await expect(within(savedFunction).getByText('saved function')).toBeVisible()
    await expect(
      within(savedFunction).getByRole('link', { name: 'Open current function' }),
    ).toBeVisible()
    for (const flag of ['approval required', 'destructive', 'not idempotent', 'tag: pulls'])
      await expect(within(merge).getByText(flag)).toBeVisible()
    await expect(within(merge).getByText('Guidance: API method documentation')).toBeVisible()
    // Reef says what Coral stored in its own words. Only the raw response keeps Coral's.
    for (const copy of [opened.getByRole('group', { name: 'Search coverage' }), results])
      await expect(copy).not.toHaveTextContent(/retriever|saved_function|results_found/)
  },
}
/** A response from before Search Hits. Reef shows the stored text. */
export const HistoricalSearchEarlierShape: Story = {
  args: discovery(searched, stored.earlierShape),
  play: async ({ canvasElement }) => {
    const opened = await openDetail(canvasElement)
    await expect(opened.getByText('Stored in an earlier shape')).toBeVisible()
    await expect(
      opened.getByText(codeContaining('"typescript_declaration"'), { selector: 'pre' }),
    ).toBeVisible()
  },
}
/** The declaration is longer than the cap, so it scrolls inside its block. */
export const HistoricalDescribeOperation: Story = {
  args: discovery(
    { kind: 'describe', callPath: 'coral.providers.github.issues.create', types: [] },
    stored.describeOperation,
  ),
  play: async ({ canvasElement }) => {
    const opened = await openDetail(canvasElement)
    await expect(opened.getByRole('heading', { name: 'Create an issue' })).toBeVisible()
    // The raw response holds the same text, later in the detail.
    const [declaration] = opened.getAllByText(codeContaining('namespace issues'), {
      selector: 'pre',
    })
    // The block shows 20 of the 24 lines; the other lines scroll.
    await expect(declaration.scrollHeight - declaration.clientHeight).toBeGreaterThan(60)
  },
}
export const HistoricalDescribeSavedFunction: Story = {
  args: discovery(
    {
      kind: 'describe',
      callPath: 'coral.functions.github.openIssuesByLabel',
      types: ['Issue', 'Label'],
    },
    stored.describeFunction,
  ),
  play: async ({ canvasElement }) => {
    const opened = await openDetail(canvasElement)
    // The request states the Call Path, under its label, as the search states its query.
    await expect(opened.getByText('Call path').nextElementSibling).toHaveTextContent(
      'coral.functions.github.openIssuesByLabel',
    )
    // The types the agent asked for, when it named any.
    await expect(opened.getByText('Types').nextElementSibling).toHaveTextContent('IssueLabel')
    await expect(opened.getByRole('heading', { name: 'Source' })).toBeVisible()
    await expect(opened.getByText('error TS2304')).toBeVisible()
    await expect(opened.getByText("Cannot find name 'coral'.")).toBeVisible()
  },
}
export const UnknownCompletion: Story = {
  args: {
    page: {
      ...page,
      selected: `${task}/interactions/unknown`,
      selectedRow: tool('unknown', 'exec', 'unknown'),
      detail: {
        kind: 'tool',
        task,
        interaction: {
          name: `${task}/interactions/unknown`,
          agent: '',
          tool: 'exec',
          intent: 'Send reminders',
          outcome: 'unknown',
          startedAt: at,
          completedAt: null,
          acceptedRun: '',
        },
        evidence: { kind: 'unknown' },
      },
    },
  },
}
export const Unavailable: Story = {
  args: {
    page: { ...page, detail: null, selectedRow: null, unavailable: true },
  },
}
export const OutsideWorkspace: Story = {
  args: {
    page: {
      ...page,
      rows: [standalone],
      branches: {},
      expanded: [],
    },
  },
}
export const PendingApproval: Story = {
  args: {
    page: {
      ...page,
      detail: {
        ...detail,
        context: approvalContext,
        request: {
          kind: 'approval',
          actionable: true,
          name: `${workspace}/approvals/mail`,
          run,
          invocation,
          provider: call.provider,
          callPath: call.callPath,
          arguments: JSON.stringify({
            recipient: 'owner@example.com',
            lines: Array.from({ length: 80 }, (_, i) => `Invoice ${i + 1}`),
          }),
          state: 'pending',
          deadline: at,
          approver: '',
          decidedAt: null,
          comment: null,
        },
      },
    },
  },
}
export const ResolvedApproval: Story = {
  args: {
    page: {
      ...page,
      detail: {
        ...detail,
        context: approvalContext,
        request: {
          kind: 'approval',
          name: `${workspace}/approvals/mail`,
          run,
          invocation,
          provider: call.provider,
          callPath: call.callPath,
          arguments: detail.arguments,
          state: 'approved',
          deadline: at,
          approver: 'Reviewing owner',
          decidedAt: at,
          comment: 'Checked the exact invoice recipient.',
        },
      },
    },
  },
}
