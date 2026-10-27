import type { Meta, StoryObj } from '@storybook/react-vite'
import { createRoutesStub, useSearchParams } from 'react-router'
import { expect, fn, waitFor, within } from 'storybook/test'
import { PageLayout } from '@/wax/components/page-layout'
import { trail } from '@/views/management/layout'
import { resizeViewport } from '../../../.storybook/browser'
import { Activity } from './activity'
import { ActionReviewSession } from '../actions/review'
import { stored } from './discovery/fixtures'
import { discoveryView, functionTarget } from './discovery/response'
import { programAt } from './side-panel-fixtures'
import { toolOwner } from '@/lib/call-root'
import { freezeStoryClock, minutesAfterStoryNow } from './story-clock'
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

/** Each panel header centres its icon (when it has one), its heading's first line, its status
 * and its close control on one line, whether the heading wraps or not. */
async function expectAlignedHeaders(dialog: HTMLElement) {
  const centre = (element: Element) => {
    const box = element.getBoundingClientRect()
    return box.top + box.height / 2
  }
  const headers = [...dialog.querySelectorAll('header')].filter((header) =>
    header.querySelector(':scope > div > h2, :scope > div > h3'),
  )
  await expect(headers.length).toBeGreaterThan(0)
  for (const header of headers) {
    const heading = header.querySelector('h2, h3')!
    const style = getComputedStyle(heading)
    const firstLine =
      heading.getBoundingClientRect().top +
      parseFloat(style.paddingTop) +
      parseFloat(style.lineHeight) / 2
    // The heading's own container is never a leading mark; only a sibling icon is.
    const leadingIcon = header.firstElementChild!.contains(heading)
      ? []
      : [header.firstElementChild!]
    const marks = [...leadingIcon, ...header.lastElementChild!.children]
    for (const mark of marks)
      await expect(Math.abs(centre(mark) - firstLine)).toBeLessThanOrEqual(1)
  }
}

/** The element that scrolls `pane`'s content: the drawer itself, or a split pane's viewport. */
function scrollerIn(pane: HTMLElement) {
  if (/auto|scroll/.test(getComputedStyle(pane).overflowY)) return pane
  return [...pane.querySelectorAll<HTMLElement>('*')].find((element) =>
    /auto|scroll/.test(getComputedStyle(element).overflowY),
  )!
}

/** The selected record's decision sits below its scrolling evidence, never inside it: it
 * stays in view, the evidence's fade never covers it, and a click reaches each control. */
async function expectDecisionBelowEvidence(pane: HTMLElement, names: string[]) {
  const scroller = scrollerIn(pane)
  for (const name of names) {
    // One control per choice: the footer is not repeated inside the evidence.
    const [control, ...copies] = within(pane).getAllByRole('button', { name })
    await expect(copies).toEqual([])
    await expect(scroller).not.toContainElement(control!)
    await waitFor(() => {
      const box = control!.getBoundingClientRect()
      const bounds = pane.getBoundingClientRect()
      expect(box.top).toBeGreaterThanOrEqual(scroller.getBoundingClientRect().bottom)
      expect(box.bottom).toBeLessThanOrEqual(bounds.bottom)
      for (const y of [box.top + 2, box.top + box.height / 2, box.bottom - 2])
        expect(control).toContainElement(
          document.elementFromPoint(box.left + box.width / 2, y) as HTMLElement,
        )
    })
  }
}

/** Reading the evidence never scrolls the decision out of the selected record's pane. */
async function expectPinnedFooter(pane: HTMLElement, names: string[]) {
  const scroller = scrollerIn(pane)
  await waitFor(() => expect(scroller.scrollHeight).toBeGreaterThan(scroller.clientHeight * 2))
  for (const fraction of [0, 0.5, 1]) {
    scroller.scrollTop = (scroller.scrollHeight - scroller.clientHeight) * fraction
    await expectDecisionBelowEvidence(pane, names)
  }
  // Long content wraps or scrolls inside its block, never the drawer sideways.
  await expect(scroller.scrollWidth).toBeLessThanOrEqual(scroller.clientWidth)
}

/** How far evidence runs below the visible part of `scroller`; the bottom fade shows while it is above 0. */
const fadeBelow = (scroller: HTMLElement) =>
  parseFloat(getComputedStyle(scroller).getPropertyValue('--scroll-area-overflow-y-end')) || 0

/** A short phone screen: Chromatic reads `viewports`; the Vitest run reads the `viewport` global. */
const shortScreen = { viewport: { value: 'mobile1', isRotated: false } }

/** The Run's accepted source beside the selected record highlights `line`, and says so. */
async function expectSourceLine(dialog: HTMLElement, line: number) {
  const source = within(dialog).getByRole('region', { name: 'Accepted source' })
  await waitFor(() =>
    expect(
      [...source.querySelectorAll<HTMLElement>('[data-highlighted]')].map(
        (item) => item.dataset.line,
      ),
    ).toEqual([`${line}`]),
  )
  await expect(within(source).getByRole('status')).toHaveTextContent(`Line ${line}`)
}

const openDrawer = async (canvasElement: HTMLElement, name?: string) =>
  within(canvasElement.ownerDocument.body).findByRole('dialog', name ? { name } : undefined)

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
  actorLabel: 'You',
  workspaceLabel: 'Finance',
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
/** The program the Run accepted; its tool call and its emission sit at their own lines. */
const program = [
  "const invoices = await coral.providers.billing.listInvoices({ status: 'overdue' })",
  'const owner = await coral.providers.crm.getOwner({ invoice: invoices.data[0].id })',
  "coral.emit({ invoices: 24, groupedBy: 'owner' })",
  "await coral.providers.mail.send({ to: owner.email, subject: 'Invoice INV-4171 is overdue' })",
  'return { invoices: 24 }',
].join('\n')
const call: ActivityRow & { kind: 'invocation' } = {
  kind: 'invocation',
  name: invocation,
  workspace,
  actor: execution.actor,
  agent: '',
  task,
  run,
  provider: `${workspace}/providers/mail`,
  tool: 'send',
  callPath: 'mail.send',
  state: 'pending',
  outcome: 'unknown',
  startedAt: at,
  endedAt: null,
  durationNanos: '221000000000',
  requiresAction: true,
  dispatchedAt: null,
  waitingNanos: '221000000000',
  sourceLocation: programAt('coral.providers.mail.send', program),
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
  sourceLocation: programAt('coral.emit', program),
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
  intent = 'Find the billing tool',
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
// An approval is always reviewed inside its Run, and any call shows beside the Run's program.
const approvalContext: RunDetail = {
  kind: 'run',
  run: execution,
  submission: execution.submission,
  source: program,
  linkedFunctions: [],
  terminal: { kind: 'unavailable' },
}
const detail: InvocationDetail = {
  kind: 'invocation',
  invocation: call,
  arguments:
    '{"to":"owner@example.com","subject":"Invoice INV-4171 is overdue","body":"Your invoice is ready for review."}',
  result: { kind: 'unknown' },
  requests: [{ kind: 'approval', name: `${workspace}/approvals/mail` }],
  request: null,
  requestUnavailable: false,
  context: approvalContext,
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
        tool('search', 'search', 'succeeded', 'Find billing tools'),
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
          sourceLocation: programAt('coral.providers.billing.listInvoices', program),
          state: 'ended',
          outcome: 'upstream_outcome',
          requiresAction: false,
        },
        call,
        {
          ...call,
          name: `${workspace}/invocations/uncertain`,
          callPath: 'crm.getOwner',
          sourceLocation: programAt('coral.providers.crm.getOwner', program),
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
    const dialog = await within(canvasElement.ownerDocument.body).findByRole('dialog')
    const drawer = within(dialog)
    // The call shows beside its Run's program, at the line that made it.
    await expectSourceLine(dialog, 4)
    await expect(drawer.getByText('Arguments (3)')).toBeVisible()
    await expect(drawer.getByRole('button', { name: 'Close details' })).toBeVisible()
    await expect(drawer.queryByRole('button', { name: 'Approve' })).not.toBeInTheDocument()
    await expect(drawer.queryByText('Copy link')).not.toBeInTheDocument()
    await expectAlignedHeaders(await openDrawer(canvasElement))
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
        feedback: 'I could not find a tool to export invoices.\nSearch returned no matching tools.',
      },
    },
  },
  // On a phone the Task heading wraps; its status and close control still centre on its first line.
  globals: shortScreen,
  play: async ({ canvasElement }) => {
    const dialog = await openDrawer(canvasElement, 'Activity details')
    const heading = within(dialog).getByRole('heading', { level: 2, name: goal.intent })
    const line = parseFloat(getComputedStyle(heading).lineHeight)
    await expect(heading.getBoundingClientRect().height).toBeGreaterThan(line * 2)
    await expectAlignedHeaders(dialog)
    // Nothing to decide leaves no footer bar.
    for (const footer of dialog.querySelectorAll('footer')) await expect(footer).not.toBeVisible()
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
              // A Run's end is no call of the program's.
              sourceLocation: undefined,
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
    await expect(within(table).getByRole('link', { name: 'Find billing tools' })).toBeVisible()
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
              // A list layout only; nothing opens it beside a program.
              sourceLocation: undefined,
            },
            {
              ...call,
              name: `${invocation}-failed`,
              state: 'ended',
              outcome: 'coral_failure',
              requiresAction: false,
              callPath: 'coral.providers.linear.updateIssue',
              // A list layout only; nothing opens it beside a program.
              sourceLocation: undefined,
            },
            {
              ...call,
              name: `${invocation}-uncertain`,
              state: 'ended',
              outcome: 'indeterminate',
              requiresAction: false,
              callPath: 'coral.providers.linear.createIssue',
              // A list layout only; nothing opens it beside a program.
              sourceLocation: undefined,
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
        // A list layout only; nothing opens it beside a program.
        sourceLocation: undefined,
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
// executions, and executions hold 0, 1 or 30 tools, in rotation.
const largeCounts = { executions: [0, 1, 3], tools: [0, 1, 30] } as const
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
  tools: number,
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
        intent: `Send reminder batch ${i + 1} (${tools} tool${tools === 1 ? '' : 's'})`,
        acceptedRun: name,
      }
    : undefined,
})
const largeBranchOf = (rows: ActivityRow[]) => ({ rows, next: '', unavailable: false })
// The whole fixture, built once: every record and every branch it can expand.
const largeRows = new Map<string, ActivityRow>()
const largeBranches: ActivityPage['branches'] = {}
function addLargeRun(row: ActivityRow & { kind: 'run' }, tools: number) {
  largeRows.set(row.name, row)
  largeBranches[row.name] = largeBranchOf(
    Array.from({ length: tools }, (_, k) => ({
      ...call,
      name: `${row.name}/invocations/${k}`,
      run: row.name,
      startedAt: row.startedAt,
      callPath: `billing.listInvoices.page${k + 1}`,
      // A list layout only; nothing opens it beside a program.
      sourceLocation: undefined,
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
      const tools = largeCounts.tools[(i + j) % 3]!
      return [
        lookup(j * 2),
        lookup(j * 2 + 1),
        addLargeRun(
          largeRun(`${workspace}/runs/${id}-r${j}`, parent.name, j, tools, parent.startedAt),
          tools,
        ),
      ]
    }).flat(),
    lookup(98, 'Find the reconciled invoices'),
    lookup(99, 'Check the reconciled totals'),
  ]
  for (const child of children) largeRows.set(child.name, child)
  largeBranches[parent.name] = largeBranchOf(children)
  if (i % 3) return [parent]
  const tools = largeCounts.tools[(i / 3) % 3]!
  const solo = largeRun(`${workspace}/runs/s${i}`, '', i, tools, largeStart(i, 20))
  return [parent, addLargeRun(solo, tools)]
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
      // Once it has slid in, the drawer rests at the list's right, inside the window:
      // against its edge when it floats, at the page's edge when it docks.
      await waitFor(() => {
        expect(dialog.getAnimations()).toHaveLength(0)
        expect(dialog.getBoundingClientRect().right).toBeLessThanOrEqual(window.innerWidth)
        expect(dialog.getBoundingClientRect().right).toBeGreaterThan(
          list.getBoundingClientRect().right,
        )
      })
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
  play: async ({ canvasElement }) => {
    await expectAlignedHeaders(await openDrawer(canvasElement, 'Activity details'))
  },
}
/** An uncaught upstream error shows the response's status and the start of its body. */
export const UpstreamToolError: Story = {
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
          message: 'ToolError: The upstream service returned an error',
          error: {
            name: 'ToolError',
            message: 'The upstream service returned an error',
            frames: [{ source: 'Program', line: 1, column: 14 }],
            toolCode: 'error_response',
            toolTraceId: '',
            toolStatus: 404,
            toolBody: '{"message":"No invoices for this account"',
            toolBodyTruncated: true,
            cause: null,
            errors: [],
            truncated: false,
            causeOmitted: false,
            omittedErrors: 0,
            circular: false,
          },
        },
      },
    },
  },
  play: async ({ canvasElement }) => {
    const drawer = within(await within(canvasElement.ownerDocument.body).findByRole('dialog'))
    await expect(drawer.getByText('Upstream HTTP status: 404')).toBeVisible()
    await expect(
      drawer.getByText(codeContaining('No invoices for this account'), { selector: 'pre' }),
    ).toBeVisible()
    await expect(drawer.getByText('This is only the start of the value.')).toBeVisible()
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
              // A Run's end is no call of the program's.
              sourceLocation: undefined,
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
        // The program that made the emission listed above and returned this value.
        source: program,
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
    const value = drawer.getByRole('region', { name: 'Final value' })
    await expect(within(value).getByRole('group', { name: 'Structure' })).toHaveTextContent(
      '"invoices": 24',
    )
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
          value: '{"status":404,"data":{"message":"Not found"},"metadata":{"kind":"http"}}',
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
          value: '{"status":200,"data":{"id":9007199254740993},"metadata":{"kind":"http"}}',
        },
      },
    },
  },
  play: async ({ canvasElement }) => {
    const response = await within(canvasElement.ownerDocument.body).findByRole('region', {
      name: 'Provider response',
    })
    const value = within(response).getByRole('group', { name: 'Structure' })
    await expect(value).toBeVisible()
    // The integer past 2^53 keeps every digit, under its key.
    await expect(value).toHaveTextContent('"data": { 1 property"id": 9007199254740993')
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
          code: 'tool_disabled',
          message: 'The tool is disabled.',
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
        context: approvalContext,
      },
    },
  },
  play: async ({ canvasElement }) => {
    const dialog = await openDrawer(canvasElement, 'Activity details')
    await expectAlignedHeaders(dialog)
    // The emission names its call and record and shows its exact value; no technical details.
    await expect(
      within(dialog).getByRole('heading', { level: 2, name: 'coral.emit #7' }),
    ).toBeVisible()
    await expect(within(dialog).getByRole('region', { name: 'Emitted value' })).toHaveTextContent(
      '"groupedBy"',
    )
    await expect(dialog).not.toHaveTextContent('Technical details')
    // Beside it, the Run's program, at the line that emitted it.
    await expectSourceLine(dialog, 3)
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
    const owner = toolOwner(target.callPath, [{ id: 'github', callRoot: 'coral.providers.github' }])
    const path = functionTarget(target.callPath)
    if (owner && target.title === target.callPath) target.title = owner.relativePath
    target.currentHref = owner
      ? `/${workspace}/providers/${owner.provider.id}/tools/${owner.relativePath}`
      : `/${workspace}/functions/${path}`
  }
  const name = context.kind === 'describe' ? 'describe' : 'search'
  const row = tool(name, name, 'succeeded', 'Find the tool that merges a pull request')
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
          intent: 'Find the tool that merges a pull request',
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
    await expectAlignedHeaders(await openDrawer(canvasElement))
    const opened = await openDetail(canvasElement)
    // The intent titles the panel; the query stays under its own label, once.
    await expect(
      opened.getByRole('heading', {
        level: 2,
        name: 'Find the tool that merges a pull request',
      }),
    ).toBeVisible()
    await expect(opened.queryByText('Intent', { selector: 'dt' })).toBeNull()
    await expect(opened.getByText('Query').nextElementSibling).toHaveTextContent('“github pulls”')
    // One Results heading counts this page and leads its results; "+" says more remain.
    await expect(opened.getByRole('region', { name: 'Results (5+)' })).toContainElement(
      opened.getByRole('list', { name: 'Results in rank order' }),
    )
    // What the search asked comes first, under Search.
    const asked = opened.getByRole('region', { name: 'Search' })
    await expect(
      within(asked)
        .getAllByRole('term')
        .map((term) => term.textContent),
    ).toEqual(['Query', 'Provider', 'Page'])
    // Every retriever searched, so the page states no coverage and no extra sentence.
    await expect(opened.queryByRole('group', { name: 'Search coverage' })).toBeNull()
    await expect(opened.queryByText(/after this page/)).toBeNull()
    const results = within(opened.getByRole('list', { name: 'Results in rank order' }))
    await expect(results.getAllByRole('listitem')).toHaveLength(5)
    // A hit names no title, so its path under the namespace heads it.
    await expect(results.getByText('pulls.updateBranch')).toBeVisible()
    // A search shows only its results: no raw response and no technical details.
    const dialog = await openDrawer(canvasElement)
    await expect(dialog).not.toHaveTextContent('Raw response')
    await expect(dialog).not.toHaveTextContent('Technical details')
  },
}
/** A page retained before Coral trimmed Search Hits, with titles, full coverage, and paths. */
export const HistoricalSearchUntrimmed: Story = {
  args: discovery(searched, stored.untrimmed),
  play: async ({ canvasElement }) => {
    const opened = await openDetail(canvasElement)
    // Each badge states the count; a "+" says more matched than Coral returned.
    const coverage = within(opened.getByRole('group', { name: 'Search coverage' }))
    await expect(coverage.getByText('100+ found')).toBeVisible()
    await expect(coverage.getByText('none match')).toBeVisible()
    await expect(coverage.queryByText(/searched|more available/)).toBeNull()
    // The heading counts this page's results, not the kind's returned count.
    await expect(opened.getByRole('heading', { name: 'Results (5+)' })).toBeVisible()
    await expect(opened.queryByText(/after this page/)).toBeNull()
    const results = within(opened.getByRole('list', { name: 'Results in rank order' }))
    await expect(results.getByText('Update a pull request branch')).toBeVisible()
    // A retained page lists its matched declaration paths as a count on the compact row.
    await expect(results.getByText('1 declaration path')).toBeVisible()
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
/** Both kinds of result, with flags and a guidance match. */
export const HistoricalSearchMixedResults: Story = {
  args: discovery({ ...searched, query: 'github issues by label' }, stored.mixed),
  play: async ({ canvasElement, userEvent }) => {
    const opened = await openDetail(canvasElement)
    const results = opened.getByRole('list', { name: 'Results in rank order' })
    const [savedFunction, merge] = within(results).getAllByRole('listitem')
    await expect(within(savedFunction).getByText('saved function')).toBeVisible()
    // An icon beside the call path opens today's function, named with its path.
    const current = within(savedFunction).getByRole('link', {
      name: /^Open current function coral\.functions\./,
    })
    await expect(current).toBeVisible()
    await expect(current.getAttribute('href')).toMatch(/\/functions\//)
    // Approval shows as the Providers page's named hand; other flags keep their text.
    await expect(within(merge).getByRole('img', { name: /^Needs approval/ })).toBeVisible()
    for (const flag of ['destructive', 'not idempotent', 'tag: pulls'])
      await expect(within(merge).getByText(flag)).toBeVisible()
    await expect(within(results).queryByText(/Matched declaration paths/)).toBeNull()
    // Each card folds its own retained guidance, description and signature into Details.
    const detailsIn = (row: HTMLElement) =>
      within(row).getByText('Details', { selector: 'summary' }).closest('details')!
    const [functionDetails, mergeDetails] = [detailsIn(savedFunction), detailsIn(merge)]
    const signature = within(mergeDetails).getByText(codeContaining('merge(input: { body?'), {
      selector: 'pre',
    })
    const guidance = within(mergeDetails).getByText('API method documentation')
    await expect(signature).not.toBeVisible()
    await expect(guidance).not.toBeVisible()
    await userEvent.click(within(merge).getByText('Details'))
    await waitFor(() => expect(mergeDetails).toHaveAttribute('open'))
    await expect(signature).toBeVisible()
    await expect(guidance).toBeVisible()
    await expect(within(mergeDetails).getByText(/^> \[!NOTE\]/)).toBeVisible()
    // Opening one card leaves the others closed.
    await expect(functionDetails).not.toHaveAttribute('open')
    await expect(
      within(functionDetails).getByText(/^Lists the open issues of one repository/),
    ).not.toBeVisible()
    await userEvent.click(within(savedFunction).getByText('Details'))
    await waitFor(() => expect(functionDetails).toHaveAttribute('open'))
    await expect(mergeDetails).toHaveAttribute('open')
    await userEvent.click(within(merge).getByText('Details'))
    await waitFor(() => expect(mergeDetails).not.toHaveAttribute('open'))
    await expect(functionDetails).toHaveAttribute('open')
    await expect(signature).not.toBeVisible()
    // The link and the approval hand stay in the summary, outside Details.
    await expect(mergeDetails).not.toContainElement(
      within(merge).getByRole('img', { name: /^Needs approval/ }),
    )
    await expect(functionDetails).not.toContainElement(current)
    // Reef says what Coral stored in its own words. Only the raw response keeps Coral's.
    await expect(opened.getByRole('region', { name: 'Results (6+)' })).not.toHaveTextContent(
      /retriever|saved_function|results_found/,
    )
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
/** The declaration is longer than the cap, so it scrolls inside its block once Details opens. */
export const HistoricalDescribeTool: Story = {
  args: discovery(
    { kind: 'describe', callPath: 'coral.providers.github.issues.create', types: [] },
    stored.describeTool,
  ),
  play: async ({ canvasElement, userEvent }) => {
    const opened = await openDetail(canvasElement)
    // A compact Result names the target; its documentation waits in a closed Details.
    const result = within(opened.getByRole('region', { name: 'Result' }))
    await expect(result.getByText('Create an issue')).toBeVisible()
    const summary = opened.getByText('Details', { selector: 'summary' })
    const details = summary.closest('details')!
    await expect(details).not.toHaveAttribute('open')
    await expect(within(details).getByRole('heading', { name: 'Declaration' })).not.toBeVisible()
    await userEvent.click(summary)
    await waitFor(() => expect(details).toHaveAttribute('open'))
    const [declaration] = within(details).getAllByText(codeContaining('namespace issues'), {
      selector: 'pre',
    })
    await expect(declaration).toBeVisible()
    // The block shows 20 of the 24 lines; the other lines scroll.
    await expect(declaration.scrollHeight - declaration.clientHeight).toBeGreaterThan(60)
    // The native summary takes keyboard focus; closing folds the documentation away again.
    summary.focus()
    await expect(summary).toHaveFocus()
    await userEvent.click(summary)
    await waitFor(() => expect(details).not.toHaveAttribute('open'))
    await expect(declaration).not.toBeVisible()
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
  play: async ({ canvasElement, userEvent }) => {
    const opened = await openDetail(canvasElement)
    // The request states the Call Path, under its label, as the search states its query.
    await expect(opened.getByText('Call path').nextElementSibling).toHaveTextContent(
      'coral.functions.github.openIssuesByLabel',
    )
    // The types the agent asked for, when it named any.
    await expect(opened.getByText('Types').nextElementSibling).toHaveTextContent('IssueLabel')
    // The Result counts the diagnostics; Details holds the source and each diagnostic.
    await expect(
      within(opened.getByRole('region', { name: 'Result' })).getByText('1 error'),
    ).toBeVisible()
    await userEvent.click(opened.getByText('Details', { selector: 'summary' }))
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
/** A call that waits for your approval, with `args` as its arguments. */
const pendingApproval = (args: unknown): ActivityPage => ({
  ...page,
  detail: {
    ...detail,
    context: approvalContext,
    request: {
      kind: 'approval',
      taskIntent: goal.intent,
      actionable: true,
      name: `${workspace}/approvals/mail`,
      run,
      invocation,
      provider: call.provider,
      providerLabel: 'Mail',
      callPath: call.callPath,
      arguments: JSON.stringify(args),
      state: 'pending',
      // A pending approval is still open, so its deadline is ahead of whoever views it.
      deadline: new Date(Date.now() + 15 * 60_000).toISOString(),
      approver: '',
      decidedAt: null,
      comment: null,
    },
  },
})
export const PendingApproval: Story = {
  beforeEach: freezeStoryClock,
  args: {
    page: pendingApproval({
      recipient: 'owner@example.com',
      lines: Array.from({ length: 80 }, (_, i) => `Invoice ${i + 1}`),
    }),
  },
  globals: shortScreen,
  play: async ({ canvasElement, userEvent }) => {
    const dialog = await openDrawer(canvasElement, 'Review action')
    await expectAlignedHeaders(dialog)
    // The review decides this one call: no technical details, program body, history or Run
    // cancel. When the call started closes the list, after who requested it.
    for (const gone of ['Technical details', 'Program body', 'View in history', 'Cancel run'])
      await expect(within(dialog).queryByText(gone)).toBeNull()
    const list = within(dialog).getByText('Requested by', { selector: 'dt' }).closest('dl')!
    await expect(
      within(list)
        .getAllByRole('term')
        .map((term) => term.textContent),
    ).toEqual(['Task intent', 'Run intent', 'Requested by', 'Call started'])
    // The comment and the decision stay.
    await userEvent.click(within(dialog).getByText('Add a comment', { selector: 'summary' }))
    await expect(within(dialog).getByLabelText('Comment (optional)')).toBeVisible()
    // The Run's source shares the drawer, so the decision pins to the selected record's pane.
    const selected = within(dialog).getByRole('region', { name: 'Selected record' })
    await expectPinnedFooter(selected, ['Deny this call', 'Approve this call'])
    // Above the decision, a fade marks the evidence still below, until the reader reaches it.
    const scroller = scrollerIn(selected)
    scroller.scrollTop = 0
    await waitFor(() => expect(fadeBelow(scroller)).toBeGreaterThan(0))
    scroller.scrollTop = scroller.scrollHeight
    await waitFor(() => expect(fadeBelow(scroller)).toBe(0))
    await expect(within(dialog).getByRole('region', { name: 'Accepted source' })).toBeVisible()
    // The review's own header holds the drawer's one close control; no Run header sits above it.
    const close = within(dialog).getByRole('button', { name: 'Close details' })
    await expect(within(selected).getAllByRole('button', { name: 'Close details' })).toEqual([
      close,
    ])
    const header = within(selected).getByRole('heading', { name: 'mail.send' }).closest('header')
    await expect(header).toContainElement(close)
  },
}
/** Evidence that fits its pane leaves no fade, and the decision shows whole. */
export const ShortPendingApproval: Story = {
  args: { page: pendingApproval('owner@example.com') },
  play: async ({ canvasElement }) => {
    // Storybook's own UI cannot resize its viewport, so this play needs the test runner.
    if (!(await resizeViewport(1440, 1800))) return
    const dialog = await openDrawer(canvasElement, 'Review action')
    const selected = within(dialog).getByRole('region', { name: 'Selected record' })
    const scroller = scrollerIn(selected)
    await waitFor(() => expect(scroller.scrollHeight).toBeLessThanOrEqual(scroller.clientHeight))
    await waitFor(() => expect(fadeBelow(scroller)).toBe(0))
    await expectDecisionBelowEvidence(selected, ['Deny this call', 'Approve this call'])
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
          taskIntent: goal.intent,
          name: `${workspace}/approvals/mail`,
          run,
          invocation,
          provider: call.provider,
          providerLabel: 'Mail',
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
const connection = `${workspace}/accountConnections/linear`
/** A Run that waits on your shared account connection, not on one call's approval. */
export const PendingConnectionFooter: Story = {
  beforeEach: freezeStoryClock,
  globals: shortScreen,
  parameters: { chromatic: { viewports: [390] } },
  args: {
    page: {
      ...page,
      selected: run,
      selectedRow: execution,
      detail: {
        kind: 'run',
        run: {
          ...execution,
          actionPage: {
            total: '1',
            next: '',
            actions: [
              {
                kind: 'account_connection',
                name: connection,
                callPath: '',
                providerLabel: 'Linear',
                deadline: minutesAfterStoryNow(15),
              },
            ],
          },
        },
        submission: execution.submission,
        request: {
          kind: 'account_connection',
          name: connection,
          provider: `${workspace}/providers/linear`,
          state: 'pending',
          expiresAt: minutesAfterStoryNow(15),
        },
        // The program whose call waits on this connection.
        source: "return await coral.providers.linear.listIssues({ assignee: 'me' })",
        linkedFunctions: [],
        terminal: { kind: 'unavailable' },
      },
    },
  },
  play: async ({ canvasElement }) => {
    await expect(canvasElement.ownerDocument.defaultView!.innerHeight).toBeLessThanOrEqual(568)
    const dialog = await openDrawer(canvasElement, 'Review action')
    // The Run's header and the shared connection's own header align alike.
    await expectAlignedHeaders(dialog)
    // A Run panel keeps no long retained source: the decision shows whole in its footer.
    const edge = dialog.getBoundingClientRect()
    for (const name of ['Decline', 'Connect Linear']) {
      const control = within(dialog).getByRole('button', { name })
      await expect(control.closest('footer')).not.toBeNull()
      const box = control.getBoundingClientRect()
      await expect(box.top >= edge.top && box.bottom <= edge.bottom).toBe(true)
    }
  },
}
/** A call selected on its own, beside the source of the Run that made it. */
export const InvocationWithRunEvidence: Story = {
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
        result: { kind: 'upstream_outcome', value: '{"metadata":{"status":202}}' },
        context: {
          ...approvalContext,
          run: { ...execution, state: 'failed', endedAt: at, requiresAction: false },
          linkedFunctions: [
            {
              savedFunction: 'old-function-id',
              path: 'billing.remind',
              source: 'export default () => "snapshot"',
            },
          ],
          terminal: {
            kind: 'failed',
            code: 'program_execution_failed',
            message: 'The program failed after this call returned.',
            error: null,
          },
        },
      },
    },
  },
  play: async ({ canvasElement }) => {
    const drawer = within(
      await within(canvasElement.ownerDocument.body).findByRole('dialog', {
        name: 'Activity details',
      }),
    )
    await expect(drawer.getByRole('heading', { level: 2, name: 'mail.send' })).toBeVisible()
    await expectAlignedHeaders(await openDrawer(canvasElement, 'Activity details'))
    await expect(drawer.getByRole('region', { name: 'Provider response' })).toBeVisible()
    // The call leads; the Run's source shows in its own pane below.
    const source = drawer.getByRole('region', { name: 'Accepted source' })
    await expect(source.querySelector('[data-line="1"]')).toBeVisible()
  },
}
