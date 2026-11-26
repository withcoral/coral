/** @vitest-environment happy-dom */
import { act, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, it, onTestFinished, vi } from 'vitest'
import { markupRoute, renderLivePage, renderRoute } from '@/test-support/route'
import { Activity, duration } from './activity'
import { ActivityArchivedContext } from './live'
import { ProviderMarksContext } from './call-tool'
import { rowLabel } from './record'
import { ActionReviewSession } from '../actions/review'
import { activityPage, approvalRequest, run as waiting, taskRow, workspace } from './fixtures'
import { absorbPage, openTrace, receiveTrace, type ActivityTraceStore } from '@/lib/activity-trace'
import type { ActivityPage, ActivityRow, ActivityViewProps } from './types'

const startedAt = '2026-09-18T14:02:00.000Z'
const goal: ActivityRow & { kind: 'task' } = {
  kind: 'task',
  name: `${workspace}/tasks/invoices`,
  workspace,
  agent: '',
  actor: 'you',
  intent: 'Find invoices',
  outcome: 'success',
  startedAt,
  endedAt: startedAt,
}
const run: ActivityRow & { kind: 'run' } = {
  kind: 'run',
  name: `${workspace}/runs/one`,
  workspace,
  agent: '',
  actor: 'you',
  task: goal.name,
  state: 'succeeded',
  startedAt,
  endedAt: startedAt,
  durationNanos: '1000000000',
  requiresAction: false,
}
const invocation: ActivityRow & { kind: 'invocation' } = {
  ...run,
  kind: 'invocation',
  name: `${workspace}/invocations/one`,
  run: run.name,
  state: 'ended',
  outcome: 'upstream_outcome',
  dispatchedAt: startedAt,
  waitingNanos: '0',
  toolKind: 'provider',
  provider: `${workspace}/providers/linear`,
  tool: 'listIssues',
  callPath: 'coral.providers.linear.listIssues',
}
const tool: ActivityRow & { kind: 'tool' } = {
  kind: 'tool',
  name: `${goal.name}/interactions/search`,
  workspace,
  agent: '',
  task: goal.name,
  tool: 'search',
  intent: 'Find tools',
  outcome: 'succeeded',
  startedAt,
  completedAt: startedAt,
  acceptedRun: '',
}
function render(row: ActivityRow) {
  return markupRoute(<Activity page={activityPage({ rows: [row] })} />, '/activity')
}

// The label is the exact state, where the tone alone would mislead. The
// StatusMarkers story shows the tones.
it.each<[string, ActivityRow, string]>([
  ['timed out execution', { ...run, state: 'timed_out' }, 'timed out'],
  [
    'execution that contains an action but is not an approval target',
    { ...run, state: 'running', requiresAction: true },
    'running',
  ],
  ['unknown attempt', { ...tool, outcome: 'unknown' }, 'result unknown'],
  [
    'accepted but unavailable execution',
    { ...tool, tool: 'exec', acceptedRun: run.name },
    'accepted · run unavailable',
  ],
  [
    'tool call that needs action',
    {
      ...invocation,
      state: 'pending',
      outcome: 'unknown',
      requiresAction: true,
    },
    'pending',
  ],
  ['a response received, not inferred success', invocation, 'completed'],
  ['uncertain dispatch', { ...invocation, outcome: 'indeterminate' }, 'result unknown'],
  ['a Coral failure', { ...invocation, outcome: 'coral_failure' }, 'failed'],
  [
    'Run termination, not an upstream error',
    { ...invocation, outcome: 'run_terminated' },
    'run ended',
  ],
])('marks %s with its exact accessible state', (_name, row, label) => {
  const markup = render(row)
  expect(markup).toContain(`aria-description="${label}"`)
  expect(markup).toContain(`title="${label}"`)
  expect(markup).not.toContain('>STATE<')
})

// Catches: a log or emission row marked "diagnostic" or "retained", which says nothing about it.
it.each(['program.logged', 'program.emitted', 'run.started'])(
  'marks no state on a %s output row',
  (eventKind) => {
    const markup = render({
      kind: 'output',
      name: `${invocation.run}/events/1`,
      workspace: invocation.workspace,
      run: invocation.run,
      label: 'output #1',
      startedAt: invocation.startedAt,
      eventKind,
      agent: '',
    })
    expect(markup).not.toContain('aria-description=')
    expect(markup).not.toMatch(/retained|diagnostic/)
  },
)

// Catches: a call that names its tool by the full Call Path while the workspace still has its
// provider, or a call whose provider is gone that loses its kind and its path.
it.each([
  [
    'its provider',
    [{ callRoot: 'coral.providers.linear', displayName: 'Linear' }],
    'Linear listIssues',
    false,
  ],
  [
    'no provider that owns it',
    [{ callRoot: 'coral.providers.mail', displayName: 'Mail' }],
    'coral.providers.linear.listIssues',
    true,
  ],
])('names a call by the logo and tool of %s', (_, providers, name, kindShown) => {
  renderRoute(
    <ProviderMarksContext value={providers}>
      <Activity page={activityPage({ rows: [invocation] })} />
    </ProviderMarksContext>,
    { entry: '/activity' },
  )
  expect(screen.getByRole('link', { name })).toBeInTheDocument()
  expect(screen.getByTitle(invocation.callPath)).toBeInTheDocument()
  expect(screen.queryAllByRole('img', { name: 'tool' })).toHaveLength(kindShown ? 1 : 0)
})

/** A page with the waterfall of `root` open, holding `children` of each record. */
const waterfallPage = (
  root: ActivityRow,
  children: Record<string, ActivityRow[]>,
  overrides: Partial<ActivityPage> = {},
) =>
  activityPage({
    rows: [root],
    expanded: Object.keys(children),
    branches: Object.fromEntries(
      Object.entries(children).map(([parent, rows]) => [
        parent,
        { rows, next: '', unavailable: false },
      ]),
    ),
    ...overrides,
  })
const records = () => screen.getByRole('table', { name: 'Activity records' })

it('marks only the tool call as needing action, never its containing Run', () => {
  const asking = { ...run, task: '', state: 'running' as const, requiresAction: true }
  expect(render(asking)).not.toContain('needs action')
  const markup = markupRoute(
    <Activity
      page={waterfallPage(asking, {
        [asking.name]: [{ ...invocation, task: '', state: 'pending', requiresAction: true }],
      })}
    />,
    '/activity',
  )
  expect(markup.split('needs action')).toHaveLength(2)
})

it('retains the selected review and draft across a live refresh, but conceals access loss', async () => {
  const user = userEvent.setup()
  const initial = activityPage({
    rows: [waiting],
    selected: waiting.name,
    selectedRow: waiting,
    detail: {
      kind: 'run',
      run: waiting,
      request: approvalRequest,
      source: 'return null;',
      linkedSharedTools: [],
      linkedPrivateTools: [],
      terminal: { kind: 'unavailable' },
    },
  })
  const { update: publish } = renderLivePage(
    (page) => (
      <ActionReviewSession>
        <Activity page={page} />
      </ActionReviewSession>
    ),
    initial,
    { entry: '/activity?record=one&request=one' },
  )
  expect(document.body).toHaveTextContent('exact-recipient')
  const comment = screen.getByRole('textbox', { name: 'Comment (optional)' })
  await user.type(comment, 'Keep this draft')

  // A newer record arrives above the waiting Run.
  await publish({ ...initial, rows: [taskRow('newer', 'A newer goal'), waiting] })
  expect(document.body).toHaveTextContent('A newer goal')
  expect(document.body).toHaveTextContent('exact-recipient')
  expect(screen.getByRole('textbox', { name: 'Comment (optional)' })).toBe(comment)
  expect(comment).toHaveValue('Keep this draft')

  await publish({
    ...initial,
    detail: null,
    selectedRow: null,
    unavailable: true,
  })
  expect(document.body).not.toHaveTextContent('exact-recipient')
  expect(screen.queryByRole('button', { name: 'Approve' })).not.toBeInTheDocument()
})

// Catches: a Workspace without an advertised endpoint showing a button that
// copies nothing, or agent history telling an Agent to connect as a person.
const PERSON = 'Connect an MCP client to this workspace. Its calls show here.'
it.each<[string, ActivityViewProps['connect'], boolean, string]>([
  ['workspace activity', { endpoint: 'https://coral.example.com/mcp' }, true, PERSON],
  ['workspace without an endpoint', { endpoint: '' }, false, PERSON],
  ['agent history', undefined, false, "The agent's calls show here."],
  // The Overview's own setup card shows how to connect.
  ['overview activity', undefined, false, 'Your calls show here.'],
])('empty %s says what fills it', (kind, connect, offered, description) => {
  const page = activityPage({
    agent: kind === 'agent history' ? `${workspace}/agents/a` : null,
  })
  renderRoute(<Activity page={page} connect={connect} />, {
    entry: '/activity',
  })
  expect(screen.getByText('No activity yet')).toBeInTheDocument()
  expect(screen.getByText(description)).toBeInTheDocument()
  expect(screen.queryAllByRole('button', { name: 'Copy MCP URL' })).toHaveLength(offered ? 1 : 0)
})

// Catches: an archived workspace offering an endpoint that takes no calls, or
// telling an owner to ask an owner.
it.each<['owner' | 'member', string]>([
  ['owner', 'Restore this workspace to start new runs.'],
  ['member', 'Ask a workspace owner to restore this workspace. Then start new runs.'],
])('empty archived activity tells the %s how to start new runs', (role, description) => {
  renderRoute(
    <ActivityArchivedContext value={role}>
      <Activity page={activityPage()} />
    </ActivityArchivedContext>,
    { entry: '/activity' },
  )
  expect(screen.getByText('No activity')).toBeInTheDocument()
  expect(screen.getByText(description)).toBeInTheDocument()
  expect(screen.queryByRole('button', { name: 'Copy MCP URL' })).toBeNull()
})

// The Overview shows a glimpse: one link to the full list replaces paging.
it.each<[string, ActivityViewProps['more'], boolean, string[]]>([
  [
    'a glimpse with older rows',
    { to: '/w/activity', label: 'View all activity', roots: 5 },
    true,
    ['View all activity'],
  ],
  [
    'a glimpse with no older rows',
    { to: '/w/activity', label: 'View all activity', roots: 5 },
    false,
    [],
  ],
  ['the full list', undefined, true, []],
])('%s shows only its own paging links', (_, more, older, links) => {
  renderRoute(
    <Activity page={activityPage({ rows: [run], hasBefore: true, hasAfter: older })} more={more} />,
    { entry: '/activity' },
  )
  expect(
    screen.queryAllByRole('link', { name: /page|activity/i }).map((link) => link.textContent),
  ).toEqual(links)
})

// Catches: a glimpse growing past its few roots when one of them is expanded.
it.each<[string, ActivityViewProps['more'], string[]]>([
  ['a glimpse', { to: '/w/activity', label: 'View all activity', roots: 5 }, ['Find invoices']],
  // The list names the root; its waterfall lists the child, under the Task's header.
  ['the full list', undefined, ['Find invoices', 'Find tools']],
])('%s lists child records only when it pages', (_, more, shownRecords) => {
  renderRoute(
    <Activity
      page={activityPage({
        rows: [goal],
        expanded: [goal.name],
        branches: {
          [goal.name]: { rows: [tool], next: '', unavailable: false },
        },
      })}
      more={more}
    />,
    { entry: '/activity' },
  )
  const shown = screen.getAllByRole('link').map((link) => link.textContent)
  expect(shown.filter((text) => text !== 'View all activity')).toEqual(shownRecords)
  // A record with nothing under it offers no toggle.
  expect(screen.queryAllByRole('button', { name: /^(Collapse|Expand) record$/ })).toHaveLength(0)
  if (!more)
    expect(
      within(screen.getByRole('region', { name: 'Task' })).getByRole('heading', {
        name: goal.intent,
      }),
    ).toBeInTheDocument()
})

// Catches: a glimpse loading older records, or keeping newer ones a live update merged in.
it('a glimpse lists only its latest roots and never loads more', () => {
  const control = {
    load: vi.fn(),
    loading: false,
    failed: false,
    automatic: false,
  }
  renderRoute(
    <Activity
      page={activityPage({ rows: [goal, run], hasAfter: false })}
      list={{ count: 2, newer: control, older: control }}
      more={{ to: '/w/activity', label: 'View all activity', roots: 1 }}
    />,
    { entry: '/activity' },
  )
  expect(screen.getAllByRole('link').map((link) => link.textContent)).toEqual([
    'Find invoices',
    'View all activity',
  ])
  expect(screen.queryByRole('button', { name: /^Load (newer|older) records$/ })).toBeNull()
  expect(screen.queryByText(/^The list shows/)).toBeNull()
  expect(control.load).not.toHaveBeenCalled()
})

// Catches: a glimpse record opening on the Overview instead of the Activity page, or a
// record of the full list opening its detail without its waterfall.
it.each<[string, ActivityViewProps['more'], string, 'record' | 'expand']>([
  [
    'a glimpse',
    { to: `/${workspace}/activity`, label: 'View all activity', roots: 5 },
    `/${workspace}/activity`,
    'record',
  ],
  ['the full list', undefined, `/${workspace}`, 'expand'],
])('%s opens a clicked record where it lists in full', async (_, more, pathname, key) => {
  const user = userEvent.setup()
  // The Overview lists its glimpse at the Workspace's own path.
  const { router } = renderRoute(
    <Activity page={activityPage({ rows: [goal], listPath: `/${workspace}` })} more={more} />,
    { entry: `/${workspace}` },
  )
  const link = screen.getByRole('link', { name: 'Find invoices' })
  expect(new URL(link.getAttribute('href')!, 'http://reef').pathname).toBe(pathname)
  await user.click(link)
  expect(router.state.location.pathname).toBe(pathname)
  const query = new URLSearchParams(router.state.location.search)
  expect(query.get(key)).toBe(goal.name)
})

// Catches: a Task picked in the list opening its waterfall without its details, or a
// standalone Run's waterfall opening with a selection it never asked for.
it.each([
  ['a Task opens its waterfall and its details', goal, goal.name],
  ['a standalone Run opens its waterfall alone', { ...run, task: '' }, null],
])('in the full list, %s', async (_, row, record) => {
  const user = userEvent.setup()
  const { router } = renderRoute(<Activity page={activityPage({ rows: [row] })} />)
  await user.click(screen.getByRole('link', { name: rowLabel(row) }))
  const query = new URLSearchParams(router.state.location.search)
  expect(query.getAll('expand')).toEqual([row.name])
  expect(query.get('record')).toBe(record)
})

// Catches: a root with no children showing rows that are not its own, or a Task's own row
// in its table, which its header above already states.
it.each([
  ['a Task', goal, 0],
  ['a standalone Run', { ...run, task: '' }, 1],
] as const)('shows %s with no children as its header or its row alone', (_, root, own) => {
  renderRoute(<Activity page={waterfallPage(root, { [root.name]: [] })} />, {
    entry: '/activity',
  })
  const rows = within(records()).getAllByRole('row')
  // The column heading, then the root's own row, if it has one.
  expect(rows).toHaveLength(1 + own)
  expect(screen.queryAllByRole('region', { name: 'Task' })).toHaveLength(1 - own)
  if (own) expect(within(rows[1]!).getByRole('link', { name: rowLabel(root) })).toBeInTheDocument()
})

it('says that the history of an open record is unavailable, under that record', () => {
  renderRoute(
    <Activity
      page={activityPage({
        rows: [goal],
        expanded: [goal.name],
        branches: { [goal.name]: { rows: [], next: '', unavailable: true } },
      })}
    />,
    { entry: '/activity' },
  )
  // The Task's header names it; under the column heading, its history says it is unavailable.
  expect(
    within(screen.getByRole('region', { name: 'Task' })).getByRole('heading', {
      name: goal.intent,
    }),
  ).toBeInTheDocument()
  const rows = within(records()).getAllByRole('row')
  expect(rows).toHaveLength(2)
  expect(rows[1]).toHaveTextContent('This history is unavailable.')
  // No summary states a value from a history it cannot read.
  expect(screen.getByText('Time is unavailable. Some records are unavailable.')).toBeVisible()
})

const dayRecords = (label: string) =>
  within(screen.getByRole('rowgroup', { name: label }))
    .getAllByRole('link')
    .map((link) => link.textContent)

// The clock never decides a heading: a record started "today" still shows its date.
// The midnight split itself is checked in the browser (DayBoundary story).
it('labels a day by its UTC date, even today, and shows the full instant on hover', () => {
  vi.useFakeTimers({ toFake: ['Date'] })
  onTestFinished(() => {
    vi.useRealTimers()
  })
  vi.setSystemTime('2026-09-30T00:30:00.000Z')
  renderRoute(
    <Activity
      page={activityPage({
        rows: [{ ...goal, startedAt: '2026-09-30T00:04:00.000Z' }],
      })}
    />,
    { entry: '/activity' },
  )
  expect(dayRecords('Wed 30 Sep 2026')).toEqual([goal.intent])
  expect(screen.getByText('00:04:00')).toHaveAttribute('title', '2026-09-30 00:04:00 UTC')
})

it('names the day of an open record that sits outside every day group', () => {
  renderRoute(<Activity page={activityPage({ rows: [], selectedRow: goal })} />, {
    entry: '/activity',
  })
  const row = within(screen.getByRole('table', { name: 'Tasks and runs' }))
    .getAllByRole('row')
    .find((candidate) => within(candidate).queryByText('Outside this workspace or page:'))!
  expect(within(row).getByText('Fri 18 Sep 2026')).toBeInTheDocument()
})

// Catches: a "Reveal record" link below the detail of a record the list does not show. Its
// row above the table already leads to it.
it('ends the detail of a record outside the list without a link back to it', () => {
  renderRoute(
    <Activity
      page={activityPage({
        rows: [],
        selected: goal.name,
        selectedRow: goal,
        detail: { kind: 'task', task: goal },
      })}
    />,
    { entry: '/activity?record=x' },
  )
  // A Task states its details in the header above its waterfall, never in a rail.
  expect(screen.queryByRole('dialog', { name: 'Activity details' })).toBeNull()
  const header = screen.getByRole('region', { name: 'Task' })
  expect(within(header).getByRole('heading', { level: 2, name: goal.intent })).toBeInTheDocument()
  expect(screen.queryByRole('link', { name: 'Reveal record' })).toBeNull()
})

const recordNames = () =>
  within(records())
    .getAllByRole('row')
    .slice(1)
    .map((row) => row.querySelector('[data-record-link]')?.textContent)

// Catches: a Run's calls listed beside it instead of under it, or a Run's Events as rows.
it('lists a Run under its Task and its calls under the Run, with Events as marks', () => {
  const log: ActivityRow = {
    kind: 'output',
    name: `${run.name}/events/2`,
    workspace,
    agent: '',
    task: goal.name,
    run: run.name,
    label: 'log #2',
    startedAt,
    eventKind: 'program.logged',
  }
  renderRoute(
    <Activity page={waterfallPage(goal, { [goal.name]: [run], [run.name]: [invocation, log] })} />,
    { entry: '/activity' },
  )
  // The Task's header names the Task; its table starts at the Run.
  expect(recordNames()).toEqual(['you', invocation.callPath])
  // A screen reader hears the same nesting the indent shows.
  expect(
    within(records())
      .getAllByRole('row')
      .slice(1)
      .map((row) => row.getAttribute('aria-level')),
  ).toEqual(['1', '2'])
  const mark = within(records()).getByRole('link', { name: 'log #2' })
  // The mark sits on its Run's row and links to the Event.
  expect(mark.closest('[role=row]')).toContainElement(
    screen.getAllByRole('link', { name: 'you' })[0]!,
  )
  expect(new URLSearchParams(mark.getAttribute('href')!.split('?')[1]).get('record')).toBe(log.name)
})

// Catches: marks that only a pointer reaches, or ← and → acting on a row with no marks.
it('← and → select the marks of the cursor Run from the keyboard', async () => {
  const user = userEvent.setup()
  const mark = (sequence: number): ActivityRow => ({
    kind: 'output',
    name: `${run.name}/events/${sequence}`,
    workspace,
    agent: '',
    task: goal.name,
    run: run.name,
    label: `log #${sequence}`,
    startedAt,
    eventKind: 'program.logged',
  })
  const { router } = renderRoute(
    <Activity
      page={waterfallPage(goal, {
        [goal.name]: [run],
        [run.name]: [mark(1), invocation, mark(3)],
      })}
    />,
    { entry: '/activity' },
  )
  const selected = () => new URLSearchParams(router.state.location.search).get('record')
  act(() => records().focus())
  // The first record under the Task's header is its Run.
  await user.keyboard('{ArrowDown}{ArrowRight}')
  expect(selected()).toBe(mark(1).name)
  await user.keyboard('{ArrowLeft}')
  expect(selected()).toBe(mark(3).name)
  // The keys stay with the table.
  expect(records()).toHaveFocus()
})

// Catches: a click on a mark opening its Run, or a hidden mark still taking clicks.
it('opens a mark you click, and hides every mark when you turn marks off', async () => {
  const user = userEvent.setup()
  const log: ActivityRow = {
    kind: 'output',
    name: `${run.name}/events/2`,
    workspace,
    agent: '',
    task: goal.name,
    run: run.name,
    label: 'log #2',
    startedAt,
    eventKind: 'program.logged',
  }
  const { router } = renderRoute(
    <Activity page={waterfallPage(goal, { [goal.name]: [run], [run.name]: [log] })} />,
    { entry: '/activity' },
  )
  await user.click(within(records()).getByRole('link', { name: 'log #2' }))
  expect(new URLSearchParams(router.state.location.search).get('record')).toBe(log.name)
  const toggle = screen.getByRole('button', { name: 'Marks' })
  expect(toggle).toHaveAttribute('aria-pressed', 'true')
  // Its icon has one shape for on and another for off: the pressed fill is not the only cue.
  const shape = () => toggle.querySelector('svg')!.innerHTML
  const on = shape()
  await user.click(toggle)
  expect(toggle).toHaveAttribute('aria-pressed', 'false')
  expect(toggle).toHaveAccessibleName('Marks')
  expect(shape()).not.toBe(on)
  expect(within(records()).queryByRole('link', { name: 'log #2' })).toBeNull()
  await user.click(toggle)
  expect(toggle).toHaveAttribute('aria-pressed', 'true')
  expect(shape()).toBe(on)
  expect(within(records()).getByRole('link', { name: 'log #2' })).toBeVisible()
})

const foreignCall = { ...invocation, task: `${workspace}/tasks/of-an-agent` }
const foreignPage = activityPage({
  rows: [goal],
  selected: foreignCall.name,
  selectedRow: foreignCall,
  detail: {
    kind: 'invocation',
    invocation: foreignCall,
    arguments: '{}',
    result: { kind: 'unknown' },
    requests: [],
    request: null,
    requestUnavailable: false,
  },
})
// Catches: an Agent's call you review showing no row at all, because the list's scope does
// not return its Task, or the waterfall claiming a history it cannot read.
it('shows a selected record alone when Coral returns no top-level record for it', () => {
  const foreign = foreignCall
  renderRoute(<Activity page={foreignPage} />, { entry: '/activity?record=x' })
  const rows = within(records()).getAllByRole('row').slice(1)
  expect(rows[0]).toHaveAttribute('aria-selected', 'true')
  expect(rows[0]).toHaveAttribute('data-record-key', foreign.name)
  // Only the record and the line that says its history is unavailable.
  expect(rows).toHaveLength(1)
  expect(screen.getByText('Time is unavailable. Some records are unavailable.')).toBeVisible()
  // The list offers no row that would open a call as if it were a Task or a Run.
  expect(screen.queryByText('Outside this workspace or page:')).toBeNull()
})

const reader = (store: ActivityTraceStore) => ({ store, retry: vi.fn(), prefer: vi.fn() })

// Catches: "unavailable" said of a history that only has not loaded yet, such as a record
// whose Task is not on the list's page, or a value from a read that failed.
it('says a history loads until Coral answers, and that it did not load when a read fails', async () => {
  const looking = openTrace(foreignCall.task)
  const view = renderLivePage(
    (trace: ReturnType<typeof reader>) => <Activity page={foreignPage} trace={trace} />,
    reader(looking),
    { entry: '/activity?record=x' },
  )
  // The selected record shows meanwhile, alone.
  expect(within(records()).getAllByRole('row').slice(1)).toHaveLength(1)
  expect(screen.getAllByText(/^Loading records…/).length).toBeGreaterThan(0)
  // One status announces the load, not one for the timeline and one for its time.
  expect(
    screen
      .getAllByRole('status')
      .filter((status) => status.textContent!.startsWith('Loading records…')),
  ).toHaveLength(1)
  expect(screen.queryByText(/unavailable/)).toBeNull()

  const failed = reader({ ...looking, failed: true })
  await view.update(failed)
  expect(screen.getByText('Time is unavailable. The records did not load.')).toBeVisible()
  expect(screen.queryByText(/^Loading records…/)).toBeNull()
  await userEvent.click(
    within(screen.getByRole('alert')).getByRole('button', { name: 'Try again' }),
  )
  expect(failed.retry).toHaveBeenCalledOnce()

  // Coral answers that it returns no such top-level record.
  await view.update(reader({ ...looking, unavailable: true }))
  expect(screen.getByText('Time is unavailable. Some records are unavailable.')).toBeVisible()
  expect(screen.queryByText(/^Loading records…/)).toBeNull()
  expect(within(records()).getAllByRole('row').slice(1)).toHaveLength(1)
})

/** A call of the Run at child ordinal `position`. */
const callAt = (position: number): ActivityRow => ({
  ...invocation,
  name: `${invocation.name}-${position}`,
  position,
})
// Catches: a step that skips children the trace has not read, or that stops at them
// without asking for the ones it stopped at, so the next press waits for every other read.
it('stops ↑ and ↓ at children that are not loaded and asks for the ones beside the cursor', async () => {
  const user = userEvent.setup()
  const page = activityPage({
    rows: [goal],
    expanded: [goal.name, run.name],
    branches: {
      [goal.name]: { rows: [{ ...run, position: 0 }], total: '1', next: '', unavailable: false },
      [run.name]: {
        rows: [callAt(0), callAt(1), callAt(250), callAt(251)],
        total: '252',
        next: '',
        unavailable: false,
      },
    },
  })
  const trace = reader(absorbPage(openTrace(goal.name), page))
  renderRoute(<Activity page={page} trace={trace} />, { entry: '/activity' })
  const cursor = () =>
    records().querySelector('[role=row][data-active]')?.getAttribute('data-record-key')
  expect(within(records()).getByText('Loading 248 records…')).toBeVisible()
  act(() => records().focus())
  // The Run under the Task's header, then the two calls before the gap.
  await user.keyboard('{ArrowDown}{ArrowDown}{ArrowDown}')
  expect(cursor()).toBe(callAt(1).name)
  await user.keyboard('{ArrowDown}')
  expect(cursor()).toBe(callAt(1).name)
  expect(trace.prefer).toHaveBeenLastCalledWith([{ parent: run.name, at: 2 }])
})

const timeParts = () => screen.getByRole('list', { name: 'Time by part' }).textContent

// Catches: the time of work in progress worked out once and never again, so the breakdown
// stands still while a Run runs.
it('counts the time of work in progress in the breakdown as the clock moves', async () => {
  vi.useFakeTimers({ toFake: ['Date', 'setInterval', 'clearInterval'] })
  onTestFinished(() => {
    vi.useRealTimers()
  })
  const started = Date.parse(goal.startedAt)
  vi.setSystemTime(started + 10_000)
  const open = { ...goal, outcome: 'open' as const, endedAt: null }
  const running = { ...run, state: 'running' as const, endedAt: null, position: 0 }
  const page = activityPage({
    rows: [open],
    expanded: [open.name, running.name],
    branches: {
      [open.name]: { rows: [running], total: '1', next: '', unavailable: false },
      [running.name]: { rows: [], total: '0', next: '', unavailable: false },
    },
  })
  renderRoute(<Activity page={page} trace={reader(absorbPage(openTrace(open.name), page))} />, {
    entry: '/activity',
  })
  const before = timeParts()
  expect(screen.getByText('Work is in progress. These values change.')).toBeVisible()
  await act(async () => {
    await vi.advanceTimersByTimeAsync(5_000)
  })
  expect(timeParts()).not.toBe(before)
})

// Catches: the controls of an axis shown around a table with no record on it.
it('shows the table alone while no record is open', () => {
  renderRoute(<Activity page={activityPage({ rows: [] })} />, { entry: '/activity' })
  expect(within(records()).getByText('Select a task or a run to see its timeline.')).toBeVisible()
  expect(screen.queryByRole('group', { name: 'Timeline controls' })).toBeNull()
})

it('asks for the children that end where ↑ stopped, from a record below them', async () => {
  const user = userEvent.setup()
  const below = { ...invocation, name: `${invocation.name}-250`, position: 250 }
  const page = activityPage({
    rows: [goal],
    expanded: [goal.name, run.name],
    selected: below.name,
    selectedRow: below,
    branches: {
      [goal.name]: { rows: [{ ...run, position: 0 }], total: '1', next: '', unavailable: false },
      [run.name]: { rows: [below], total: '251', offset: 250, next: '', unavailable: false },
    },
  })
  const trace = reader(absorbPage(openTrace(goal.name), page))
  renderRoute(<Activity page={page} trace={trace} />, { entry: '/activity?record=x' })
  // The rail shows the record, so the keys start from it.
  const rail = await screen.findByRole('dialog')
  act(() => rail.focus())
  await user.keyboard('{ArrowUp}')
  expect(trace.prefer).toHaveBeenLastCalledWith([{ parent: run.name, at: 249 }])
})

// Only top-level records are grouped by day: a child stays in its parent's waterfall.
it('keeps a child that started after midnight out of the list of days', () => {
  const parent = { ...goal, startedAt: '2026-09-28T23:58:00.000Z' }
  const child = { ...run, startedAt: '2026-09-29T00:04:00.000Z' }
  renderRoute(<Activity page={waterfallPage(parent, { [parent.name]: [child] })} />, {
    entry: '/activity',
  })
  expect(screen.queryByRole('rowgroup', { name: 'Tue 29 Sep 2026' })).toBeNull()
  expect(dayRecords('Mon 28 Sep 2026')).toEqual([goal.intent])
  // The page holds no record of the Run's own, and the row after it says so.
  expect(recordNames()).toEqual(['you', undefined])
  expect(within(records()).getAllByRole('row')[2]).toHaveTextContent('Not loaded.')
  // The child still states its own time, on its row.
  expect(within(records()).getAllByRole('row')[1]).toHaveTextContent(
    'Start (acceptance): 00:04:00.000 UTC.',
  )
})

// Catches: a short call that reads "0s", a unit that shows time that has not passed yet, or
// a counting duration whose last digit sits still between its one-second ticks.
it.each([
  [0, '0ms', '0s'],
  [400_000, '<1ms', '0s'],
  [263_900_000, '263ms', '0s'],
  [999_999_999, '999ms', '0s'],
  [1_000_000_000, '1.0s', '1s'],
  [2_490_000_000, '2.4s', '2s'],
  [9_999_999_999, '9.9s', '9s'],
  [10_000_000_000, '10s', '10s'],
  [59_999_999_999, '59s', '59s'],
  [263_001_000_000, '4m 23s', '4m 23s'],
  [3_600_000_000_000, '1h 0m', '1h 0m'],
])('shows %d ns as %s, or %s while it counts', (nanos, ended, counting) => {
  expect(duration(nanos)).toBe(ended)
  expect(duration(String(nanos))).toBe(ended)
  expect(duration(nanos, { counting: true })).toBe(counting)
})

const line = (number: number) => document.querySelector(`[data-line="${number}"]`)
// Catches: stepping to another record of the same Run remounting the drawer, which reset
// the source pane, its split and its scroll, or a call highlighted by its order instead
// of the location Coral stored for it.
it('keeps one accepted-source split while the selection moves within its Run', async () => {
  const source = [
    'export default async function program() {',
    '  await coral.providers.linear.listIssues({})',
    '  await coral.linear.listIssues({ page: 2 })',
    '}',
  ].join('\n')
  // Coral stored where each call ran; the later record ran the earlier line.
  const second = {
    ...invocation,
    name: `${workspace}/invocations/two`,
    position: 1,
    sourceLocation: { document: { kind: 'program' as const }, line: 2, column: 38 },
  }
  const first = {
    ...invocation,
    position: 0,
    sourceLocation: { document: { kind: 'program' as const }, line: 3, column: 22 },
  }
  const context = {
    kind: 'run' as const,
    run,
    source,
    linkedSharedTools: [],
    linkedPrivateTools: [],
    terminal: { kind: 'unavailable' as const },
    request: null,
    requestUnavailable: false,
  }
  const { router } = renderRoute(
    (location) => {
      const selected = new URLSearchParams(location.search).get('record')
      const row = [first, second].find((candidate) => candidate.name === selected) ?? null
      return (
        <Activity
          page={activityPage({
            rows: [run],
            expanded: [run.name],
            branches: {
              [run.name]: {
                rows: [first, second],
                next: '',
                unavailable: false,
              },
            },
            selected: row?.name ?? null,
            selectedRow: row,
            detail: row && {
              kind: 'invocation',
              invocation: row,
              arguments: '{}',
              result: { kind: 'upstream_outcome', value: '{}' },
              requests: [],
              request: null,
              requestUnavailable: false,
              context,
            },
          })}
        />
      )
    },
    { entry: `/activity?record=${encodeURIComponent(first.name)}` },
  )
  await vi.waitFor(() => expect(line(3)).toHaveAttribute('data-highlighted', 'true'))
  const code = line(1)!.closest('pre')

  await act(() => router.navigate(`/activity?record=${encodeURIComponent(second.name)}`))
  await vi.waitFor(() => expect(line(2)).toHaveAttribute('data-highlighted', 'true'))
  expect(line(3)).not.toHaveAttribute('data-highlighted')
  expect(line(1)!.closest('pre')).toBe(code)
})

const splitContext = {
  kind: 'run' as const,
  run: { ...run, intent: 'Send invoice reminders' },
  source: 'coral.emit(1)',
  linkedSharedTools: [],
  linkedPrivateTools: [],
  terminal: { kind: 'unavailable' as const },
  request: null,
  requestUnavailable: false,
}
const emission = {
  kind: 'output' as const,
  name: `${run.name}/events/3`,
  workspace,
  run: run.name,
  label: 'output #3',
  startedAt,
  eventKind: 'program.emitted',
  agent: '',
}
// Catches: a fixed Run header kept above the split, so a Run's header showed twice and an
// tool call showed its parent Run's header above its own, the close control left the
// selected record's own header, or any record ended in a "View in history" link.
it.each([
  [
    'an emission',
    emission,
    {
      kind: 'output',
      run: run.name,
      event: { kind: 'program.emitted', value: '1', sequence: '3', occurredAt: startedAt },
      context: splitContext,
    },
    'coral.emit #3',
  ],
  [
    'a log',
    { ...emission, eventKind: 'program.logged' },
    {
      kind: 'output',
      run: run.name,
      event: {
        kind: 'program.logged',
        level: 'info',
        message: 'sent 1',
        sequence: '3',
        occurredAt: startedAt,
      },
      context: splitContext,
    },
    'console.info #3',
  ],
  ['a Run', { ...run, intent: 'Send invoice reminders' }, splitContext, 'Send invoice reminders'],
  [
    'a tool',
    invocation,
    {
      kind: 'invocation',
      invocation,
      arguments: '{}',
      result: { kind: 'upstream_outcome', value: '{}' },
      requests: [],
      request: null,
      requestUnavailable: false,
      context: splitContext,
    },
    'coral.providers.linear.listIssues',
  ],
] satisfies [string, ActivityRow, ActivityPage['detail'], string][])(
  'heads %s selection in the split with its own header alone',
  (_, selectedRow, detail, title) => {
    renderRoute(
      <Activity
        page={activityPage({
          rows: [goal],
          selected: selectedRow.name,
          selectedRow,
          detail,
        })}
      />,
      { entry: '/activity?record=one' },
    )
    const drawer = screen.getByRole('dialog', { name: 'Activity details' })
    expect(within(drawer).getByRole('region', { name: 'Accepted source' })).toBeInTheDocument()
    const selected = within(drawer).getByRole('region', {
      name: 'Selected record',
    })
    const [heading, ...others] = within(selected).getAllByRole('heading', {
      level: 2,
    })
    expect(others).toHaveLength(0)
    expect(heading).toHaveTextContent(title)
    const header = heading!.closest('header')!
    expect(within(drawer).getAllByRole('button', { name: 'Close details' })).toEqual([
      within(header).getByRole('button', { name: 'Close details' }),
    ])
    for (const name of ['View in history', 'Reveal record'])
      expect(within(drawer).queryByRole('link', { name })).toBeNull()
  },
)

// Catches: a Task header without the Task's feedback when a link opens a record under the
// Task. The trace reads the Task's detail for it; a page that selects the Task holds it.
it.each(['a link opens a record under the Task', 'the Task itself is selected'] as const)(
  'states the feedback of the Task when %s',
  (opened) => {
    const feedback = 'Found 3 invoices.\nTwo were already paid.'
    const child = { ...tool, position: 0 }
    const taskDetail = { kind: 'task' as const, task: goal, feedback }
    const branches = {
      [goal.name]: { rows: [child], total: '1', next: '', unavailable: false },
    }
    const underTask = opened === 'a link opens a record under the Task'
    const page = activityPage({
      rows: [goal],
      expanded: [goal.name],
      branches,
      ...(underTask
        ? {
            selected: tool.name,
            selectedRow: child,
            detail: {
              kind: 'tool' as const,
              task: goal.name,
              interaction: child,
              evidence: { kind: 'unknown' as const },
            },
          }
        : { selected: goal.name, selectedRow: goal, detail: taskDetail }),
    })
    // The trace's read of the Task's detail, answered as the loader answers it.
    const read = receiveTrace(
      absorbPage(openTrace(goal.name), page),
      { windows: [], taskDetail: true },
      activityPage({ rows: [goal], selected: goal.name, selectedRow: goal, detail: taskDetail }),
    )
    renderRoute(<Activity page={page} trace={underTask ? reader(read) : undefined} />, {
      entry: `/activity?record=${encodeURIComponent(underTask ? tool.name : goal.name)}`,
    })
    const header = screen.getByRole('region', { name: 'Task' })
    // Exactly as the agent wrote it, line breaks and all.
    expect(within(header).getByText(/^Found 3 invoices\./).textContent).toBe(feedback)
  },
)
