/** @vitest-environment happy-dom */
import { act, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, it, onTestFinished, vi } from 'vitest'
import { markupRoute, renderLivePage, renderRoute } from '@/test-support/route'
import { Activity, duration } from './activity'
import { ActivityArchivedContext } from './live'
import { ActionReviewSession } from '../actions/review'
import { activityPage, approval, approvalRequest, run as waiting, workspace } from './fixtures'
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

it('marks only the tool call as needing action, never its containing Run', () => {
  expect(render({ ...run, state: 'running', requiresAction: true })).not.toContain('needs action')
  expect(render({ ...invocation, state: 'pending', requiresAction: true })).toContain(
    'needs action',
  )
})

it('retains the selected review and draft across pending reorder and removal, but conceals access loss', async () => {
  const user = userEvent.setup()
  const initial = activityPage({
    rows: [waiting],
    selected: waiting.name,
    selectedRow: waiting,
    pending: {
      actions: [{ action: approval, run: waiting }],
      actionCount: '1',
      next: '',
      unavailable: false,
    },
    detail: {
      kind: 'run',
      run: waiting,
      request: approvalRequest,
      source: 'return null;',
      linkedFunctions: [],
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

  await publish({
    ...initial,
    pending: { actions: [], actionCount: '0', next: '', unavailable: false },
  })
  expect(document.body).toHaveTextContent('exact-recipient')
  expect(screen.getByRole('textbox', { name: 'Comment (optional)' })).toBe(comment)
  expect(comment).toHaveValue('Keep this draft')

  await publish({
    ...initial,
    detail: null,
    selectedRow: null,
    unavailable: true,
    pending: { actions: [], actionCount: '0', next: '', unavailable: true },
  })
  expect(document.body).not.toHaveTextContent('exact-recipient')
  expect(screen.queryByRole('button', { name: 'Approve this call' })).not.toBeInTheDocument()
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
  ['the full list', undefined, ['Find invoices', 'Find tools']],
])('%s lists child records only when it pages', (_, more, records) => {
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
  expect(shown.filter((text) => text !== 'View all activity')).toEqual(records)
  expect(screen.queryAllByRole('button', { name: /record$/ })).toHaveLength(more ? 0 : 1)
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

// Catches: a glimpse record opening its detail on the Overview instead of the Activity page.
it.each<[string, ActivityViewProps['more'], string]>([
  [
    'a glimpse',
    { to: `/${workspace}/activity`, label: 'View all activity', roots: 5 },
    `/${workspace}/activity`,
  ],
  ['the full list', undefined, `/${workspace}`],
])('%s opens a clicked record where it lists in full', async (_, more, pathname) => {
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
  expect(new URLSearchParams(router.state.location.search).get('record')).toBe(goal.name)
})

// An expanded record with no children says why, in terms of its own lifecycle.
// A Run in a state Reef does not know may still be running, so its copy claims neither.
it.each<[string, ActivityRow, string]>([
  ['ended Run', run, 'This execution called no tools.'],
  [
    'running Run',
    { ...run, state: 'running', endedAt: null },
    'This execution has not called a tool yet.',
  ],
  [
    'Run in an unknown state',
    { ...run, state: 'unknown', endedAt: null },
    'This execution has no recorded tools.',
  ],
  ['ended Task', goal, 'This task made no calls.'],
  ['open Task', { ...goal, outcome: 'open', endedAt: null }, 'This task has not made a call yet.'],
])('an expanded %s with no children explains why', (_name, row, copy) => {
  renderRoute(
    <Activity
      page={activityPage({
        rows: [row],
        expanded: [row.name],
        branches: { [row.name]: { rows: [], next: '', unavailable: false } },
      })}
    />,
    { entry: '/activity' },
  )
  expect(screen.getByRole('cell', { name: copy })).toBeInTheDocument()
})

it('an unavailable expanded record heads a group whose only child is the status row', () => {
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
  // The day heading, then the record heading, then its one child.
  const rows = within(screen.getByRole('rowgroup', { name: 'Fri 18 Sep 2026' })).getAllByRole('row')
  expect(rows).toHaveLength(3)
  expect(rows[1]).toHaveAttribute('aria-level', '2')
  expect(within(rows[1]!).getByRole('link', { name: goal.intent })).toBeInTheDocument()
  expect(within(rows[1]!).getByRole('button', { name: 'Collapse record' })).toBeInTheDocument()
  expect(rows[2]).toHaveTextContent('This history is unavailable.')
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

it('names the day of a selected record that sits outside every day group', () => {
  renderRoute(<Activity page={activityPage({ rows: [], selectedRow: goal })} />, {
    entry: '/activity',
  })
  const row = within(screen.getByRole('table', { name: 'Activity records' }))
    .getAllByRole('row')
    .find((candidate) => within(candidate).queryByText('Outside this workspace or page:'))!
  expect(within(row).getByText('Fri 18 Sep 2026')).toBeInTheDocument()
})

const headingLevel = (name: string) =>
  screen
    .getAllByRole('row')
    .find((row) => within(row).queryByRole('link', { name }))!
    .getAttribute('aria-level')

// Wax pins each heading under its parent's; Activity nests a Run's group in its Task's.
it('nests an expanded Run under its expanded Task, under the day', () => {
  renderRoute(
    <Activity
      page={activityPage({
        rows: [goal],
        expanded: [goal.name, run.name],
        branches: {
          [goal.name]: { rows: [run], next: '', unavailable: false },
          [run.name]: { rows: [invocation], next: '', unavailable: false },
        },
      })}
    />,
    { entry: '/activity' },
  )
  expect(headingLevel(goal.intent)).toBe('2')
  expect(headingLevel('you')).toBe('3')
  expect(screen.getByRole('rowgroup', { name: 'Fri 18 Sep 2026' })).toContainElement(
    screen.getByRole('link', { name: invocation.callPath }),
  )
})

// Only top-level records are grouped by day: a child stays with its parent.
it('keeps a child that started after midnight under its parent and its day', () => {
  const parent = { ...goal, startedAt: '2026-09-28T23:58:00.000Z' }
  const child = { ...run, startedAt: '2026-09-29T00:04:00.000Z' }
  renderRoute(
    <Activity
      page={activityPage({
        rows: [parent],
        expanded: [parent.name],
        branches: {
          [parent.name]: { rows: [child], next: '', unavailable: false },
        },
      })}
    />,
    { entry: '/activity' },
  )
  expect(screen.queryByRole('rowgroup', { name: 'Tue 29 Sep 2026' })).toBeNull()
  expect(dayRecords('Mon 28 Sep 2026')).toEqual([goal.intent, 'you'])
  expect(headingLevel(goal.intent)).toBe('2')
  expect(screen.getByText('00:04:00')).toHaveAttribute('title', '2026-09-29 00:04:00 UTC')
})

// Catches: a duration that shows fractions of a second, or time that has not passed yet.
it.each([
  [0, '0s'],
  [999_999_999, '0s'],
  [1_000_000_000, '1s'],
  [1_999_999_999, '1s'],
  [59_999_999_999, '59s'],
  [263_001_000_000, '4m 23s'],
  [3_600_000_000_000, '1h 0m'],
])('shows %d ns as whole time, %s', (nanos, text) => {
  expect(duration(nanos)).toBe(text)
  expect(duration(String(nanos))).toBe(text)
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
    linkedFunctions: [],
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
  linkedFunctions: [],
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
    'program output',
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
    'program log',
  ],
  [
    'a Run',
    { ...run, intent: 'Send invoice reminders' },
    splitContext,
    'Send invoice reminders',
    'program run',
  ],
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
    'tool',
  ],
] satisfies [string, ActivityRow, ActivityPage['detail'], string, string][])(
  'heads %s selection in the split with its own header alone',
  (_, selectedRow, detail, title, kind) => {
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
    expect(header).toHaveTextContent(kind)
    expect(within(drawer).getAllByRole('button', { name: 'Close details' })).toEqual([
      within(header).getByRole('button', { name: 'Close details' }),
    ])
    for (const name of ['View in history', 'Reveal record'])
      expect(within(drawer).queryByRole('link', { name })).toBeNull()
  },
)
