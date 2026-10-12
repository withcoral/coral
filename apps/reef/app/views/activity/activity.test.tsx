/** @vitest-environment happy-dom */
import { act, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { useEffect, useState } from 'react'
import { expect, it, onTestFinished, vi } from 'vitest'
import { markupRoute, renderRoute } from '@/test-support/route'
import { Activity } from './activity'
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
const operation: ActivityRow & { kind: 'invocation' } = {
  ...run,
  kind: 'invocation',
  name: `${workspace}/invocations/one`,
  run: run.name,
  state: 'ended',
  outcome: 'upstream_outcome',
  dispatchedAt: startedAt,
  waitingNanos: '0',
  provider: `${workspace}/providers/linear`,
  operation: 'listIssues',
  callPath: 'coral.providers.linear.listIssues',
}
const tool: ActivityRow & { kind: 'tool' } = {
  kind: 'tool',
  name: `${goal.name}/interactions/search`,
  workspace,
  agent: '',
  task: goal.name,
  tool: 'search',
  intent: 'Find operations',
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
  ['unknown attempt', { ...tool, outcome: 'unknown' }, 'unknown / incomplete'],
  [
    'accepted but unavailable execution',
    { ...tool, tool: 'exec', acceptedRun: run.name },
    'accepted · run unavailable',
  ],
  [
    'operation that needs action',
    { ...operation, state: 'pending', outcome: 'unknown', requiresAction: true },
    'pending',
  ],
  ['native upstream outcome, not inferred success', operation, 'upstream outcome'],
  ['uncertain dispatch', { ...operation, outcome: 'indeterminate' }, 'indeterminate'],
  [
    'Run termination, not an upstream error',
    { ...operation, outcome: 'run_terminated' },
    'run terminated',
  ],
])('marks %s with its exact accessible state', (_name, row, label) => {
  const markup = render(row)
  expect(markup).toContain(`aria-description="${label}"`)
  expect(markup).toContain(`title="${label}"`)
  expect(markup).not.toContain('>STATE<')
})

it('marks only the operation as needing action, never its containing Run', () => {
  expect(render({ ...run, state: 'running', requiresAction: true })).not.toContain('needs action')
  expect(render({ ...operation, state: 'pending', requiresAction: true })).toContain('needs action')
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
  let publish!: (value: ActivityPage) => void
  function Surface() {
    const [current, setCurrent] = useState(initial)
    useEffect(() => {
      publish = setCurrent
    }, [])
    return (
      <ActionReviewSession>
        <Activity page={current} />
      </ActionReviewSession>
    )
  }
  renderRoute(<Surface />, { entry: '/activity?record=one&request=one' })
  expect(document.body).toHaveTextContent('exact-recipient')
  const comment = screen.getByRole('textbox', { name: 'Comment (optional)' })
  await user.type(comment, 'Keep this draft')

  await act(async () =>
    publish({
      ...initial,
      pending: { actions: [], actionCount: '0', next: '', unavailable: false },
    }),
  )
  expect(document.body).toHaveTextContent('exact-recipient')
  expect(screen.getByRole('textbox', { name: 'Comment (optional)' })).toBe(comment)
  expect(comment).toHaveValue('Keep this draft')

  await act(async () =>
    publish({
      ...initial,
      detail: null,
      selectedRow: null,
      unavailable: true,
      pending: { actions: [], actionCount: '0', next: '', unavailable: true },
    }),
  )
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
  const page = activityPage({ agent: kind === 'agent history' ? `${workspace}/agents/a` : null })
  renderRoute(<Activity page={page} connect={connect} />, { entry: '/activity' })
  expect(screen.getByText('No activity yet')).toBeInTheDocument()
  expect(screen.getByText(description)).toBeInTheDocument()
  expect(screen.queryAllByRole('button', { name: 'Copy MCP URL' })).toHaveLength(offered ? 1 : 0)
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
  ['the full list', undefined, ['Find invoices', 'Find operations']],
])('%s lists child records only when it pages', (_, more, records) => {
  renderRoute(
    <Activity
      page={activityPage({
        rows: [goal],
        expanded: [goal.name],
        branches: { [goal.name]: { rows: [tool], next: '', unavailable: false } },
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
  const control = { load: vi.fn(), loading: false, failed: false, automatic: false }
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
  ['ended Run', run, 'This execution called no operations.'],
  [
    'running Run',
    { ...run, state: 'running', endedAt: null },
    'This execution has not called an operation yet.',
  ],
  [
    'Run in an unknown state',
    { ...run, state: 'unknown', endedAt: null },
    'This execution has no recorded operations.',
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
          [run.name]: { rows: [operation], next: '', unavailable: false },
        },
      })}
    />,
    { entry: '/activity' },
  )
  expect(headingLevel(goal.intent)).toBe('2')
  expect(headingLevel('you')).toBe('3')
  expect(screen.getByRole('rowgroup', { name: 'Fri 18 Sep 2026' })).toContainElement(
    screen.getByRole('link', { name: operation.callPath }),
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
        branches: { [parent.name]: { rows: [child], next: '', unavailable: false } },
      })}
    />,
    { entry: '/activity' },
  )
  expect(screen.queryByRole('rowgroup', { name: 'Tue 29 Sep 2026' })).toBeNull()
  expect(dayRecords('Mon 28 Sep 2026')).toEqual([goal.intent, 'you'])
  expect(headingLevel(goal.intent)).toBe('2')
  expect(screen.getByText('00:04:00')).toHaveAttribute('title', '2026-09-29 00:04:00 UTC')
})
