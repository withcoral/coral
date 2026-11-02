/** @vitest-environment happy-dom */
import { act, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, it } from 'vitest'
import { renderRoute } from '@/test-support/route'
import { Activity } from './activity'
import { activityPage, runRow, toolRow, workspace } from './fixtures'
import type { ActivityPage, ActivityRow } from './types'

const startedAt = '2026-09-18T14:02:00.000Z'
const goal: ActivityRow & { kind: 'task' } = {
  kind: 'task',
  name: `${workspace}/tasks/invoices`,
  workspace,
  agent: '',
  actor: 'you',
  intent: 'Send overdue invoices',
  outcome: 'open',
  startedAt,
  endedAt: null,
}
const listPath = `/${workspace}/activity`
const searched = (overrides: Partial<ActivityPage> = {}) =>
  activityPage({
    listPath,
    query: 'invoice',
    rows: [{ ...goal, intentMatches: true }],
    ...overrides,
  })
const address = (router: ReturnType<typeof renderRoute>['router']) =>
  new URLSearchParams(router.state.location.search)

it('searches as you type, with one history entry per search and Back restoring the text', async () => {
  const user = userEvent.setup()
  const { router } = renderRoute(() => <Activity page={activityPage({ listPath })} />, {
    entry: `${listPath}?record=${goal.name}&tree=1&expand=${goal.name}`,
  })
  const field = screen.getByRole('searchbox', { name: 'Search intent' })
  await user.type(field, 'in')
  expect(address(router).get('q')).toBe('in')
  expect(screen.getByRole('status')).toHaveTextContent('Type at least 3 characters to search.')
  expect(field).toHaveAccessibleDescription('Type at least 3 characters to search.')
  await user.type(field, 'voice')
  expect(address(router).get('q')).toBe('invoice')
  expect(address(router).get('record')).toBe(goal.name)
  expect(address(router).has('expand')).toBe(false)
  expect(screen.queryByText('Type at least 3 characters to search.')).toBeNull()
  await act(() => router.navigate(-1))
  expect(address(router).has('q')).toBe(false)
  expect(field).toHaveValue('')
  await user.type(field, '  ')
  expect(address(router).has('q')).toBe(false)
})

it('marks where a Task intent matches the search', () => {
  renderRoute(() => <Activity page={searched()} />, {
    entry: `${listPath}?q=invoice`,
  })
  const mark = screen.getByText('invoice', { selector: 'strong' })
  expect(mark.closest('a')).toHaveTextContent('Send overdue invoices')
})

it('says when no Task or Run intent matches the search', () => {
  renderRoute(() => <Activity page={searched({ rows: [] })} />, {
    entry: `${listPath}?q=invoice`,
  })
  expect(screen.getByText('No matching activity')).toBeVisible()
  expect(screen.getByText('No task or run intent contains “invoice”.')).toBeVisible()
  expect(screen.queryByText('No activity yet')).toBeNull()
})

const task = (id: number): ActivityRow & { kind: 'task' } => ({
  ...goal,
  name: `${workspace}/tasks/t${id}`,
  intent: `Weekly chores ${id}`,
  intentMatches: false,
})
/** A search that lists `count` Tasks only for one matching Run each. */
const keptForRuns = (count: number, overrides: Partial<ActivityPage> = {}) => {
  const rows = Array.from({ length: count }, (_, id) => task(id))
  return searched({
    rows,
    branches: Object.fromEntries(
      rows.map((row, id) => [
        row.name,
        {
          rows: [
            runRow(`r${id}`, row.name, {
              submission: { ...toolRow(row.name, `i${id}`), intent: `Export invoice ${id}` },
            }),
          ],
          total: '1',
          offset: 0,
          next: '',
          unavailable: false,
        },
      ]),
    ),
    ...overrides,
  })
}

// Catches: reveal that stops at the limit on records you expand, or writes to the address.
it('opens every Task a search lists only for its Runs, past the limit on records you expand', () => {
  const { router } = renderRoute(() => <Activity page={keptForRuns(25)} />, {
    entry: `${listPath}?q=invoice`,
  })
  const table = screen.getByRole('table', { name: 'Activity records' })
  for (let id = 0; id < 25; id += 1)
    expect(within(table).getByRole('link', { name: `Export invoice ${id}` })).toBeVisible()
  expect(screen.queryByText('Collapse a record before you expand another.')).toBeNull()
  // The search decides what opens, so the address stays short.
  expect(address(router).has('expand')).toBe(false)
})

// Catches: a collapsed Task that opens again, or that the limit keeps closed.
it('keeps a Task you collapsed closed, and lets you open it while you are at the limit', async () => {
  const user = userEvent.setup()
  const collapsed = task(0).name
  const chosen = Array.from({ length: 20 }, (_, id) => `${workspace}/runs/chosen${id}`)
  const { [collapsed]: _closed, ...branches } = keptForRuns(2).branches
  const { router } = renderRoute(
    () => (
      <Activity
        page={keptForRuns(2, {
          branches,
          expanded: [...chosen, task(1).name],
          collapsed: [collapsed],
        })}
      />
    ),
    {
      entry: `${listPath}?q=invoice&tree=1&collapse=${collapsed}${chosen
        .map((name) => `&expand=${name}`)
        .join('')}`,
    },
  )
  const table = screen.getByRole('table', { name: 'Activity records' })
  expect(within(table).queryByRole('link', { name: 'Export invoice 0' })).toBeNull()
  expect(within(table).getByRole('link', { name: 'Export invoice 1' })).toBeVisible()
  // Your own records are at the limit, and the search's are not among them.
  expect(screen.getByText('Collapse a record before you expand another.')).toBeVisible()
  const row = within(table)
    .getByRole('link', { name: 'Weekly chores 0' })
    .closest<HTMLElement>('[role="row"]')!
  await user.click(within(row).getByRole('button', { name: 'Expand record' }))
  expect(address(router).has('collapse')).toBe(false)
  expect(address(router).getAll('expand')).toEqual(chosen)
})

it('lists the open record that the search leaves out first, then says nothing matches', () => {
  renderRoute(
    () => <Activity page={searched({ rows: [], selected: goal.name, selectedRow: goal })} />,
    { entry: `${listPath}?q=invoice&record=${goal.name}` },
  )
  const table = screen.getByRole('table', { name: 'Activity records' })
  const rows = within(table).getAllByRole('row')
  const open = rows.findIndex((row) => within(row).queryByText('Outside this search:'))
  const empty = rows.findIndex((row) => within(row).queryByText('No matching activity'))
  expect(within(rows[open]!).getByText('Send overdue invoices')).toBeVisible()
  expect(open).toBeGreaterThan(-1)
  expect(empty).toBe(open + 1)
})

it.each([
  ['ASCII letters', 'INVOICE', 'Été invoice review', 'invoice'],
  ['accented letters', 'été', 'Été invoice review', 'Été'],
])('marks %s in either case, as Coral matches them', (_, query, intent, marked) => {
  const row = { ...goal, intent, intentMatches: true }
  renderRoute(() => <Activity page={searched({ query, rows: [row] })} />, {
    entry: `${listPath}?q=${encodeURIComponent(query)}`,
  })
  expect(screen.getByText(/./u, { selector: 'strong' }).textContent).toBe(marked)
})
