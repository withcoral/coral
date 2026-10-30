/** @vitest-environment happy-dom */
import { act, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, it } from 'vitest'
import { renderRoute } from '@/test-support/route'
import { Activity } from './activity'
import { activityPage, workspace } from './fixtures'
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

it.each([
  ['opens a Task a later page lists only for its Runs', '', [goal.name]],
  ['keeps one you collapsed closed', `&collapse=${goal.name}`, []],
])('%s', async (_, collapse, expanded) => {
  const page = searched({ rows: [{ ...goal, intentMatches: false }] })
  const { router } = renderRoute(() => <Activity page={page} />, {
    entry: `${listPath}?q=invoice&tree=1${collapse}`,
  })
  await waitFor(() => expect(address(router).getAll('expand')).toEqual(expanded))
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
  ['ASCII letters in either case', 'INVOICE', 'invoice'],
  ['other letters only as typed', 'été', null],
])('marks %s, as Coral matches them', (_, query, marked) => {
  const row = { ...goal, intent: 'Été invoice review', intentMatches: true }
  renderRoute(() => <Activity page={searched({ query, rows: [row] })} />, {
    entry: `${listPath}?q=${query}`,
  })
  expect(screen.queryByText(/./, { selector: 'strong' })?.textContent ?? null).toBe(marked)
})
