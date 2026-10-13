/** @vitest-environment happy-dom */
import { act, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { useEffect, useState } from 'react'
import type { Location } from 'react-router'
import { expect, it, vi } from 'vitest'
import { renderRoute } from '@/test-support/route'
import { Activity } from './activity'
import { ActionReviewSession } from '../actions/review'
import { activityPage, approval, approvalRequest, run as waiting, workspace } from './fixtures'
import type { ActivityBranch, ActivityPage, ActivityRow, ActivityViewProps } from './types'

const startedAt = '2026-09-18T14:02:00.000Z'
const task = (id: string): ActivityRow & { kind: 'task' } => ({
  kind: 'task',
  name: `${workspace}/tasks/${id}`,
  workspace,
  agent: '',
  actor: 'you',
  intent: `Task ${id}`,
  outcome: 'success',
  startedAt,
  endedAt: startedAt,
})
const run = (parent: string, id: string, position?: number): ActivityRow & { kind: 'run' } => ({
  kind: 'run',
  name: `${workspace}/runs/${id}`,
  workspace,
  agent: '',
  // A Run row names its actor.
  actor: `Run ${id}`,
  task: parent,
  state: 'succeeded',
  startedAt,
  endedAt: startedAt,
  durationNanos: '1000000',
  requiresAction: false,
  position,
})
const tool = (parent: string, id: string, position?: number): ActivityRow & { kind: 'tool' } => ({
  kind: 'tool',
  name: `${parent}/interactions/${id}`,
  workspace,
  agent: '',
  task: parent,
  tool: 'search',
  intent: `Tool ${id}`,
  outcome: 'succeeded',
  startedAt,
  completedAt: startedAt,
  acceptedRun: '',
  position,
})

const output = (parent: string, position: number, label: string, name?: string) =>
  ({
    kind: 'output',
    name: name ?? `${parent}/events/${position + 2}`,
    workspace,
    run: parent,
    label,
    startedAt,
    eventKind: 'program.emitted',
    agent: '',
    position,
  }) satisfies ActivityRow
/** A Run's final value: the loader names it apart from its `run.ended` Event. */
const final = (parent: string) => output(parent, 1, 'final value', `${parent}/final`)

type Records = Partial<ActivityPage> & {
  rows: ActivityRow[]
  /** A selected record's detail; a Task's detail is its own. */
  details?: Record<string, ActivityPage['detail']>
}

/**
 * Activity as its route renders it: the URL selects and expands, as a loader
 * would. `publish` replaces the records, as a live update does.
 */
function renderActivity({
  entry = '/activity',
  list,
  more,
  ...initial
}: Records & { entry?: string } & Pick<ActivityViewProps, 'list' | 'more'>) {
  let publish!: (records: Records) => void
  function Surface({ location }: { location: Location }) {
    const [records, setRecords] = useState<Records>(initial)
    useEffect(() => {
      publish = setRecords
    }, [])
    const { rows, branches = {}, details = {}, ...rest } = records
    const query = new URLSearchParams(location.search)
    const expanded = query.getAll('expand')
    // A final value's detail is its Run's, so the loader selects the Run.
    const selected = query.get('record')?.replace(/\/final$/, '') ?? null
    const all = [...rows, ...Object.values(branches).flatMap((branch) => branch.rows)]
    const selectedRow = all.find((row) => row.name === selected) ?? rest.selectedRow ?? null
    const page = activityPage({
      ...rest,
      rows,
      expanded,
      branches: Object.fromEntries(
        Object.entries(branches).filter(([name]) => expanded.includes(name)),
      ) as Record<string, ActivityBranch>,
      selected,
      selectedRow,
      detail:
        (selected ? details[selected] : undefined) ??
        (selectedRow?.kind === 'task' ? { kind: 'task', task: selectedRow } : null),
    })
    return (
      <ActionReviewSession>
        <Activity page={page} list={list} more={more} />
      </ActionReviewSession>
    )
  }
  return {
    user: userEvent.setup(),
    publish: (records: Records) => act(async () => publish(records)),
    ...renderRoute((location) => <Surface location={location} />, { entry }),
  }
}

const expand = (...names: string[]) =>
  `/activity?${new URLSearchParams(names.map((name) => ['expand', name]))}`
const branch = (rows: ActivityRow[], total = rows.length): ActivityBranch => ({
  rows,
  total: String(total),
  next: '',
  unavailable: false,
})

/** The record the keyboard cursor rests on, by its link. */
const cursor = () => {
  const row = screen
    .getByRole('table', { name: 'Activity records' })
    .querySelector<HTMLElement>('[role="row"][data-active]')
  return row ? within(row).getByRole('link').textContent : null
}

// Catches: a cursor that wraps around or runs off either end of the loaded list.
it.each<[string, string, string | null]>([
  ['↓ starts at the first record', '{ArrowDown}', 'Task a'],
  ['↑ starts at the last record', '{ArrowUp}', 'Task c'],
  ['↓ stops at the last record', '{ArrowUp}{ArrowDown}', 'Task c'],
  ['↑ stops at the first record', '{ArrowDown}{ArrowUp}', 'Task a'],
  ['↓ then ↓ moves to the next record', '{ArrowDown}{ArrowDown}', 'Task b'],
])('%s', async (_, keys, expected) => {
  const { user } = renderActivity({ rows: [task('a'), task('b'), task('c')] })
  expect(cursor()).toBeNull()
  await user.keyboard(keys)
  expect(cursor()).toBe(expected)
})

it('an empty table has no cursor to move', async () => {
  const { user } = renderActivity({ rows: [] })
  await user.keyboard('{ArrowDown}{ArrowUp}')
  expect(screen.getByText('No activity yet')).toBeInTheDocument()
  expect(
    screen.getByRole('table', { name: 'Activity records' }).querySelector('[data-active]'),
  ).toBeNull()
})

const control = () => ({ load: vi.fn(), loading: false, failed: false, automatic: false })
// A Task with a nested Run, loaded out of order, with ordinal 3 and ordinal 5 unloaded.
function sparseTree() {
  const a = task('a')
  const r = run(a.name, 'r', 1)
  const list = { count: 2, newer: control(), older: control() }
  const rendered = renderActivity({
    rows: [a, task('b')],
    entry: expand(a.name, r.name),
    list,
    branches: {
      [a.name]: branch([tool(a.name, '4', 4), tool(a.name, '0', 0), r, tool(a.name, '2', 2)], 6),
      [r.name]: branch([tool(r.name, 'r0', 0)]),
    },
  })
  return { ...rendered, list }
}
async function press(user: ReturnType<typeof userEvent.setup>, key: string, times: number) {
  const seen = []
  for (let count = 0; count < times; count++) {
    await user.keyboard(key)
    seen.push(cursor())
  }
  return seen
}

// Catches: a cursor that follows the loaded array instead of the table, skips
// across missing child ordinals, or asks for more records from the keyboard.
it.each([
  ['↓', ['Task a', 'Tool 0', 'Run r', 'Tool r0', 'Tool 2', 'Tool 2']],
  // The unloaded tail of Task a's children sits above Task b.
  ['↑', ['Task b', 'Task b']],
])(
  '%s moves through records in displayed tree order and stops at unloaded gaps',
  async (key, expected) => {
    const { user, list } = sparseTree()
    expect(await press(user, key === '↓' ? '{ArrowDown}' : '{ArrowUp}', expected.length)).toEqual(
      expected,
    )
    expect(list.newer!.load).not.toHaveBeenCalled()
    expect(list.older!.load).not.toHaveBeenCalled()
  },
)

const recordIn = (router: ReturnType<typeof renderActivity>['router']) =>
  new URLSearchParams(router.state.location.search).get('record')
const expandedIn = (router: ReturnType<typeof renderActivity>['router']) =>
  new URLSearchParams(router.state.location.search).getAll('expand')

// Catches: a toggle that flips on every press, moves the cursor when toggling
// its own branch, fails to fall back to a direct Task child's parent, or
// passes the expansion bound. ← and → no longer touch expansion at all.
it.each<[string, string, string[], string[]]>([
  ['Space expands a collapsed Task', '{ArrowDown} ', [], ['a']],
  ['Space collapses an expanded Task', '{ArrowDown} ', ['a'], []],
  ['Space on a direct Task child collapses the Task', '{ArrowDown}{ArrowDown} ', ['a'], []],
  [
    'Space expands nothing at the expansion bound',
    '{ArrowDown} ',
    Array.from({ length: 20 }, (_, index) => `other${index}`),
    Array.from({ length: 20 }, (_, index) => `other${index}`),
  ],
])('%s', async (_, keys, before, after) => {
  const a = task('a')
  const name = (id: string) => (id === 'a' ? a.name : id)
  const { user, router } = renderActivity({
    rows: [a],
    entry: before.length ? expand(...before.map(name)) : '/activity',
    branches: { [a.name]: branch([tool(a.name, '0', 0)]) },
  })
  await user.keyboard(keys)
  expect(expandedIn(router)).toEqual(after.map(name))
})

/**
 * A control outside this table, left focused by whatever navigation opened
 * the page (a sidebar link, say) — never one of the table's own rows.
 */
function focusOutsideControl() {
  const link = document.createElement('a')
  link.href = '#elsewhere'
  link.textContent = 'Elsewhere'
  document.body.appendChild(link)
  link.focus()
  return () => link.remove()
}

// Catches: a first keyboard press on a freshly opened table doing nothing
// because a control the navigation left focused, unrelated to this table,
// is mistaken for a control the key belongs to.
it.each([
  ['Space expands the cursor Task', ' ', [task('a').name], null],
  ['Enter opens the cursor record', '{Enter}', [], task('a').name],
])(
  '%s even while a control outside the table is still focused',
  async (_, key, expanded, record) => {
    const a = task('a')
    const { user, router } = renderActivity({ rows: [a] })
    const restore = focusOutsideControl()
    await user.keyboard('{ArrowDown}')
    expect(cursor()).toBe('Task a')
    await user.keyboard(key)
    expect(expandedIn(router)).toEqual(expanded)
    expect(recordIn(router)).toBe(record)
    restore()
  },
)

// Catches: a held key that toggles a branch or opens the detail again on every repeat.
it.each([
  ['Space', '{ >3/}', [task('a').name], null],
  ['Enter', '{Enter>3/}', [], task('a').name],
])('a held %s acts once', async (_, keys, expanded, record) => {
  const a = task('a')
  const { user, router } = renderActivity({
    rows: [a],
    branches: { [a.name]: branch([tool(a.name, '0', 0)]) },
  })
  await user.keyboard(`{ArrowDown}${keys}`)
  expect(expandedIn(router)).toEqual(expanded)
  expect(recordIn(router)).toBe(record)
})

// Catches: a cursor left on a record its collapsed parent now hides.
it('collapsing the branch that holds the cursor moves the cursor to its parent', async () => {
  const a = task('a')
  const { user, router } = renderActivity({
    rows: [a, task('b')],
    entry: expand(a.name),
    branches: { [a.name]: branch([tool(a.name, '0', 0)]) },
  })
  await user.keyboard('{ArrowDown}{ArrowDown}')
  expect(cursor()).toBe('Tool 0')
  await user.click(screen.getByRole('button', { name: 'Collapse record' }))
  expect(expandedIn(router)).toEqual([])
  expect(cursor()).toBe('Task a')
})

// Catches: Space collapsing the enclosing Task instead of the closer Run,
// falling back on a collapsed Run instead of expanding it, leaving the
// cursor on a record the collapse just hid, or opening detail.
it.each<[string, string[], string, string[], string]>([
  [
    'an invocation nested in a Run collapses only the Run, moving the cursor to it',
    ['a', 'r'],
    '{ArrowDown}{ArrowDown}{ArrowDown}{ArrowDown} ',
    ['a'],
    'Run r',
  ],
  [
    'an expanded nested Run collapses only itself, keeping its Task expanded',
    ['a', 'r'],
    '{ArrowDown}{ArrowDown}{ArrowDown} ',
    ['a'],
    'Run r',
  ],
  [
    'a collapsed nested Run expands itself',
    ['a'],
    '{ArrowDown}{ArrowDown}{ArrowDown} ',
    ['a', 'r'],
    'Run r',
  ],
])('Space on %s', async (_, before, keys, after, expectedCursor) => {
  const a = task('a')
  const r = run(a.name, 'r', 1)
  const name = (id: string) => (id === 'a' ? a.name : id === 'r' ? r.name : id)
  const { user, router } = renderActivity({
    rows: [a],
    entry: expand(...before.map(name)),
    branches: {
      [a.name]: branch([tool(a.name, 'direct', 0), r]),
      [r.name]: branch([tool(r.name, 'nested', 0)]),
    },
  })
  await user.keyboard(keys)
  expect(expandedIn(router)).toEqual(after.map(name))
  expect(cursor()).toBe(expectedCursor)
  expect(recordIn(router)).toBeNull()
})

// Catches: a root, non-expandable record's Space doing anything at all, now
// that there is no enclosing record to fall back to.
it('Space on a root record with no parent does nothing', async () => {
  const standalone = tool('task', 'x', 0)
  const { user, router } = renderActivity({ rows: [standalone] })
  await user.keyboard('{ArrowDown} ')
  expect(cursor()).toBe('Tool x')
  expect(expandedIn(router)).toEqual([])
})

// Catches: a cursor kept by array index, which a new or reordered record retargets.
it('keeps the cursor on its record when records arrive or reorder', async () => {
  const [a, b, c] = [task('a'), task('b'), task('c')]
  const { user, publish } = renderActivity({ rows: [a, b] })
  await user.keyboard('{ArrowDown}{ArrowDown}')
  expect(cursor()).toBe('Task b')
  await publish({ rows: [c, b, a] })
  expect(cursor()).toBe('Task b')
  await user.keyboard('{ArrowDown}')
  expect(cursor()).toBe('Task a')
})

// Catches: a cursor that jumps to the submission's neighbour when it is accepted.
it('keeps the cursor on a submission once it shows as its accepted Run', async () => {
  const a = task('a')
  const submission = tool(a.name, 'exec', 0)
  const accepted = {
    ...run(a.name, 'accepted', 0),
    submission: { ...submission, intent: 'Run accepted' },
  }
  const records = (child: ActivityRow) => ({
    rows: [a, task('b')],
    branches: { [a.name]: branch([child, tool(a.name, '1', 1)]) },
  })
  const { user, publish } = renderActivity({ ...records(submission), entry: expand(a.name) })
  await user.keyboard('{ArrowDown}{ArrowDown}')
  expect(cursor()).toBe('Tool exec')
  await publish(records(accepted))
  expect(cursor()).toBe('Run accepted')
})

// Catches: a cursor that silently moves to a neighbour of a record that left, or
// stays actionable on history that access loss concealed.
it.each<[string, Records]>([
  ['the record leaves the loaded list', { rows: [task('a'), task('c')] }],
  ['access loss conceals the list', { rows: [], listUnavailable: true }],
])('clears the cursor when %s', async (_, records) => {
  const { user, publish } = renderActivity({ rows: [task('a'), task('b'), task('c')] })
  await user.keyboard('{ArrowDown}{ArrowDown}')
  expect(cursor()).toBe('Task b')
  await publish(records)
  expect(cursor()).toBeNull()
  await publish({ rows: [task('a'), task('b'), task('c')] })
  expect(cursor()).toBeNull()
  // With no cursor, ↓ starts again at the first record.
  await user.keyboard('{ArrowDown}')
  expect(cursor()).toBe('Task a')
})

it('Enter opens the cursor record’s detail', async () => {
  const b = task('b')
  const { user, router } = renderActivity({ rows: [task('a'), b] })
  await user.keyboard('{ArrowUp}{Enter}')
  expect(recordIn(router)).toBe(b.name)
  expect(await screen.findByRole('dialog', { name: 'Activity details' })).toHaveTextContent(
    'Task b',
  )
})

// Catches (#712): Enter on a focused button opening the cursor's detail instead
// of activating the button.
it('Enter on a focused button activates it, not the cursor record', async () => {
  const [a, b] = [task('a'), task('b')]
  const { user, router } = renderActivity({
    rows: [a, b],
    branches: { [b.name]: branch([tool(b.name, '0', 0)]) },
  })
  await user.keyboard('{ArrowDown}')
  screen.getAllByRole('button', { name: 'Expand record' })[1]!.focus()
  await user.keyboard('{Enter}')
  expect(expandedIn(router)).toEqual([b.name])
  expect(recordIn(router)).toBeNull()
  expect(cursor()).toBe('Task a')
})

// Catches (#712): the focused table, which scrolls and so takes a tabindex,
// keeping Space and Enter as if it were a control in a row.
it('keys act on the cursor record while the table has focus', async () => {
  const a = task('a')
  const { user, router } = renderActivity({
    rows: [a],
    branches: { [a.name]: branch([tool(a.name, '0', 0)]) },
  })
  screen.getByRole('table', { name: 'Activity records' }).focus()
  await user.keyboard('{ArrowDown} ')
  expect(expandedIn(router)).toEqual([a.name])
  await user.keyboard('{Enter}')
  expect(recordIn(router)).toBe(a.name)
})

const details = () => screen.getByRole('dialog', { name: 'Activity details' })

// Catches: a detail that reopens (and refocuses) per record, one history entry
// per arrow, a review selection carried to another record, or Space expanding
// a branch from inside the detail.
it('↑ and ↓ step the open detail through the table, replacing the URL', async () => {
  const [a, b, c] = [task('a'), task('b'), task('c')]
  const { user, router } = renderActivity({
    rows: [a, b, c],
    branches: { [b.name]: branch([tool(b.name, '0', 0)]) },
    entry: `/activity?record=${a.name}&request=r&actionsAfter=next`,
  })
  const dialog = await screen.findByRole('dialog', { name: 'Activity details' })
  await user.keyboard('{ArrowDown}')
  expect(router.state.historyAction).toBe('REPLACE')
  const query = new URLSearchParams(router.state.location.search)
  expect(query.get('record')).toBe(b.name)
  expect(query.has('request')).toBe(false)
  expect(query.has('actionsAfter')).toBe(false)
  expect(details()).toBe(dialog)
  expect(dialog).toHaveTextContent('Task b')
  await user.keyboard(' {ArrowDown}{ArrowDown}')
  expect(expandedIn(router)).toEqual([])
  expect(recordIn(router)).toBe(c.name)
  await user.keyboard('{ArrowUp}')
  expect(recordIn(router)).toBe(b.name)
})

// Catches: Escape losing the table's focus, or the cursor going back to where
// it was before the detail stepped.
it('Escape closes the detail and leaves the cursor on the last record it showed', async () => {
  const [a, b] = [task('a'), task('b')]
  const { user, router } = renderActivity({ rows: [a, b] })
  await user.keyboard('{ArrowDown}{Enter}')
  await screen.findByRole('dialog', { name: 'Activity details' })
  await user.keyboard('{ArrowDown}{Escape}')
  await vi.waitFor(() => expect(recordIn(router)).toBeNull())
  // The URL changes before React commits the closed detail, so wait for the table.
  await vi.waitFor(() => {
    expect(cursor()).toBe('Task b')
    expect(screen.getByRole('table', { name: 'Activity records' })).toHaveFocus()
  })
  await user.keyboard('{ArrowUp}')
  expect(cursor()).toBe('Task a')
})

/** Task a holds Runs one and two, each ending in its final value; Task b follows. */
function finalValues(expanded: boolean) {
  const a = task('a')
  const [one, two] = [run(a.name, 'one', 0), run(a.name, 'two', 1)]
  const shown = expanded ? [a.name, one.name, two.name] : [a.name, one.name]
  const rendered = renderActivity({
    rows: [a, task('b')],
    entry: `${expand(...shown)}&record=${final(two.name).name}`,
    branches: {
      [a.name]: branch([one, two]),
      [one.name]: branch([output(one.name, 0, 'emission one'), final(one.name)]),
      [two.name]: branch([output(two.name, 0, 'emission two'), final(two.name)]),
    },
  })
  return { ...rendered, one, two }
}

// Catches: a final value's detail stepping from its Run's heading, because the
// loader selects the Run for it: ↓ wrapped to the Run's first record and ↑
// skipped the Run's own records. In a collapsed Run the final value is not a
// row, so the detail steps from the Run's own row instead of not at all.
it.each<[string, boolean, string, (runs: { one: string; two: string }) => string]>([
  ['↓ goes to the record after the Run', true, '{ArrowDown}', () => task('b').name],
  ['↑ goes to the Run’s previous record', true, '{ArrowUp}', ({ two }) => `${two}/events/2`],
  ['in a collapsed Run, ↓ goes to the record after it', false, '{ArrowDown}', () => task('b').name],
  [
    'in a collapsed Run, ↑ goes to the record before it',
    false,
    '{ArrowUp}',
    ({ one }) => `${one}/final`,
  ],
])('from a Run’s final value, %s', async (_, expanded, key, expected) => {
  const { user, router, one, two } = finalValues(expanded)
  await screen.findByRole('dialog', { name: 'Activity details' })
  await user.keyboard(key)
  expect(recordIn(router)).toBe(expected({ one: one.name, two: two.name }))
})

// Catches: an expanded Run standing in for a final value its window has not
// loaded, which steps from the Run heading back to the Run's first record.
it('an expanded Run never stands in for a final value its window lacks', async () => {
  const a = task('a')
  const two = run(a.name, 'two', 0)
  const records = (loaded: ActivityRow[]) => ({
    rows: [a, task('b')],
    branches: { [a.name]: branch([two]), [two.name]: branch(loaded, 2) },
  })
  const { user, router, publish } = renderActivity({
    ...records([output(two.name, 0, 'emission two')]),
    entry: `${expand(a.name, two.name)}&record=${final(two.name).name}`,
  })
  await screen.findByRole('dialog', { name: 'Activity details' })
  await user.keyboard('{ArrowDown}')
  expect(recordIn(router)).toBe(final(two.name).name)
  // Once its row arrives, the final value steps on to the record after the Run.
  await publish(records([output(two.name, 0, 'emission two'), final(two.name)]))
  await user.keyboard('{ArrowDown}')
  expect(recordIn(router)).toBe(task('b').name)
})

// Catches: a cursor that Escape leaves on the Run heading rather than on its final
// value, or clears when a collapsed Run shows no final-value row.
it.each<[string, boolean, string, string]>([
  ['on the final value', true, 'final value', 'emission two'],
  ['on a collapsed Run’s own row', false, 'Run two', 'final value'],
])('Escape from a Run’s final value leaves the cursor %s', async (_, expanded, at, above) => {
  const { user, router } = finalValues(expanded)
  await screen.findByRole('dialog', { name: 'Activity details' })
  await user.keyboard('{Escape}')
  await vi.waitFor(() => expect(recordIn(router)).toBeNull())
  await vi.waitFor(() => expect(cursor()).toBe(at))
  await user.keyboard('{ArrowUp}')
  expect(cursor()).toBe(above)
})

// Catches: a cursor that follows the newly shown record but is then cleared in the
// same render, because the record it left also left the loaded window.
it('the cursor follows the detail even when the record it left leaves the table', async () => {
  const a = task('a')
  const b = task('b')
  const { router, user } = renderActivity({
    rows: [a, b],
    entry: `${expand(a.name)}&record=${tool(a.name, '0').name}`,
    branches: { [a.name]: branch([tool(a.name, '0', 0)]) },
  })
  await screen.findByRole('dialog', { name: 'Activity details' })
  // One navigation shows Task b and drops Task a's loaded children.
  await act(() => router.navigate(`/activity?record=${b.name}`, { replace: true }))
  await user.keyboard('{Escape}')
  await vi.waitFor(() => expect(recordIn(router)).toBeNull())
  await vi.waitFor(() => expect(cursor()).toBe('Task b'))
})

// The approval review's unsent comment guards stepping to another record.
async function draftOnApproval() {
  const b = task('b')
  const rendered = renderActivity({
    rows: [waiting, b],
    entry: `/activity?record=${waiting.name}`,
    details: {
      [waiting.name]: {
        kind: 'run',
        run: waiting,
        request: approvalRequest,
        source: 'return null;',
        linkedFunctions: [],
        terminal: { kind: 'unavailable' },
      },
    },
  })
  const comment = await screen.findByRole('textbox', { name: 'Comment (optional)' })
  await rendered.user.type(comment, 'Keep this draft')
  // The field keeps its own arrows; a button hands them to the detail.
  await rendered.user.keyboard('{ArrowDown}')
  expect(recordIn(rendered.router)).toBe(waiting.name)
  screen.getByRole('button', { name: 'Close details' }).focus()
  await rendered.user.keyboard('{ArrowDown}')
  const confirmation = await screen.findByRole('dialog', { name: 'Discard your comment?' })
  return { ...rendered, b, confirmation }
}

it('keeping an unsent comment cancels the step and ignores arrows meanwhile', async () => {
  const { user, router, confirmation } = await draftOnApproval()
  // The confirmation keeps every key: the detail behind it does not step.
  await user.keyboard('{ArrowUp}{ArrowDown}')
  expect(recordIn(router)).toBe(waiting.name)
  await user.click(within(confirmation).getByRole('button', { name: 'Keep editing' }))
  expect(recordIn(router)).toBe(waiting.name)
  expect(screen.getByRole('textbox', { name: 'Comment (optional)' })).toHaveValue('Keep this draft')
})

it('discarding an unsent comment completes the step', async () => {
  const { user, router, b, confirmation } = await draftOnApproval()
  await user.click(within(confirmation).getByRole('button', { name: 'Discard comment' }))
  await vi.waitFor(() => expect(recordIn(router)).toBe(b.name))
  expect(details()).toHaveTextContent('Task b')
})

// Catches: a detail that access loss concealed still stepping to other records.
it('a concealed detail does not step', async () => {
  const [a, b] = [task('a'), task('b')]
  const { user, router, publish } = renderActivity({
    rows: [a, b],
    entry: `/activity?record=${a.name}`,
  })
  await screen.findByRole('dialog', { name: 'Activity details' })
  await publish({ rows: [], listUnavailable: true, unavailable: true })
  expect(details()).toHaveTextContent('This history is unavailable.')
  await user.keyboard('{ArrowDown}')
  expect(recordIn(router)).toBe(a.name)
})

// Catches: a cursor on records the full table does not list as its own: a
// glimpse's roots, pending actions, or the selected record outside the page.
const outside = task('outside')
it.each<[string, Records & { entry?: string } & Pick<ActivityViewProps, 'more'>]>([
  [
    'a glimpse',
    { rows: [task('a')], more: { to: '/w/activity', label: 'View all activity', roots: 1 } },
  ],
  [
    'a selected record outside the page',
    { rows: [], selectedRow: outside, entry: `/activity?record=${outside.name}` },
  ],
  [
    'a pending action',
    {
      rows: [],
      pending: {
        actions: [{ action: approval, run: waiting }],
        actionCount: '1',
        next: '',
        unavailable: false,
      },
    },
  ],
])('%s takes no keyboard cursor', async (_, records) => {
  const { user, router } = renderActivity(records)
  const before = recordIn(router)
  await user.keyboard('{ArrowDown}{ArrowUp}{Enter}')
  expect(document.querySelector('[data-active]')).toBeNull()
  expect(recordIn(router)).toBe(before)
})
