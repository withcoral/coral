import { expect, it } from 'vitest'
import { activityPage, workspace } from '@/views/activity/fixtures'
import { ACTIVITY_CHILD_WINDOW } from './activity-live-types'
import { call, coral, runRow, task, toolCall, type Row } from './activity-trace.test-support'
import {
  absorbPage,
  coveredBy,
  markStale,
  missingWindows,
  nextTraceRequest,
  openTrace,
  receiveTrace,
  revalidateTrace,
  traceChildren,
  traceProgress,
  traceQuery,
  traceReadable,
  TRACE_WINDOWS_PER_REQUEST,
  type ActivityTraceStore,
  type TracePreference,
} from './activity-trace'

/** An object that holds nothing. `toMatchObject` takes `{}` for any object. */
const none = expect.toSatisfy((value: object) => Object.keys(value).length === 0, 'holds nothing')

/** The Run windows `source` was asked for, as "run@offset", in the order it was asked. */
const windowsRead = (source: ReturnType<typeof coral>, root: string, from = 0) =>
  source.calls.slice(from).flatMap((query) =>
    query
      .getAll('expand')
      .filter((parent) => parent !== root)
      .map((parent) => `${parent}@${query.get(`offset.${parent}`)}`),
  )

/** Reads until the trace asks for nothing more. Fails rather than looping forever. */
function drain(
  store: ActivityTraceStore,
  source: ReturnType<typeof coral>,
  prefer: TracePreference[] = [],
  limit = 200,
) {
  for (let count = 0; count < limit; count++) {
    const request = nextTraceRequest(store, prefer)
    if (!request) return store
    store = receiveTrace(store, request, source.load(traceQuery(store, request)))
  }
  throw new Error('The trace never stopped reading.')
}

it('reads every window of a root and its runs, however many pages they take', () => {
  const [a, b, c] = [runRow('a'), runRow('b'), runRow('c')]
  const source = coral({
    [task.name]: [a, b, c],
    [a.name]: Array.from({ length: 250 }, (_, index) => call(a, index)),
    [b.name]: [call(b, 0)],
    [c.name]: [],
  })
  const store = drain(openTrace(task.name), source)
  expect(traceChildren(store, a.name)).toHaveLength(250)
  expect(traceChildren(store, a.name).map((row) => row.position)).toEqual(
    Array.from({ length: 250 }, (_, index) => index),
  )
  expect(traceProgress(store)).toEqual({
    complete: true,
    final: true,
    unavailable: false,
    settling: false,
    stale: false,
    loaded: 254,
    total: 254,
  })
  // The cost: each window of each run is read once, and every request reads the root's.
  expect(windowsRead(source, task.name).toSorted()).toEqual(
    [`${a.name}@0`, `${a.name}@100`, `${a.name}@200`, `${b.name}@0`, `${c.name}@0`].toSorted(),
  )
  // One request for the root, then one for each window of the longest run.
  expect(source.calls).toHaveLength(1 + Math.ceil(250 / ACTIVITY_CHILD_WINDOW))
  expect(source.calls.every((query) => query.getAll('expand')[0] === task.name)).toBe(true)
  expect(source.calls.every((query) => query.get('anchor') === task.name)).toBe(true)
})

// Catches: a Task header without the Task's feedback when a link opened a record under the
// Task, more than one read of the Task's detail, or a read of it the trace could share.
it.each([
  ['with windows left to read, on a read it makes anyway', 250, 0],
  ['with every window held, in one read of its own', 3, 1],
])('reads the root Task\u2019s own detail once %s', (_, calls, extra) => {
  const a = runRow('a')
  const children = {
    [task.name]: [a],
    [a.name]: Array.from({ length: calls }, (_row, index) => call(a, index)),
  }
  const feedback = 'Found 3 invoices.'
  const source = coral(children, task, feedback)
  const without = coral(children, task)
  const store = drain(openTrace(task.name), source)
  drain(openTrace(task.name), without)
  expect(store.taskDetail).toEqual({ feedback })
  // One read selects the Task, and it is the only one.
  const selecting = source.calls.filter((query) => query.get('record') === task.name)
  expect(selecting).toHaveLength(1)
  expect(source.calls.filter((query) => query.has('record'))).toHaveLength(1)
  // Beside the reads of every window, at most one read more, only for the Task's detail.
  expect(source.calls.length - (without.calls.length - extra)).toBe(extra)
  expect(nextTraceRequest(store)).toBeNull()
})

it('takes the root Task\u2019s feedback from a page that selects the Task, and reads none', () => {
  const a = runRow('a')
  const source = coral({ [task.name]: [a], [a.name]: [] }, task, 'Done.')
  // The page you opened selects the Task, so it holds the Task's detail already.
  const opened = absorbPage(
    openTrace(task.name),
    source.load(
      new URLSearchParams({ tree: '1', anchor: task.name, record: task.name, expand: task.name }),
    ),
  )
  expect(opened.taskDetail).toEqual({ feedback: 'Done.' })
  const before = source.calls.length
  drain(opened, source)
  expect(source.calls.slice(before).filter((query) => query.has('record'))).toHaveLength(0)
})

it('keeps no feedback for a Task that has none, and asks no more', () => {
  const a = runRow('a')
  const source = coral({ [task.name]: [a], [a.name]: [] })
  const store = drain(openTrace(task.name), source)
  expect(store.taskDetail).toEqual({})
  expect(source.calls.filter((query) => query.has('record'))).toHaveLength(1)
})

it('reads the runs of a root in batches, never more windows than one request may', () => {
  const runs = Array.from({ length: 23 }, (_, index) => runRow(`r${index}`))
  const source = coral({
    [task.name]: runs,
    ...Object.fromEntries(runs.map((run) => [run.name, [call(run, 0)]])),
  })
  const store = drain(openTrace(task.name), source)
  expect(traceProgress(store).complete).toBe(true)
  for (const query of source.calls)
    expect(query.getAll('expand').length).toBeLessThanOrEqual(TRACE_WINDOWS_PER_REQUEST + 1)
  expect(source.calls).toHaveLength(1 + Math.ceil(23 / TRACE_WINDOWS_PER_REQUEST))
})

it('says a run whose children Coral withholds is unavailable, and stops asking', () => {
  const [a, b] = [runRow('a'), runRow('b')]
  const source = coral({
    [task.name]: [a, b],
    [a.name]: 'unavailable',
    [b.name]: [call(b, 0)],
  })
  const store = drain(openTrace(task.name), source)
  expect(store.branches[a.name]).toMatchObject({ unavailable: true })
  expect(traceProgress(store)).toMatchObject({ complete: false, unavailable: true })
  // The readable run still loaded.
  expect(traceChildren(store, b.name)).toHaveLength(1)
})

it('keeps what it read and stops when a window comes back empty', () => {
  const a = runRow('a')
  const rows = Array.from({ length: 150 }, (_, index) => call(a, index))
  const source = coral({ [task.name]: [a], [a.name]: rows })
  const load = source.load
  // Coral counts 150 children but returns none past the first window.
  source.load = (query) => {
    const page = load(query)
    const branch = page.branches[a.name]
    if (branch && (branch.offset ?? 0) >= 100) branch.rows = []
    return page
  }
  const store = drain(openTrace(task.name), source)
  expect(traceChildren(store, a.name)).toHaveLength(100)
  expect(traceProgress(store)).toMatchObject({ complete: false, unavailable: true })
})

it('stops after a failed read and continues after a retry', () => {
  const a = runRow('a')
  const source = coral({ [task.name]: [a], [a.name]: [call(a, 0)] })
  let store = openTrace(task.name)
  store = receiveTrace(store, nextTraceRequest(store)!, null)
  expect(store.failed).toBe(true)
  expect(nextTraceRequest(store)).toBeNull()
  store = drain({ ...store, failed: false }, source)
  expect(traceProgress(store).complete).toBe(true)
})

it('says the root is unavailable when Coral does not return it', () => {
  const source = coral({})
  source.load = () => activityPage({ rows: [], anchorUnavailable: true })
  const store = drain(openTrace(task.name), source)
  expect(store.unavailable).toBe(true)
  expect(traceProgress(store)).toMatchObject({ complete: false, unavailable: true })
})

it('reads only what later evidence can change, not the whole history again', () => {
  const done = runRow('done')
  const live = runRow('live', 'running')
  const open: Row<'task'> = { ...task, outcome: 'open', endedAt: null }
  const entries = Array.from({ length: 230 }, (_, index) => call(live, index, index < 229))
  const source = coral(
    {
      [open.name]: [done, live],
      [done.name]: Array.from({ length: 120 }, (_, index) => call(done, index)),
      [live.name]: entries,
    },
    open,
  )
  let store = drain(openTrace(open.name), source)
  expect(traceProgress(store)).toMatchObject({ complete: true, final: false })
  expect(store.branches[done.name]!.final).toBe(true)
  const before = source.calls.length

  // The pending call ends and two more start.
  entries[229] = call(live, 229)
  entries.push(call(live, 230), call(live, 231, false))
  store = drain(markStale(store), source)
  expect(traceChildren(store, live.name)).toHaveLength(232)
  expect(traceChildren(store, live.name)[229]).toMatchObject({ state: 'ended' })
  // The measured cost of one live update: one request, with the root's last window and
  // the running run's tail. The ended run is not read again.
  expect(source.calls.length - before).toBe(1)
  expect(source.calls.at(-1)!.getAll('expand')).toEqual([open.name, live.name])
  expect(source.calls.at(-1)!.get(`offset.${live.name}`)).toBe('229')
})

it('reads nothing more for a live update the list itself already holds', () => {
  const live = runRow('live', 'running')
  const open: Row<'task'> = { ...task, outcome: 'open', endedAt: null }
  const entries = [call(live, 0), call(live, 1, false)]
  const source = coral({ [open.name]: [live], [live.name]: entries }, open)
  let store = drain(openTrace(open.name), source)
  entries[1] = call(live, 1)
  // A live snapshot of the expanded records, as the Activity list receives it.
  const snapshot = source.load(
    new URLSearchParams([
      ['expand', open.name],
      ['expand', live.name],
    ]),
  )
  const covered = coveredBy(store, snapshot)
  store = markStale(absorbPage(store, snapshot), covered)
  expect([...covered]).toEqual([open.name, live.name])
  expect(traceChildren(store, live.name)[1]).toMatchObject({ state: 'ended' })
  expect(nextTraceRequest(store)).toBeNull()
})

it('returns the same store when a page adds nothing', () => {
  const a = runRow('a')
  const source = coral({ [task.name]: [a], [a.name]: [call(a, 0)] })
  const store = drain(openTrace(task.name), source)
  const again = source.load(
    new URLSearchParams([
      ['expand', task.name],
      ['expand', a.name],
    ]),
  )
  expect(absorbPage(store, again)).toBe(store)
  expect(markStale(store)).toBe(store)
})

it('ignores the windows of another root', () => {
  const other = { ...runRow('other'), task: `${workspace}/tasks/other` }
  const store = absorbPage(
    openTrace(task.name),
    activityPage({
      rows: [task],
      branches: {
        [other.name]: {
          rows: [{ ...call(other, 0), task: other.task }],
          next: '',
          unavailable: false,
        },
      },
    }),
  )
  expect(Object.keys(store.branches)).toEqual([])
})

it('finds the windows no read returned yet', () => {
  expect(
    missingWindows({
      total: 6,
      rows: { 1: call(runRow('a'), 1), 2: call(runRow('a'), 2), 5: call(runRow('a'), 5) },
      unavailable: false,
      final: false,
    }),
  ).toEqual([
    { from: 0, to: 1 },
    { from: 3, to: 5 },
  ])
})

it('reads again only the windows that can change, not everything after a call that waits', () => {
  const live = runRow('live', 'running')
  const open: Row<'task'> = { ...task, outcome: 'open', endedAt: null }
  // The sixth call waits, as for an approval, while 1,000 later ones ended.
  const entries = Array.from({ length: 1000 }, (_, index) => call(live, index, index !== 5))
  const source = coral({ [open.name]: [live], [live.name]: entries }, open)
  let store = drain(openTrace(open.name), source)
  const before = source.calls.length

  entries.push(call(live, 1000))
  store = drain(markStale(store), source)
  expect(traceChildren(store, live.name)).toHaveLength(1001)
  // The waiting call's window and the end: two windows, whatever lies between them.
  expect(windowsRead(source, open.name, before)).toEqual([`${live.name}@5`, `${live.name}@1000`])
})

it('reads a run once more after it ends, then never again', () => {
  const live = runRow('live', 'running')
  const open: Row<'task'> = { ...task, outcome: 'open', endedAt: null }
  const entries = [call(live, 0), call(live, 1, false)]
  const children = { [open.name]: [live as Row<'run'>], [live.name]: entries }
  const source = coral(children, open)
  let store = drain(openTrace(open.name), source)
  expect(store.branches[live.name]!.final).toBe(false)

  children[open.name] = [runRow('live')]
  entries[1] = call(live, 1)
  store = drain(markStale(store), source)
  expect(store.branches[live.name]!.final).toBe(true)
  expect(traceChildren(store, live.name)[1]).toMatchObject({ state: 'ended' })
  const before = source.calls.length
  store = drain(markStale(store), source)
  expect(windowsRead(source, open.name, before)).toEqual([])
})

it('drops the records of a run that Coral later withholds', () => {
  const [a, b] = [runRow('a', 'running'), runRow('b')]
  const children: Parameters<typeof coral>[0] = {
    [task.name]: [a, b],
    [a.name]: [call(a, 0), call(a, 1, false)],
    [b.name]: [call(b, 0)],
  }
  const source = coral(children)
  let store = drain(openTrace(task.name), source)
  expect(traceChildren(store, a.name)).toHaveLength(2)

  // You lose access to the run, or Coral no longer retains it.
  children[a.name] = 'unavailable'
  store = drain(markStale(store), source)
  expect(traceChildren(store, a.name)).toEqual([])
  expect(traceProgress(store)).toMatchObject({ complete: false, unavailable: true })
  // The run you can still read stays.
  expect(traceChildren(store, b.name)).toHaveLength(1)
})

it('drops every record when Coral no longer returns the root', () => {
  const a = runRow('a', 'running')
  const source = coral({ [task.name]: [a], [a.name]: [call(a, 0)] })
  let store = drain(openTrace(task.name), source)
  expect(store.rootRow).not.toBeNull()

  source.load = () => activityPage({ rows: [], anchorUnavailable: true })
  store = drain(markStale(store), source)
  expect(store).toMatchObject({ unavailable: true, rootRow: null, branches: none })
})

// Catches: a denied list read as a passing failure, which keeps the records on screen
// with "Try again".
it('drops every record when Coral denies the list, which is no failed read', () => {
  const a = runRow('a', 'running')
  const source = coral({ [task.name]: [a], [a.name]: [call(a, 0)] })
  let store = drain(openTrace(task.name), source)

  source.load = () => activityPage({ rows: [], listUnavailable: true })
  store = drain(markStale(store), source)
  expect(store).toMatchObject({ unavailable: true, failed: false, rootRow: null, branches: none })
})

// Catches: a root that stays unavailable for the whole visit after Coral withheld it once.
it('reads a root from the start when Coral returns it again', () => {
  const a = runRow('a', 'running')
  const source = coral({ [task.name]: [a], [a.name]: [call(a, 0)] })
  const load = source.load
  let store = drain(openTrace(task.name), source)
  source.load = () => activityPage({ rows: [], anchorUnavailable: true })
  store = drain(markStale(store), source)
  expect(nextTraceRequest(store)).toBeNull()

  // The list holds the root again. Nothing from before the loss shows until a read returns it.
  source.load = load
  store = absorbPage(store, activityPage({ rows: [task] }))
  expect(store).toMatchObject({ unavailable: false, branches: none })
  store = drain(store, source)
  expect(traceChildren(store, a.name)).toHaveLength(1)
  expect(traceProgress(store)).toMatchObject({ complete: true, unavailable: false })
})

it('keeps what it read when a later read fails, which is no loss of access', () => {
  const a = runRow('a', 'running')
  const source = coral({ [task.name]: [a], [a.name]: [call(a, 0)] })
  let store = drain(openTrace(task.name), source)
  store = markStale(store)
  store = receiveTrace(store, nextTraceRequest(store)!, null)
  expect(store.failed).toBe(true)
  expect(traceChildren(store, a.name)).toHaveLength(1)
})

it('reads the window that holds the ordinal you need before the rest of a long run', () => {
  const a = runRow('a')
  const source = coral({
    [task.name]: [a],
    [a.name]: Array.from({ length: 1000 }, (_, index) => call(a, index)),
  })
  let store = openTrace(task.name)
  // The root, then the first window of the run.
  for (let count = 0; count < 2; count++) {
    const request = nextTraceRequest(store)!
    store = receiveTrace(store, request, source.load(traceQuery(store, request)))
  }
  const read = (prefer: TracePreference[]) => {
    const request = nextTraceRequest(store, prefer)!
    store = receiveTrace(store, request, source.load(traceQuery(store, request)))
    return source.calls.at(-1)!.get(`offset.${a.name}`)
  }
  // Going down to ordinal 600 reads from it. Going up to 599 reads the window that ends there.
  expect(read([{ parent: a.name, at: 600 }])).toBe('600')
  expect(read([{ parent: a.name, at: 599 }])).toBe('500')
  // A preferred ordinal the trace holds changes nothing: the first gap is next.
  expect(read([{ parent: a.name, at: 650 }])).toBe('100')
  expect(traceProgress(drain(store, source)).complete).toBe(true)
})

it('counts a window by its ordinals when the loader adds a run from outside it', () => {
  const runs = Array.from({ length: 150 }, (_, index) => runRow(`r${index}`, 'running'))
  const open: Row<'task'> = { ...task, outcome: 'open', endedAt: null }
  const positioned = runs.map((run, position) => ({ ...run, position }))
  // The task's second window, with the run at ordinal 3 added as the loader adds it.
  const page = activityPage({
    rows: [open],
    branches: {
      [open.name]: {
        offset: 100,
        total: '150',
        rows: [positioned[3]!, ...positioned.slice(100)],
        next: '',
        unavailable: false,
      },
    },
  })
  const store = absorbPage(openTrace(open.name), page)
  expect(traceChildren(store, open.name).map((row) => row.position)).toEqual([
    3,
    ...Array.from({ length: 50 }, (_, index) => 100 + index),
  ])
  // The added run fills its own ordinal only. Every other ordinal before the window is missing.
  expect(missingWindows(store.branches[open.name])).toEqual([
    { from: 0, to: 3 },
    { from: 4, to: 100 },
  ])
  // The window holds the end, though it has one row more than its ordinals.
  expect([...coveredBy(markStale(store), page)]).toEqual([])
})

it('never lets an older snapshot change a branch that is final', () => {
  const a = runRow('a')
  const source = coral({ [task.name]: [a], [a.name]: [call(a, 0)] })
  const store = drain(openTrace(task.name), source)
  const older = activityPage({
    rows: [task],
    branches: {
      [a.name]: {
        offset: 0,
        total: '1',
        rows: [{ ...call(a, 0, false), position: 0 }],
        next: '',
        unavailable: false,
      },
    },
  })
  // The snapshot changes no record. It disagrees, so the trace asks Coral itself.
  const doubted = absorbPage(store, older)
  expect(traceChildren(doubted, a.name)).toEqual(traceChildren(store, a.name))
  expect(doubted.branches[a.name]).toMatchObject({ final: true, stale: [0] })
  expect(traceProgress(doubted)).toMatchObject({ complete: true, final: true, stale: true })
  // Coral's answer is the same record: the branch is as it was, with one read.
  const before = source.calls.length
  expect(drain(doubted, source).branches[a.name]).toEqual(store.branches[a.name])
  expect(source.calls.length).toBe(before + 1)
  // A snapshot that agrees asks for nothing.
  expect(
    absorbPage(store, source.load(traceQuery(store, { windows: [{ parent: a.name, offset: 0 }] }))),
  ).toBe(store)
  // A read that counts one more child is no older snapshot: the branch reads on.
  const grown = absorbPage(store, {
    ...older,
    branches: { [a.name]: { ...older.branches[a.name]!, total: '2' } },
  })
  expect(missingWindows(grown.branches[a.name])).toEqual([{ from: 1, to: 2 }])
  expect(traceProgress(grown)).toMatchObject({ complete: false, final: false })
})

// Catches: a waterfall without the calls a tool's body made, and a request for them that
// names no window with the tool call in it, which the loader leaves unanswered.
it('reads the calls a tool’s body made, each in a request that shows the tool call', () => {
  const a = runRow('a')
  const outer = toolCall(a, 'outer')
  const inner = toolCall(a, 'inner', outer)
  // The tool call is past the Run's first window.
  const own = [...Array.from({ length: 150 }, (_, index) => call(a, index)), outer]
  const source = coral({
    [task.name]: [a],
    [a.name]: own,
    [outer.name]: [{ ...call(a, 900), parent: inner.parent }, inner],
    [inner.name]: [{ ...call(a, 901), parent: { ...inner.parent!, invocation: inner.name } }],
  })
  const store = drain(openTrace(task.name), source)
  expect(traceChildren(store, outer.name).map((row) => row.name)).toEqual([
    call(a, 900).name,
    inner.name,
  ])
  expect(traceChildren(store, inner.name).map((row) => row.name)).toEqual([call(a, 901).name])
  expect(traceProgress(store)).toMatchObject({ complete: true, final: true, loaded: 155 })
  // A tool call's window comes with the window that shows the call, up to its Run's.
  const asked = (name: string) =>
    source.calls
      .filter((query) => query.getAll('expand').includes(name))
      .map((query) => [query.get(`offset.${a.name}`), query.get(`offset.${outer.name}`)])
  expect(asked(outer.name)).toEqual([
    ['150', '0'],
    ['150', '1'],
  ])
  expect(asked(inner.name)).toEqual([['150', '1']])
})

// Catches: a tool call that still runs read as settled, so the calls its body makes later
// never show.
it('reads a tool call’s calls again while the call is in progress', () => {
  const live = runRow('live', 'running')
  const outer = toolCall(live, 'outer', undefined, false)
  const made = [{ ...call(live, 900), parent: toolCall(live, 'x', outer).parent }]
  const children = { [task.name]: [live as Row<'run'>], [live.name]: [outer], [outer.name]: made }
  const source = coral(children)
  let store = drain(openTrace(task.name), source)
  expect(store.branches[outer.name]).toMatchObject({ final: false })

  made.push({ ...call(live, 901), parent: made[0]!.parent })
  children[live.name] = [toolCall(live, 'outer')]
  store = drain(markStale(store), source)
  expect(traceChildren(store, outer.name)).toHaveLength(2)
  expect(store.branches[outer.name]).toMatchObject({ final: true })
})

/** A Run whose Program calls a tool, whose body calls a tool, and so on, `depth` calls deep. */
function chain(depth: number) {
  const run = runRow('deep')
  const calls: Row<'invocation'>[] = []
  for (let index = 0; index < depth; index++)
    calls.push(toolCall(run, `deep-${index}`, calls.at(-1)))
  const children: Record<string, Row<'invocation'>[] | Row<'run'>[]> = {
    [task.name]: [run],
    [run.name]: [calls[0]!],
  }
  calls.forEach((made, index) => (children[made.name] = calls.slice(index + 1, index + 2)))
  return { run, calls, source: coral(children) }
}

// Catches: a tool call nested deeper than one batch of windows asked for again and again
// with no room for its own window, so the trace never finished. The loader expands at
// most 20 records for a request: the root, the Run and 18 calls.
it.each([
  ['more calls deep than one batch of windows holds', TRACE_WINDOWS_PER_REQUEST + 2],
  ['as many calls deep as one request can open', 18],
])('reads a chain of tool calls %s', (_, depth) => {
  const { calls, source } = chain(depth)
  const store = drain(openTrace(task.name), source)
  expect(traceProgress(store)).toMatchObject({ complete: true, final: true, unavailable: false })
  expect(calls.every((made) => store.branches[made.name])).toBe(true)
  // Every read moved on: none asked for what the one before it asked for.
  const asked = source.calls.map(String)
  expect(new Set(asked).size).toBe(asked.length)
})

// Catches: the same loop one call deeper, and a request the loader refuses.
it('says the calls of a tool call nested deeper than a request can open are unavailable', () => {
  const { calls, source } = chain(19)
  const store = drain(openTrace(task.name), source)
  const deepest = calls.at(-1)!
  expect(traceReadable(store, calls.at(-2)!.name)).toBe(true)
  expect(traceReadable(store, deepest.name)).toBe(false)
  // The call itself shows: only what its body made is out of reach.
  expect(traceChildren(store, calls.at(-2)!.name)).toEqual([expect.objectContaining(deepest)])
  expect(store.branches[deepest.name]).toBeUndefined()
  expect(traceProgress(store)).toMatchObject({ complete: false, unavailable: true })
  expect(nextTraceRequest(store)).toBeNull()
})

// Catches: the calls under a Run or a tool call that Coral withheld showing again, unread,
// the moment Coral returns that Run or call. This is the lost root, one and two levels down.
it.each([
  ['a run', (run: Row<'run'>) => run.name],
  ['a tool call', (_: Row<'run'>, outer: Row<'invocation'>) => outer.name],
])(
  'drops everything under %s that Coral withholds, and reads it again on its return',
  (_, pick) => {
    const run = runRow('a', 'running')
    // Both still work, so later evidence reads their windows again.
    const outer = toolCall(run, 'outer', undefined, false)
    const inner = toolCall(run, 'inner', outer)
    const leaf = { ...call(run, 7), parent: toolCall(run, 'x', inner).parent }
    const children: Parameters<typeof coral>[0] = {
      [task.name]: [run],
      [run.name]: [outer],
      [outer.name]: [inner],
      [inner.name]: [leaf],
    }
    const source = coral(children)
    let store = drain(openTrace(task.name), source)
    expect(traceChildren(store, inner.name)).toHaveLength(1)

    const withheld = pick(run, outer)
    const readable = children[withheld]!
    children[withheld] = 'unavailable'
    store = drain(markStale(store), source)
    expect(store.branches[withheld]).toMatchObject({ unavailable: true, rows: none })
    // Nothing under the withheld record stays, at any depth.
    const below = [outer.name, inner.name].filter((name) => name !== withheld)
    expect(below.map((name) => store.branches[name])).toEqual(below.map(() => undefined))
    expect(traceProgress(store)).toMatchObject({ complete: false, unavailable: true })

    // A snapshot of the list returns the record's window again.
    children[withheld] = readable
    const again = new URLSearchParams({ tree: '1' })
    for (const parent of [task.name, run.name, outer.name]) again.append('expand', parent)
    const before = source.calls.length
    store = absorbPage(store, source.load(again))
    // What the snapshot shows is back. The calls under it are not, until a read returns them.
    expect(store.branches[inner.name]).toBeUndefined()
    expect(traceProgress(store)).toMatchObject({ complete: false, unavailable: false })
    expect(nextTraceRequest(store)?.windows.map((window) => window.parent)).toContain(inner.name)
    store = drain(store, source)
    expect(source.calls.length).toBeGreaterThan(before + 1)
    expect(traceChildren(store, inner.name)).toEqual([expect.objectContaining(leaf)])
    expect(traceProgress(store)).toMatchObject({ complete: true, unavailable: false })
  },
)

// Catches: a trace that is final, and so reads nothing more, keeping its records on
// screen after the list's own snapshot says Coral withholds the root's children.
it('drops a final trace when a snapshot of the list withholds its root', () => {
  const run = runRow('a')
  const outer = toolCall(run, 'outer')
  const source = coral({
    [task.name]: [run],
    [run.name]: [outer],
    [outer.name]: [{ ...call(run, 7), parent: toolCall(run, 'x', outer).parent }],
  })
  let store = drain(openTrace(task.name), source)
  expect(traceProgress(store)).toMatchObject({ complete: true, final: true })
  expect(nextTraceRequest(markStale(store))).toBeNull()

  store = absorbPage(
    store,
    activityPage({
      rows: [task],
      branches: { [task.name]: { rows: [], next: '', unavailable: true } },
    }),
  )
  expect(Object.keys(store.branches)).toEqual([task.name])
  expect(store.branches[task.name]).toMatchObject({ unavailable: true, rows: none })
  expect(traceProgress(store)).toMatchObject({ complete: false, unavailable: true })
})

/** A Task of two ended Runs, read in full: every branch is final. */
function settled() {
  const [a, b] = [runRow('a'), runRow('b')]
  const outer = toolCall(a, 'outer')
  const children: Parameters<typeof coral>[0] = {
    [task.name]: [a, b],
    [a.name]: [outer, ...Array.from({ length: 120 }, (_, index) => call(a, index + 1))],
    [outer.name]: [{ ...call(a, 900), parent: toolCall(a, 'x', outer).parent }],
    [b.name]: [call(b, 0)],
  }
  const source = coral(children)
  const store = drain(openTrace(task.name), source)
  expect(traceProgress(store)).toMatchObject({ complete: true, final: true, stale: false })
  expect(nextTraceRequest(markStale(store))).toBeNull()
  return { a, b, outer, children, source, store }
}

// Catches: a trace that reads a record once and then shows it for the whole visit, though
// Coral stopped returning it. Main read every window on screen again with each snapshot.
it('reads every final window once more when asked to revalidate, and changes nothing that Coral still returns', () => {
  const { a, b, outer, source, store } = settled()
  const before = source.calls.length
  const marked = revalidateTrace(store)
  // The records stay on screen, and final, while the reads run.
  expect(traceChildren(marked, a.name)).toHaveLength(121)
  expect(traceProgress(marked)).toMatchObject({ complete: true, final: true, stale: true })
  const again = drain(marked, source)
  expect(again.branches).toEqual(store.branches)
  expect(traceProgress(again)).toMatchObject({ complete: true, final: true, stale: false })
  // Every window once: the Task's, the long Run's two, the other Run's and the tool call's.
  const read = windowsRead(source, task.name, before)
  expect(new Set(read)).toEqual(
    new Set([`${a.name}@0`, `${a.name}@100`, `${b.name}@0`, `${outer.name}@0`]),
  )
  // Two requests, as for the first read of these windows.
  expect(source.calls.length - before).toBe(2)
})

// Catches: the records of a Run, and of the calls under it, on screen after your access to
// that Run ended, or after retention deleted it, because the trace had read them for good.
it.each([
  ['withholds a run', 'withheld'],
  ['no longer lists a run', 'deleted'],
] as const)('drops what a revalidation finds that Coral %s after its final read', (_, how) => {
  const { a, b, outer, children, source, store } = settled()
  if (how === 'withheld') children[a.name] = 'unavailable'
  else children[task.name] = [b]
  const again = drain(revalidateTrace(store), source)
  expect(store.branches[outer.name]).toBeDefined()
  expect(again.branches[outer.name]).toBeUndefined()
  if (how === 'withheld') {
    expect(again.branches[a.name]).toMatchObject({ unavailable: true, rows: none })
    expect(traceProgress(again)).toMatchObject({ complete: false, unavailable: true })
  } else {
    // The Task holds one Run now, at the first ordinal, and nothing past it.
    expect(again.branches[a.name]).toBeUndefined()
    expect(traceChildren(again, task.name)).toEqual([expect.objectContaining({ name: b.name })])
    expect(again.branches[task.name]).toMatchObject({ total: 1, final: true })
    expect(traceProgress(again)).toMatchObject({ complete: true, final: true, loaded: 2 })
  }
  // The other Run is as it was.
  expect(again.branches[b.name]).toEqual(store.branches[b.name])
})

// Catches: a revalidation that Coral does not answer treated as a denial, or as an answer.
it('keeps the records and stops when a revalidation read fails, and drops the root that Coral then withholds', () => {
  const { a, source, store } = settled()
  const marked = revalidateTrace(store)
  const failed = receiveTrace(marked, nextTraceRequest(marked)!, null)
  expect(failed.failed).toBe(true)
  expect(traceChildren(failed, a.name)).toHaveLength(121)
  expect(nextTraceRequest(failed)).toBeNull()
  // After a retry, Coral no longer returns the root.
  source.load = () => activityPage({ rows: [], anchorUnavailable: true })
  const gone = drain({ ...failed, failed: false }, source)
  expect(gone).toMatchObject({ unavailable: true, rootRow: null, branches: none })
})

const open: Row<'task'> = { ...task, outcome: 'open', endedAt: null }
const names = (rows: { name: string }[]) => rows.map((row) => row.name.split('/').at(-1))

// Catches: an open Task that loses a Run showing the Runs after it twice, with the lost
// Run's records still counted. An open Task's own window is never final, and the trace
// took every read of it as one that only adds children.
// The last case catches: a lost Run noticed only when the count shrinks. Another Run that
// starts in the same interval keeps the count as it was.
it.each([
  ['the first of three runs', 3, 0, 0],
  ['the last of three runs', 3, 2, 0],
  ['a run early in a task of 250', 250, 5, 0],
  ['a run in the last window of a task of 250', 250, 240, 0],
  ['a run early in a task of 250 while another starts', 250, 5, 1],
])('holds exactly the runs Coral lists after an open task loses %s', (_case, count, lost, more) => {
  const runs = Array.from({ length: count }, (_item, index) => runRow(`r${index}`))
  const started = Array.from({ length: more }, (_item, index) => runRow(`new${index}`))
  const children: Parameters<typeof coral>[0] = {
    [open.name]: runs,
    ...Object.fromEntries([...runs, ...started].map((run) => [run.name, [call(run, 0)]])),
  }
  const source = coral(children, open)
  let store = drain(openTrace(open.name), source, [], 400)
  expect(traceChildren(store, open.name)).toHaveLength(count)

  const left = [...runs.filter((_run, index) => index !== lost), ...started]
  children[open.name] = left
  // Later evidence reads the Task's tail again, as for any open Task.
  store = drain(markStale(store), source, [], 400)
  expect(names(traceChildren(store, open.name))).toEqual(names(left))
  expect(store.branches[open.name]!.total).toBe(left.length)
  expect(store.branches[runs[lost]!.name]).toBeUndefined()
  // Each Run that stays has its records, read or kept, and nothing is missing.
  expect(left.every((run) => traceChildren(store, run.name).length === 1)).toBe(true)
  expect(traceProgress(store)).toMatchObject({ complete: true, loaded: left.length * 2 })
})

// Catches: a page that the trace did not ask for taken as proof that records are gone. A
// snapshot can be older than what the trace holds, or hold another window.
it('never drops or moves a record for a page it did not ask for', () => {
  const [a, b, c] = [runRow('a'), runRow('b'), runRow('c')]
  const source = coral({ [open.name]: [a, b, c], [a.name]: [], [b.name]: [], [c.name]: [] }, open)
  const store = drain(openTrace(open.name), source)
  const page = (rows: Row<'run'>[]) =>
    activityPage({
      rows: [open],
      branches: {
        [open.name]: {
          offset: 0,
          total: String(rows.length),
          rows: rows.map((row, position) => ({ ...row, position })),
          next: '',
          unavailable: false,
        },
      },
    })
  // Other records at the ordinals the trace holds, and fewer of them.
  const doubted = absorbPage(store, page([b, c]))
  expect(names(traceChildren(doubted, open.name))).toEqual(['a', 'b', 'c'])
  expect(doubted.branches[open.name]).toMatchObject({ total: 3, stale: [0] })
  // The trace asks Coral itself. Coral still lists all three: nothing changes.
  const asked = drain(doubted, source)
  expect(names(traceChildren(asked, open.name))).toEqual(['a', 'b', 'c'])
  expect(asked.branches[open.name]!.stale).toBeUndefined()
  // A page with fewer records and the same ones first is an older snapshot.
  expect(names(traceChildren(absorbPage(store, page([a, b])), open.name))).toEqual(['a', 'b', 'c'])
  // A read that fails proves nothing either.
  const failed = receiveTrace(doubted, nextTraceRequest(doubted)!, null)
  expect(names(traceChildren(failed, open.name))).toEqual(['a', 'b', 'c'])
})

// Catches: a Run that Coral withheld once staying unavailable for the whole visit after
// Coral returns it again; rows from before shown while the trace asks; and a trace that
// asks again and again for what Coral still withholds.
it('asks once more for what Coral withheld when it revalidates, and shows only what the answer holds', () => {
  const { a, b, children, source, store } = settled()
  const readable = children[a.name]!
  children[a.name] = 'unavailable'
  let current = drain(revalidateTrace(store), source)
  expect(current.branches[a.name]).toMatchObject({ unavailable: true, rows: none })

  // Still withheld: the next revalidation asks once, and stops.
  let before = source.calls.length
  current = revalidateTrace(current)
  // Nothing from before shows while it asks.
  expect(traceChildren(current, a.name)).toEqual([])
  expect(traceProgress(current)).toMatchObject({ unavailable: true })
  current = drain(current, source)
  expect(windowsRead(source, task.name, before).filter((read) => read.startsWith(a.name))).toEqual([
    `${a.name}@0`,
  ])
  expect(current.branches[a.name]).toEqual({ total: 0, rows: {}, unavailable: true, final: true })
  expect(nextTraceRequest(current)).toBeNull()

  // Coral returns the Run. Evidence alone asks for nothing; a revalidation reads it all.
  children[a.name] = readable
  expect(nextTraceRequest(markStale(current))).toBeNull()
  before = source.calls.length
  current = drain(revalidateTrace(current), source)
  expect(traceChildren(current, a.name)).toHaveLength(121)
  expect(traceChildren(current, b.name)).toHaveLength(1)
  expect(traceProgress(current)).toMatchObject({ complete: true, unavailable: false, final: true })
  expect(current.branches).toEqual(store.branches)
})

it('asks once more for a root that Coral withheld when it revalidates', () => {
  const { a, source, store } = settled()
  const load = source.load
  const withheld: string[][] = []
  source.load = (query) => {
    withheld.push(query.getAll('expand'))
    return activityPage({ rows: [], anchorUnavailable: true })
  }
  let current = drain(revalidateTrace(store), source)
  expect(current).toMatchObject({ unavailable: true, branches: none })
  // Still withheld: evidence alone asks for nothing; a revalidation asks once, and stops.
  expect(nextTraceRequest(markStale(current))).toBeNull()
  const before = withheld.length
  current = drain(revalidateTrace(current), source)
  expect(current).toMatchObject({ unavailable: true, branches: none })
  expect(withheld.slice(before)).toEqual([[task.name]])
  // Coral returns it.
  source.load = load
  current = drain(revalidateTrace(current), source)
  expect(traceChildren(current, a.name)).toHaveLength(121)
  expect(traceProgress(current)).toMatchObject({ complete: true, unavailable: false })
})

// Catches: the tail of a Run that runs waiting behind a check of every window of a long
// history, so its values are a full read old instead of one request.
it('reads what later evidence changed before the windows it only checks', () => {
  const ended = Array.from({ length: 25 }, (_, index) => runRow(`e${index}`))
  const live = runRow('live', 'running')
  const tail = [call(live, 0), call(live, 1, false)]
  const children: Parameters<typeof coral>[0] = {
    [open.name]: [...ended, live],
    ...Object.fromEntries(ended.map((run) => [run.name, [call(run, 0)]])),
    [live.name]: tail,
  }
  const source = coral(children, open)
  let store = drain(openTrace(open.name), source)
  // New evidence, at the moment a revalidation is due.
  tail.push(call(live, 2))
  store = revalidateTrace(markStale(store))
  const first = nextTraceRequest(store)!
  // The first request holds the Run that runs, at its tail, and the Task's own tail.
  expect(first.windows).toContainEqual({ parent: live.name, offset: 1 })
  store = receiveTrace(store, first, source.load(traceQuery(store, first)))
  expect(traceChildren(store, live.name)).toHaveLength(3)
  // The checks follow.
  const before = source.calls.length
  store = drain(store, source)
  expect(source.calls.length - before).toBeGreaterThan(0)
  expect(traceProgress(store)).toMatchObject({ complete: true, stale: false })
})

// Catches: a check of a long history that starts over with each minute's evidence. A check
// that takes longer than a minute then reads its first windows again and again, and a Run
// that Coral stopped returning stays on screen because no read reaches it.
it('ends a check that takes longer than the time between two before it starts the next', () => {
  const ended = Array.from({ length: 45 }, (_, index) => runRow(`e${index}`))
  const live = runRow('live', 'running')
  const tail = [call(live, 0)]
  const children: Parameters<typeof coral>[0] = {
    [open.name]: [...ended, live],
    ...Object.fromEntries(ended.map((run) => [run.name, [call(run, 0)]])),
    [live.name]: tail,
  }
  const source = coral(children, open)
  let store = drain(openTrace(open.name), source)
  // Coral stops returning the Run that a check reads last.
  const last = ended.at(-1)!
  children[last.name] = 'unavailable'
  // Each minute brings evidence and a revalidation, and has time for two requests. A check
  // of these 46 Runs takes five.
  for (let minute = 0; minute < 4; minute++) {
    tail.push(call(live, tail.length))
    store = revalidateTrace(markStale(store))
    for (let read = 0; read < 2; read++) {
      const request = nextTraceRequest(store)
      if (!request) break
      // The Run that runs comes first, also in the middle of a check.
      if (!read) expect(request.windows.map((window) => window.parent)).toContain(live.name)
      store = receiveTrace(store, request, source.load(traceQuery(store, request)))
    }
    expect(traceChildren(store, live.name)).toHaveLength(tail.length)
  }
  expect(store.branches[last.name]).toMatchObject({ unavailable: true, rows: none })
  expect(traceChildren(store, ended[0]!.name)).toHaveLength(1)
})

// Catches: the same first ten parents picked for every request while evidence marks more
// than ten again each time, so the ones after them are never read.
it('reads more changing runs than one request holds in turn', () => {
  const runs = Array.from({ length: 12 }, (_, index) => runRow(`r${index}`, 'running'))
  const source = coral(
    {
      [open.name]: runs,
      ...Object.fromEntries(runs.map((run) => [run.name, [call(run, 0)]])),
    },
    open,
  )
  let store = drain(openTrace(open.name), source)
  const read: string[][] = []
  for (let round = 0; round < 3; round++) {
    // Evidence marks every Run again before each answer.
    store = markStale(store)
    const request = nextTraceRequest(store)!
    read.push(request.windows.map((window) => window.parent).filter((name) => name !== open.name))
    store = receiveTrace(store, request, source.load(traceQuery(store, request)))
  }
  const ids = (round: number) => read[round]!.map((name) => name.split('/').at(-1))
  expect(ids(0)).toEqual(['r0', 'r1', 'r2', 'r3', 'r4', 'r5', 'r6', 'r7', 'r8', 'r9'])
  // The next request starts after the last Run the one before it read.
  expect(ids(1).slice(0, 4)).toEqual(['r10', 'r11', 'r0', 'r1'])
  expect(ids(2).slice(0, 2)).toEqual(['r8', 'r9'])
})

// Catches: an answer to a request that left before the trace learned that Coral withholds
// a Run returning that Run's records, as final, for the rest of the visit.
it('keeps a run withheld when an answer from before the trace learned of it joins', () => {
  const { a, b, children, source, store } = settled()
  // A check read leaves. Coral answers it with the Run's records.
  const checking = revalidateTrace(store)
  const request = nextTraceRequest(checking)!
  const older = source.load(traceQuery(checking, request))
  expect(request.windows.map((window) => window.parent)).toContain(a.name)
  // Meanwhile a snapshot of the list says that Coral withholds the Run.
  children[a.name] = 'unavailable'
  const told = absorbPage(
    checking,
    activityPage({
      rows: [task],
      branches: { [a.name]: { rows: [], next: '', unavailable: true } },
    }),
  )
  expect(traceChildren(told, a.name)).toEqual([])
  // The older answer joins: the Run stays withheld, and the other Run is as it was.
  let current = receiveTrace(told, request, older)
  expect(traceChildren(current, a.name)).toEqual([])
  expect(current.branches[a.name]).toMatchObject({ unavailable: true })
  expect(traceChildren(current, b.name)).toHaveLength(1)
  current = drain(current, source)
  expect(traceChildren(current, a.name)).toEqual([])
  // Coral returns the Run. A request that leaves now is later than that news: it decides.
  children[a.name] = [call(a, 0)]
  current = drain(revalidateTrace(current), source)
  expect(traceChildren(current, a.name)).toHaveLength(1)
})

// Catches: a check that waits for good behind evidence that marks the same windows again
// before each answer; and a parent that reads only its first changeable window, so a Run
// that ended in a later window shows as running until the check.
it('reads a check window with every second request, and each changeable window of a parent in turn', () => {
  const runs = Array.from({ length: 150 }, (_, index) =>
    runRow(`r${index}`, index === 3 ? 'running' : 'succeeded'),
  )
  const source = coral(
    {
      [open.name]: runs,
      ...Object.fromEntries(runs.map((run) => [run.name, [call(run, 0)]])),
    },
    open,
  )
  // The Task has two windows that evidence can change, and two that a check reads.
  let store = revalidateTrace(markStale(drain(openTrace(open.name), source)))
  expect(store.branches[open.name]).toMatchObject({ stale: [3, 150], check: [0, 100] })
  const asked: number[] = []
  for (let read = 0; read < 4; read++) {
    const request = nextTraceRequest(store)!
    asked.push(request.windows.find((window) => window.parent === open.name)!.offset)
    // Evidence marks every changeable window again before the next request.
    store = markStale(receiveTrace(store, request, source.load(traceQuery(store, request))))
  }
  // What changed, then a check, then what changed in the next window. That read holds the
  // last check window too, so the first changeable window has its turn again.
  expect(asked).toEqual([3, 0, 150, 3])
})

// Catches: an answer that withholds a Run winning over a later snapshot of the list that
// shows it readable, so the Run stays hidden until the next check; and the opposite, a
// readable snapshot that shows withheld records without asking Coral.
it('asks once more for a run that an answer withholds and a later snapshot shows readable', () => {
  const { a, b, children, source, store } = settled()
  const checking = revalidateTrace(store)
  const request = nextTraceRequest(checking)!
  expect(request.windows.map((window) => window.parent)).toContain(a.name)
  children[a.name] = 'unavailable'
  const older = source.load(traceQuery(checking, request))
  // The list's snapshot after the request left still shows the Run with its records.
  const shown = { branches: { [a.name]: { rows: [], next: '', unavailable: false } } }

  // Without a later snapshot, the answer decides at once: the Run is withheld, for good.
  const alone = receiveTrace(checking, request, older)
  expect(alone.branches[a.name]).toEqual({ total: 0, rows: {}, unavailable: true, final: true })

  // With one, the records still go at once, and the next request asks for the Run.
  let current = receiveTrace(checking, request, older, shown)
  expect(traceChildren(current, a.name)).toEqual([])
  expect(current.branches[a.name]).toMatchObject({ unavailable: true, retry: true })
  expect(traceChildren(current, b.name)).toHaveLength(1)
  const again = nextTraceRequest(current)!
  expect(again.windows).toContainEqual({ parent: a.name, offset: 0 })

  // Coral still withholds it: that answer is final, even with the same later snapshot.
  const denied = receiveTrace(current, again, source.load(traceQuery(current, again)), shown)
  expect(denied.branches[a.name]).toEqual({ total: 0, rows: {}, unavailable: true, final: true })

  // Coral grants it: the Run and its records show.
  children[a.name] = [call(a, 0)]
  current = drain(current, source)
  expect(traceChildren(current, a.name)).toHaveLength(1)
})

// Catches: an answer that withholds the root winning over a later snapshot of the list
// that shows it, so the whole trace stays unavailable and asks nothing more.
it.each([
  ['Coral does not find the anchor', { anchorUnavailable: true }],
  ['Coral denies the list', { listUnavailable: true }],
  ['the answer lists no root', {}],
] as const)(
  'asks once more for a root that an answer withholds and a later snapshot lists: %s',
  (_case, how) => {
    const { a, children, source, store } = settled()
    const checking = revalidateTrace(store)
    const request = nextTraceRequest(checking)!
    const older = { ...source.load(traceQuery(checking, request)), rows: [], branches: {}, ...how }
    const lists = { rows: [task], branches: {} }
    const hides = { rows: [], branches: {} }

    // A later snapshot that does not list the root changes nothing: the answer is final.
    for (const newer of [undefined, hides]) {
      const final = receiveTrace(checking, request, older, newer)
      expect(final).toMatchObject({ unavailable: true, branches: none })
      expect(final.retry).toBeUndefined()
      expect(nextTraceRequest(markStale(final))).toBeNull()
    }

    // One that lists it: nothing shows, and the next request asks Coral for the root.
    const hidden = receiveTrace(checking, request, older, lists)
    expect(hidden).toMatchObject({ unavailable: true, retry: true, branches: none })
    const again = nextTraceRequest(hidden)!
    expect(again.windows).toEqual([{ parent: task.name, offset: 0 }])

    // Coral still withholds it: that answer is final, also with the same later snapshot.
    const denied = receiveTrace(hidden, again, older, lists)
    expect(denied).toMatchObject({ unavailable: true, branches: none })
    expect(denied.retry).toBeUndefined()
    expect(nextTraceRequest(denied)).toBeNull()

    // Coral returns it: the trace reads it from the start, with every Run.
    const granted = drain(hidden, source)
    expect(granted.unavailable).toBe(false)
    expect(traceChildren(granted, a.name)).toHaveLength(children[a.name]!.length)
  },
)

// Catches: a Run that the trace asks for once more never asked for, because evidence marks
// more changing Runs than one request holds before every request, and an ask comes only
// after every changed window.
it.each([9, 10, 12, 20])(
  'asks for a withheld run in its turn while %i running runs change before every request',
  (count) => {
    const a = runRow('a')
    const running = Array.from({ length: count }, (_, index) => runRow(`r${index}`, 'running'))
    const children: Parameters<typeof coral>[0] = {
      [open.name]: [a, ...running],
      [a.name]: [call(a, 0)],
      ...Object.fromEntries(running.map((run) => [run.name, [call(run, 0, false)]])),
    }
    const source = coral(children, open)
    let store = revalidateTrace(drain(openTrace(open.name), source))
    // The first request that reads the Run gets an answer that withholds it, and a later
    // snapshot shows it: the trace asks for it once more.
    for (let read = 0; ; read++) {
      expect(read).toBeLessThan(10)
      store = markStale(store)
      const request = nextTraceRequest(store)!
      if (!request.windows.some((window) => window.parent === a.name)) {
        store = receiveTrace(store, request, source.load(traceQuery(store, request)))
        continue
      }
      children[a.name] = 'unavailable'
      const older = source.load(traceQuery(store, request))
      children[a.name] = [call(a, 0)]
      const shown = { branches: { [a.name]: { rows: [], next: '', unavailable: false } } }
      store = receiveTrace(store, request, older, shown)
      break
    }
    expect(store.branches[a.name]).toMatchObject({ unavailable: true, retry: true })
    // Evidence marks every running Run again before each request. The ask still comes
    // within two requests, and the Runs that run are read meanwhile.
    const asked: boolean[] = []
    for (let read = 0; read < 2 && !asked.at(-1); read++) {
      store = markStale(store)
      const request = nextTraceRequest(store)!
      asked.push(request.windows.some((window) => window.parent === a.name))
      expect(request.windows.length).toBeGreaterThan(1)
      store = receiveTrace(store, request, source.load(traceQuery(store, request)))
    }
    expect(asked.at(-1)).toBe(true)
    expect(traceChildren(store, a.name)).toHaveLength(1)
  },
)
