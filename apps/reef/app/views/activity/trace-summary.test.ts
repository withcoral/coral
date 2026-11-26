import { expect, it } from 'vitest'
import {
  absorbPage,
  markStale,
  nextTraceRequest,
  openTrace,
  receiveTrace,
  revalidateTrace,
} from '@/lib/activity-trace'
import { activityPage } from './fixtures'
import { recordRows, waterfall } from './trace'
import { timeSummary } from './trace-summary'
import type { ActivityRow } from './types'
import {
  T0,
  traceCall,
  traceEvent,
  tracePage,
  traceRun,
  traceStore,
  traceTask,
  traceTool,
} from './trace-fixtures'

const task = traceTask()

it('splits a root time into parts that add up to it', () => {
  const run = traceRun('a', 10_000, 30_000)
  const store = traceStore(task, {
    // A search for 2 s, then 8 s of nothing recorded, then the Run.
    [task.name]: [traceTool('s', 'search', 0, 2_000), run],
    [run.name]: [
      // Waits 4 s for a person, then 6 s with its provider.
      traceCall(run, 'c1', [11_000, 15_000, 21_000]),
      // Overlaps the first call: sent at once, 5 s with its provider.
      traceCall(run, 'c2', [18_000, 18_000, 23_000]),
    ],
  })
  const summary = timeSummary(store, T0)
  expect(summary).toEqual({
    state: 'ready',
    total: 60_000,
    // 15–23 s: overlapping calls count once.
    calls: 8_000,
    waiting: 4_000,
    // The Run's 20 s less the 12 s its calls cover.
    programs: 8_000,
    discovery: 2_000,
    agentModel: 38_000,
    ongoing: false,
    unknownEnds: 0,
  })
  if (summary.state !== 'ready') throw new Error('not ready')
  expect(
    summary.calls + summary.waiting + summary.programs + summary.discovery + summary.agentModel,
  ).toBe(summary.total)
})

it('counts work in progress up to now and says that it still grows', () => {
  const open = traceTask({ outcome: 'open', endedAt: null })
  const run = traceRun('a', 0, null)
  const store = traceStore(open, {
    [open.name]: [run],
    [run.name]: [traceCall(run, 'c1', [1_000, null, null])],
  })
  expect(timeSummary(store, T0 + 10_000)).toMatchObject({
    state: 'ready',
    total: 10_000,
    waiting: 9_000,
    programs: 1_000,
    agentModel: 0,
    ongoing: true,
  })
})

it('counts a record with no recorded end from its start only, and says how many', () => {
  const run = traceRun('a', 1_000, null, { state: 'unknown' })
  const store = traceStore(task, {
    [task.name]: [traceTool('s', 'search', 0, null), run],
    [run.name]: [],
  })
  expect(timeSummary(store, T0 + 500_000)).toMatchObject({
    state: 'ready',
    total: 60_000,
    programs: 0,
    discovery: 0,
    agentModel: 60_000,
    ongoing: false,
    unknownEnds: 2,
  })
})

// Catches: a Run with no recorded end counted twice when it is the root, and its
// Program's recorded time moved into Agent model. A Run ran at least until its last
// recorded activity; nothing counts past it.
it.each(
  (
    [
      ['failed', 'a failed'],
      ['unknown', 'an unknown-state'],
    ] as const
  ).flatMap(([state, label]) => {
    const ran = (parent: string) =>
      traceRun('a', 10_000, null, { state, task: parent, durationNanos: '0' })
    const inTask = ran(task.name)
    const alone = ran('')
    const activity = (run: typeof inTask) => [
      traceCall(run, 'c1', [10_000, 10_000, 15_000]),
      traceCall(run, 'c2', [25_000, 25_000, 30_000]),
      traceEvent(run, 3, 32_000),
    ]
    return [
      [
        `${label} Run of a Task, with its activity`,
        task,
        inTask,
        activity(inTask),
        { total: 60_000, calls: 10_000, programs: 12_000, agentModel: 38_000 },
      ],
      [
        `${label} Run of a Task, with no activity`,
        task,
        inTask,
        [],
        { total: 60_000, calls: 0, programs: 0, agentModel: 60_000 },
      ],
      [
        `${label} standalone Run, with its activity`,
        alone,
        alone,
        activity(alone),
        { total: 22_000, calls: 10_000, programs: 12_000, agentModel: 0 },
      ],
      [
        `${label} standalone Run, with no activity`,
        alone,
        alone,
        [],
        { total: 0, calls: 0, programs: 0, agentModel: 0 },
      ],
    ] as const
  }),
)('counts %s up to its last recorded activity, once', (_, root, run, children, parts) => {
  const store = traceStore(root, {
    ...(root.kind === 'task' && { [root.name]: [run] }),
    [run.name]: [...children],
  })
  expect(timeSummary(store, T0 + 500_000)).toEqual({
    state: 'ready',
    waiting: 0,
    discovery: 0,
    ...parts,
    ongoing: false,
    unknownEnds: 1,
  })
})

/** A call Coral sent at 4 s whose end it never recorded, though the call ended. */
const sentAndLost = (run: ReturnType<typeof traceRun>) =>
  traceCall(run, 'lost', [0, 4_000, null], { state: 'ended' })

// Catches: a call with no recorded end that Coral sent counted as waiting past the time
// its Run and the total count to, so the parts added up to more than the total. Its
// dispatch is recorded activity; nothing after it is.
it.each(
  (() => {
    const alone = traceRun('alone', 0, null, { state: 'unknown', task: '', durationNanos: '0' })
    const inTask = traceRun('in-task', 0, null, { state: 'unknown', durationNanos: '0' })
    return [
      [
        'a sent call with no recorded end, in a standalone Run',
        alone,
        { [alone.name]: [sentAndLost(alone)] },
        { total: 4_000, waiting: 4_000, discovery: 0, agentModel: 0, unknownEnds: 2 },
      ],
      [
        'a sent call with no recorded end, in a Run of a Task',
        task,
        { [task.name]: [inTask], [inTask.name]: [sentAndLost(inTask)] },
        { total: 60_000, waiting: 4_000, discovery: 0, agentModel: 56_000, unknownEnds: 2 },
      ],
      [
        'a search of the Task with no recorded end',
        task,
        { [task.name]: [traceTool('lost', 'search', 5_000, null)] },
        { total: 60_000, waiting: 0, discovery: 0, agentModel: 60_000, unknownEnds: 1 },
      ],
    ] satisfies [string, ActivityRow, Record<string, ActivityRow[]>, object][]
  })(),
)('counts %s up to its last recorded activity', (_, root, children, parts) => {
  const summary = timeSummary(traceStore(root, children), T0 + 500_000)
  expect(summary).toEqual({ state: 'ready', calls: 0, programs: 0, ongoing: false, ...parts })
  if (summary.state !== 'ready') throw new Error('not ready')
  expect(
    summary.calls + summary.waiting + summary.programs + summary.discovery + summary.agentModel,
  ).toBe(summary.total)
})

// Catches: the view failing with "Maximum call stack size exceeded" on a long history. The
// total took every instant of every record as one argument of a function call.
it('states the time of a history of more records than one call takes arguments', () => {
  const run = traceRun('long', 0, 20_000)
  const calls = Array.from({ length: 150_000 }, (_, index) =>
    traceCall(run, `c${index}`, [1_000, 1_000, 2_000 + (index % 10) * 1_000]),
  )
  const store = traceStore(task, { [task.name]: [run], [run.name]: calls })
  expect(timeSummary(store, T0)).toMatchObject({ state: 'ready', calls: 10_000 })
  // The axis of the same history spans the Task.
  const { from, to } = waterfall(store, new Set(), T0)
  expect(to - from).toBe(60_000)
})

it('includes a run that outlives its task in the total', () => {
  const run = traceRun('a', 50_000, 90_000)
  const store = traceStore(task, { [task.name]: [run], [run.name]: [] })
  expect(timeSummary(store, T0)).toMatchObject({ total: 90_000, programs: 40_000 })
})

it('states no time while the trace still reads, or when evidence is unavailable', () => {
  const run = traceRun('a', 0, 9_000)
  const unread = absorbPage(openTrace(task.name), tracePage(task, { [task.name]: [run] }))
  expect(timeSummary(unread, T0)).toEqual({ state: 'loading' })

  const withheld = absorbPage(
    unread,
    activityPage({
      rows: [task],
      branches: { [run.name]: { rows: [], next: '', unavailable: true } },
    }),
  )
  expect(timeSummary(withheld, T0)).toEqual({ state: 'unavailable' })
})

// Catches: a Run that just ended showing what the trace read before its end as exact
// values. A live snapshot says the Run ended before any read returns its last records.
it('states no time for a run that ended until a read after its end holds its records', () => {
  // A Task with a Run in progress is open.
  const open = traceTask({ outcome: 'open', endedAt: null })
  const running = traceRun('a', 0, null)
  const first = traceCall(running, 'c1', [0, 0, 1])
  let store = traceStore(open, { [open.name]: [running], [running.name]: [first] })
  expect(timeSummary(store, T0)).toMatchObject({ state: 'ready', ongoing: true })
  expect(waterfall(store, new Set(), T0).rows).toHaveLength(3)

  // The Run made one more call before it ended.
  const ended = traceRun('a', 0, 9_000)
  store = absorbPage(store, tracePage(open, { [open.name]: [ended] }))
  expect(timeSummary(store, T0)).toEqual({ state: 'loading' })
  // The trace asks for the Run's records again without waiting for more evidence.
  expect(nextTraceRequest(store)?.windows.map((window) => window.parent)).toContain(ended.name)

  const last = traceCall(ended, 'c2', [1, 1, 2])
  store = absorbPage(store, tracePage(open, { [ended.name]: [first, last] }))
  expect(timeSummary(store, T0)).toMatchObject({ state: 'ready', ongoing: false })
  expect(waterfall(store, new Set(), T0).rows).toHaveLength(4)
  // Every record is read: only the Task's own detail, for its header, is left to read.
  expect(nextTraceRequest(store)).toEqual({ windows: [], taskDetail: true })
})

/** A call of the private tool `id`, whose body makes the calls in its own window. */
const toolCall = (
  run: ReturnType<typeof traceRun>,
  id: string,
  times: [number, number | null, number | null],
) =>
  traceCall(run, id, times, {
    toolKind: 'private',
    provider: '',
    tool: id,
    callPath: `coral.private.${id}`,
  })

// Catches: the calls a tool's body made left out of the time, or the tool's own call
// counted as time with a provider.
it('counts the calls a tool’s body made, and the tool’s own code as program time', () => {
  const run = traceRun('a', 0, 9_000)
  const report = toolCall(run, 'report', [1_000, 1_000, 7_000])
  const inside = {
    parent: { invocation: report.name, callPath: report.callPath, toolKind: 'private' as const },
  }
  const store = traceStore(task, {
    [task.name]: [run],
    [run.name]: [report],
    // Waits 1 s, then 2 s with its provider.
    [report.name]: [traceCall(run, 'c1', [2_000, 3_000, 5_000], inside)],
  })
  expect(timeSummary(store, T0)).toMatchObject({
    calls: 2_000,
    waiting: 1_000,
    // The Run's 9 s less the 3 s of the provider call and its wait.
    programs: 6_000,
  })
})

// Catches: values stated from records that a failed read left out of date.
it('states no time after a refresh fails, and states it again once a retry reads it', () => {
  const open = traceTask({ outcome: 'open', endedAt: null })
  const run = traceRun('a', 0, 9_000)
  const calls = [traceCall(run, 'c1', [0, 0, 1])]
  let store = traceStore(open, { [open.name]: [run], [run.name]: calls })
  const ready = { state: 'ready', programs: 8_999 }
  expect(timeSummary(store, T0)).toMatchObject(ready)

  // Later evidence: a read is due. Until it answers, the values stay.
  store = markStale(store)
  expect(timeSummary(store, T0)).toMatchObject(ready)
  const request = nextTraceRequest(store)!
  // The read fails. That is no denial: the records stay, and the summary states no value.
  store = receiveTrace(store, request, null)
  expect(waterfall(store, new Set(), T0).rows).toHaveLength(3)
  expect(timeSummary(store, T0)).toEqual({ state: 'failed' })

  // A retry reads a second Run that the failed read would have returned.
  const later = traceRun('b', 20_000, 30_000)
  store = absorbPage(
    { ...store, failed: false },
    tracePage(open, { [open.name]: [run, later], [later.name]: [] }),
  )
  expect(timeSummary(store, T0)).toMatchObject({ state: 'ready', programs: 18_999 })
})

// Catches: the same for the read that checks records which change no more.
it('states no time when the read that revalidates an ended root fails', () => {
  const run = traceRun('a', 0, 9_000)
  const store = revalidateTrace(
    traceStore(task, { [task.name]: [run], [run.name]: [traceCall(run, 'c1', [0, 0, 1])] }),
  )
  // While the read runs, the values stay.
  expect(timeSummary(store, T0)).toMatchObject({ state: 'ready' })
  const failed = receiveTrace(store, nextTraceRequest(store)!, null)
  expect(timeSummary(failed, T0)).toEqual({ state: 'failed' })
})

// Catches: a waterfall that still shows a Run the open Task no longer lists, or shows a Run
// twice because the Runs after the lost one moved up.
it('shows each run once after an open task loses one', () => {
  const open = traceTask({ outcome: 'open', endedAt: null })
  const runs = ['a', 'b', 'c'].map((id) => traceRun(id, 0, 9_000))
  const calls = (kept: typeof runs) =>
    Object.fromEntries(
      kept.map((run) => [
        run.name,
        [traceCall(run, `call-${run.name.split('/').at(-1)}`, [0, 0, 1])],
      ]),
    )
  let store = traceStore(open, { [open.name]: runs, ...calls(runs) })

  // Coral lists two Runs now. Later evidence makes the trace read the Task's window again.
  const kept = runs.slice(1)
  store = markStale(store)
  const request = nextTraceRequest(store)!
  store = receiveTrace(store, request, tracePage(open, { [open.name]: kept, ...calls(kept) }))
  expect(timeSummary(store, T0)).toMatchObject({ state: 'ready' })
  expect(recordRows(waterfall(store, new Set(), T0).rows).map((row) => row.row.name)).toEqual([
    open.name,
    ...kept.flatMap((run) => [run.name, calls([run])[run.name]![0]!.name]),
  ])
})
