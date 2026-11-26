import {
  traceCalls,
  traceChildren,
  traceProgress,
  traceRuns,
  type ActivityTraceStore,
} from '@/lib/activity-trace'
import { intervalsLength, unionIntervals, type Interval } from '@/wax/components/timeline'
import { expandable } from './navigation'
import { spanOf, waitOf, type SpanRow } from './trace'
import type { ActivityRow } from './types'

type CallRow = Extract<ActivityRow, { kind: 'invocation' }>
type RunRow = Extract<ActivityRow, { kind: 'run' }>
type ToolRow = Extract<ActivityRow, { kind: 'tool' }>

/**
 * Whether a summary can state exact values. `loading`: the trace has not read every
 * record yet, or a record ended and no read after its end holds its children yet.
 * `unavailable`: Coral withholds or lacks evidence the value needs, so no
 * read completes it. `failed`: a read failed, so nothing loads until you try again. A
 * summary shows a value only when it is ready.
 */
export type SummaryState = 'loading' | 'unavailable' | 'failed' | 'ready'
type Pending = Exclude<SummaryState, 'ready'>

/** Why no more records are on their way: a read failed. Else they still load. */
const pendingState = (store: ActivityTraceStore): Pending => (store.failed ? 'failed' : 'loading')

function evidenceState(store: ActivityTraceStore): SummaryState {
  const progress = traceProgress(store)
  if (progress.unavailable) return 'unavailable'
  // What the trace holds under a record that just ended can be from before its end.
  if (!progress.complete || progress.settling) return pendingState(store)
  // A read of what later evidence changed failed: what the trace holds is from before it,
  // and nothing reads again until a retry. A read in progress leaves the values in place.
  return store.failed && progress.stale ? 'failed' : 'ready'
}
/** The trace holds every record, so a summary can state exact values. */
const complete = (store: ActivityTraceStore) => {
  const state = evidenceState(store)
  return state === 'ready' ? null : state
}

/** Every tool call of the trace, also the ones a tool's body made, with its Run. */
function calls(store: ActivityTraceStore): { call: CallRow; run: RunRow }[] {
  return traceRuns(store).flatMap((run) =>
    traceCalls(store, run.name).map((call) => ({ call, run })),
  )
}

/** When Coral sent `call` upstream, if it did. */
const dispatched = (call: CallRow) =>
  call.dispatchedAt ? Date.parse(call.dispatchedAt) : undefined

/** When the last recorded activity under `run` took place: the latest start, dispatch or end. */
function lastActivity(store: ActivityTraceStore, run: string) {
  let last = -Infinity
  for (const call of traceCalls(store, run)) {
    const { start, end } = spanOf(call)
    last = Math.max(last, end.kind === 'ended' ? end.at : (dispatched(call) ?? start))
  }
  for (const row of traceChildren(store, run))
    if (row.kind === 'output') last = Math.max(last, Date.parse(row.startedAt))
  return last
}

/** The length of `intervals` that `covered` does not cover. */
const uncovered = (intervals: Interval[], covered: Interval[]) =>
  intervalsLength(unionIntervals([...intervals, ...covered])) -
  intervalsLength(unionIntervals(covered))

/** Where a root's time went, in milliseconds. The five parts add up to `total`. */
export interface TimeBreakdown {
  /** From the first known start to the last known end, or to now while work goes on. */
  total: number
  /**
   * A tool call was with its provider: from when Coral sent it to when it ended. A call of
   * a private or shared tool runs code in Coral, so only the calls that code made count.
   */
  calls: number
  /** A tool call waited before Coral sent it, and no other call was with its provider. */
  waiting: number
  /** A Program, or the code of a tool it called, ran with no provider call in progress. */
  programs: number
  /** A Task tool call that is no Run, such as a search, with nothing else in progress. */
  discovery: number
  /**
   * No recorded Coral activity covers it. It is the time between records, not a
   * measurement of the model: it also holds client work, network delay and idle time.
   */
  agentModel: number
  /** A Run or a tool call goes on, so every part still grows. */
  ongoing: boolean
  /**
   * Records with no recorded end. A Run counts up to its last recorded activity, which it
   * outlived, and a call up to its dispatch; any other record counts from its start only.
   */
  unknownEnds: number
}

export type TimeSummary = { state: Pending } | ({ state: 'ready' } & TimeBreakdown)

export function timeSummary(store: ActivityTraceStore, now: number): TimeSummary {
  const missing = complete(store)
  if (missing) return { state: missing }
  if (!store.rootRow || store.rootRow.kind === 'output') return { state: 'unavailable' }
  let ongoing = false
  let unknownEnds = 0
  const instants: number[] = []
  /** `seen`: when the record's last recorded activity took place, which it outlived. */
  const interval = (row: SpanRow, seen?: () => number): Interval => {
    const { start, end } = spanOf(row)
    // An open Task is a goal: it neither runs the clock nor lacks an end.
    const works = row.kind !== 'task'
    if (works && end.kind === 'ongoing') ongoing = true
    if (works && end.kind === 'unknown') unknownEnds += 1
    const to =
      end.kind === 'ended'
        ? end.at
        : end.kind === 'ongoing' && works
          ? Math.max(now, start)
          : Math.max(seen?.() ?? start, start)
    instants.push(start, to)
    return { from: start, to }
  }
  const runs = traceRuns(store)
  // A Run at the root is its own trace's one Run.
  if (store.rootRow.kind !== 'run') interval(store.rootRow)
  const programs = runs.map((run) => interval(run, () => lastActivity(store, run.name)))
  const tools =
    store.rootRow.kind === 'task'
      ? traceChildren(store, store.root)
          .filter((row): row is ToolRow => row.kind === 'tool')
          .map((row) => interval(row))
      : []
  const sent: Interval[] = []
  const waits: Interval[] = []
  for (const { call } of calls(store)) {
    // A call with no recorded end counts up to its dispatch, the last activity it recorded.
    const whole = interval(call, () => dispatched(call) ?? -Infinity)
    const wait = waitOf(call)
    if (wait) waits.push({ from: wait.from, to: wait.to ?? whole.to })
    // Only a call Coral sent upstream spends time with its provider. A tool whose body
    // Coral runs is code, and the calls that code made are here too.
    const code: boolean = expandable(call)
    const sentAt = dispatched(call)
    if (sentAt !== undefined && !code)
      sent.push({ from: Math.max(sentAt, whole.from), to: Math.max(whole.to, sentAt) })
  }
  for (const run of runs)
    for (const row of traceChildren(store, run.name))
      if (row.kind === 'output') instants.push(Date.parse(row.startedAt))
  // One at a time: a long history has more instants than a call takes arguments.
  let [first, last] = [Infinity, -Infinity]
  for (const instant of instants) {
    first = Math.min(first, instant)
    last = Math.max(last, instant)
  }
  const total = last - first
  const callTime = intervalsLength(unionIntervals(sent))
  const waiting = uncovered(waits, sent)
  const programTime = uncovered(programs, [...sent, ...waits])
  const discovery = uncovered(tools, [...sent, ...waits, ...programs])
  return {
    state: 'ready',
    total,
    calls: callTime,
    waiting,
    programs: programTime,
    discovery,
    agentModel: Math.max(0, total - callTime - waiting - programTime - discovery),
    ongoing,
    unknownEnds,
  }
}
