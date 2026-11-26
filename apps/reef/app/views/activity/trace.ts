import {
  missingWindows,
  traceChildren,
  traceReadable,
  traceRecord,
  type ActivityTraceStore,
} from '@/lib/activity-trace'
import type { Interval } from '@/wax/components/timeline'
import { expandable, recordParents, type ActivityStop } from './navigation'
import { rowStatus } from './record'
import type { ActivityPage, ActivityRow } from './types'

/** A record with a place of its own in a waterfall: everything but a Run's Events. */
export type SpanRow = Exclude<ActivityRow, { kind: 'output' }>
/** A Run's Event. A waterfall shows it as a mark on its Run. */
export type EventRow = Extract<ActivityRow, { kind: 'output' }>
type CallRow = Extract<ActivityRow, { kind: 'invocation' }>
type RunRow = Extract<ActivityRow, { kind: 'run' }>

/**
 * How a record's interval ends. `ongoing` only when Coral's record says the work goes on:
 * a missing end alone is `unknown`, never evidence of work.
 */
export type SpanEnd = { kind: 'ended'; at: number } | { kind: 'ongoing' } | { kind: 'unknown' }

const at = (instant: string) => Date.parse(instant)

/** The instant Coral recorded `row` ending, if any. */
export function recordEnd(row: SpanRow): string | null {
  return row.kind === 'tool' ? row.completedAt : row.endedAt
}

/** Coral's record says the work goes on. */
export function isOngoing(row: SpanRow) {
  switch (row.kind) {
    case 'task':
      return !row.endedAt && row.outcome === 'open'
    case 'run':
      return row.state === 'running'
    case 'invocation':
      return row.state === 'pending' || row.state === 'dispatching'
    case 'tool':
      return false
  }
}

/**
 * When `row` starts and how it ends, in epoch milliseconds. A Run starts when Coral
 * accepted it and a tool call when Coral admitted it, so both include their waiting.
 */
export function spanOf(row: SpanRow): { start: number; end: SpanEnd } {
  const end = recordEnd(row)
  return {
    start: at(row.startedAt),
    end: end
      ? { kind: 'ended', at: Math.max(at(end), at(row.startedAt)) }
      : isOngoing(row)
        ? { kind: 'ongoing' }
        : { kind: 'unknown' },
  }
}

/**
 * The part of a tool call spent before Coral sent it upstream, from its start. `to` is
 * null while it still waits. Undefined when it never waited.
 */
export function waitOf(row: CallRow): { from: number; to: number | null } | undefined {
  const from = at(row.startedAt)
  if (row.dispatchedAt) {
    const to = at(row.dispatchedAt)
    return to > from ? { from, to } : undefined
  }
  if (row.state === 'pending') return { from, to: null }
  // A call that ended without ever being sent waited all its life.
  return row.endedAt && at(row.endedAt) > from ? { from, to: at(row.endedAt) } : undefined
}

/**
 * What a tool call that still waits is waiting for, as its Run's pending requests say.
 * Null when the Run names none for this tool, and always for a wait that is over: no row
 * says what a settled wait was for.
 */
export function pendingWaitReason(
  row: CallRow,
  run: RunRow | undefined,
): 'approval' | 'account_connection' | null {
  if (row.state === 'ended' || !row.requiresAction) return null
  const kinds = new Set(
    (run?.actionPage?.actions ?? [])
      .filter((action) => action.callPath === row.callPath)
      .map((action) => action.kind),
  )
  // Two kinds for one tool do not say which this call waits on.
  return kinds.size === 1 ? [...kinds][0]! : null
}

/** The Events a waterfall marks on a Run: what the Program wrote, and a cancellation. */
export const isMarked = (row: EventRow) =>
  row.eventKind !== 'run.started' && row.eventKind !== 'run.ended'

export interface WaterfallRecord {
  kind: 'record'
  row: SpanRow
  depth: number
  /** Enclosing records, nearest first. */
  parents: string[]
  /** It has children to show or hide. */
  expandable: boolean
  expanded: boolean
  start: number
  end: SpanEnd
  wait?: { from: number; to: number | null }
  /** Its Events, in order. Only a Run has any: Coral lists every Event at its Run's top level. */
  marks: { row: EventRow; at: number }[]
}

/** Children the trace does not hold: not read yet, or not readable. */
export interface WaterfallGap {
  kind: 'gap'
  key: string
  parent: string
  depth: number
  parents: string[]
  /**
   * Ordinals [from, to) among the parent's children. Both 0 for an unavailable history, and
   * for children no read has counted yet.
   */
  from: number
  to: number
  reason: 'unloaded' | 'unavailable'
}

export type WaterfallRow = WaterfallRecord | WaterfallGap

export interface Waterfall {
  rows: WaterfallRow[]
  /** The axis, in epoch milliseconds. It covers every known record, also one that outlives its Task. */
  from: number
  to: number
  /** When Coral worked: every tool call and Run, joined. The rest is time between them. */
  busy: Interval[]
  /** A Run or a tool call goes on, so the axis ends now and grows. */
  ongoing: boolean
}

/** A record's interval as far as it is known at `now`. An unknown end has no length. */
function knownInterval(start: number, end: SpanEnd, now: number): Interval {
  return {
    from: start,
    to: end.kind === 'ended' ? end.at : end.kind === 'ongoing' ? Math.max(now, start) : start,
  }
}

/**
 * Lays out everything the trace holds under its root: each record on one row, in Coral's
 * order, with a Run's Events as marks on the Run and the calls a tool's body made under
 * that tool's call. `collapsed` records hide their children. Children the trace does not
 * hold show as a gap where they belong.
 */
export function waterfall(
  store: ActivityTraceStore,
  collapsed: ReadonlySet<string>,
  now: number,
): Waterfall {
  const rows: WaterfallRow[] = []
  const instants: number[] = []
  const busy: Interval[] = []
  let ongoing = false
  const gaps = (parent: string, depth: number, parents: string[]) => {
    const branch = store.branches[parent]
    // No read has returned these children yet, so nothing says how many they are. Children
    // nested deeper than a request can open are not readable at all.
    if (!branch)
      return [
        {
          kind: 'gap' as const,
          key: `${parent}#unread`,
          parent,
          depth,
          parents,
          from: 0,
          to: 0,
          reason: traceReadable(store, parent) ? ('unloaded' as const) : ('unavailable' as const),
        },
      ]
    const list: WaterfallGap[] = missingWindows(branch).map(({ from, to }) => ({
      kind: 'gap',
      key: `${parent}#${from}`,
      parent,
      depth,
      parents,
      from,
      to,
      reason: 'unloaded',
    }))
    if (branch?.unavailable)
      list.push({
        kind: 'gap',
        key: `${parent}#unavailable`,
        parent,
        depth,
        parents,
        from: branch.total,
        to: branch.total,
        reason: 'unavailable',
      })
    return list
  }
  const visit = (row: SpanRow, depth: number, parents: string[], shown: boolean) => {
    const holds = expandable(row)
    const children = holds ? traceChildren(store, row.name) : []
    const span = spanOf(row)
    // An open Task is a goal, not work: only a Run or a tool call runs the axis to now.
    const interval = knownInterval(span.start, span.end, row.kind === 'task' ? span.start : now)
    instants.push(interval.from, interval.to)
    if (row.kind !== 'task') {
      busy.push(interval)
      if (span.end.kind === 'ongoing') ongoing = true
    }
    const events = children.filter((child): child is EventRow => child.kind === 'output')
    for (const event of events) instants.push(at(event.startedAt))
    const nested = children.filter((child): child is SpanRow => child.kind !== 'output')
    const inside = holds ? [row.name, ...parents] : parents
    const missing = holds ? gaps(row.name, depth + 1, inside) : []
    const expanded = holds && !collapsed.has(row.name)
    if (shown)
      rows.push({
        kind: 'record',
        row,
        depth,
        parents,
        expandable: holds && (nested.length > 0 || missing.length > 0),
        expanded,
        ...span,
        ...(row.kind === 'invocation' && waitOf(row) && { wait: waitOf(row) }),
        marks: events.filter(isMarked).map((event) => ({ row: event, at: at(event.startedAt) })),
      })
    // Children and gaps interleave in Coral's order.
    const pending = [...missing]
    for (const child of nested) {
      while (pending.length && pending[0]!.to <= (child.position ?? Infinity)) {
        const gap = pending.shift()!
        if (shown && expanded) rows.push(gap)
      }
      visit(child, depth + 1, inside, shown && expanded)
    }
    if (shown && expanded) rows.push(...pending)
  }
  if (store.rootRow && store.rootRow.kind !== 'output') visit(store.rootRow, 0, [], true)
  const known = instants.filter((value) => Number.isFinite(value))
  // One at a time: a long history has more instants than a call takes arguments.
  const from = known.reduce((least, value) => Math.min(least, value), known[0] ?? now)
  const last = known.reduce((most, value) => Math.max(most, value), known[0] ?? now)
  // An axis of no length would divide by zero wherever it is drawn.
  return { rows, from, to: Math.max(last, from + 1), busy, ongoing }
}

/**
 * How much of the time from `from` to `to` the `folds` compress. A fold that the range
 * only cuts counts for the part inside it, so the sum never exceeds the range.
 */
export const compressedTime = (folds: readonly Interval[], from: number, to: number) =>
  folds.reduce(
    (sum, fold) => sum + Math.max(0, Math.min(fold.to, to) - Math.max(fold.from, from)),
    0,
  )

/** A stretch of one lane of the overview in which records of one kind worked. */
export interface OverviewSpan {
  lane: number
  kind: Exclude<SpanRow['kind'], 'task'>
  /** Epoch milliseconds. */
  from: number
  to: number
  /** A record in it failed. */
  failed: boolean
  /** How its last record ends. An open end reaches the end of the overview. */
  end: SpanEnd['kind']
}

/** What an overview draws, counted, so it can also say it in words. */
export interface OverviewCounts {
  runs: number
  calls: number
  failed: number
  ongoing: number
  unknownEnds: number
}

/** The most lanes an overview draws. Its strip has room for no more thin lines. */
const OVERVIEW_LANES = 8

/** Whether Coral's record of `row` says it failed. */
export const recordFailed = (row: SpanRow) => rowStatus(row)?.tone === 'error'

/**
 * The whole waterfall in a few lanes, for a strip that shows all of it at once. Each Run
 * and tool call goes in a lane by its place among the rows, so the strip keeps the shape of
 * the waterfall. With no more records than lanes, each has a lane and a span of its own.
 * With more, neighbours share a lane, and those of one kind that are closer than `join` on
 * the axis show as one span. `axis` places an instant on that axis; a folded axis draws a
 * long gap short, so the same time is not the same distance everywhere. A span never covers
 * a stretch longer than `join` in which none of its records worked. A Task only frames the
 * others, so it has no span. An open end reaches `end`.
 */
export function overview(
  rows: readonly WaterfallRow[],
  end: number,
  join = 0,
  axis: (time: number) => number = (time) => time,
): { lanes: number; spans: OverviewSpan[]; counts: OverviewCounts } {
  const records = recordRows(rows).filter((record) => record.row.kind !== 'task')
  const lanes = Math.min(records.length, OVERVIEW_LANES)
  const counts: OverviewCounts = { runs: 0, calls: 0, failed: 0, ongoing: 0, unknownEnds: 0 }
  const groups = new Map<string, OverviewSpan[]>()
  records.forEach((record, index) => {
    const lane = Math.floor((index * lanes) / records.length)
    const kind = record.row.kind as OverviewSpan['kind']
    const failed = recordFailed(record.row)
    if (kind === 'run') counts.runs += 1
    else counts.calls += 1
    if (failed) counts.failed += 1
    if (record.end.kind === 'ongoing') counts.ongoing += 1
    if (record.end.kind === 'unknown') counts.unknownEnds += 1
    const span = {
      lane,
      kind,
      ...knownInterval(record.start, record.end, end),
      failed,
      end: record.end.kind,
    }
    const key = `${lane}:${kind}`
    groups.set(key, [...(groups.get(key) ?? []), span])
  })
  const spans: OverviewSpan[] = []
  for (const group of groups.values()) {
    // Rows are in Coral's order, not in time: a later row can start before an earlier one.
    let last: OverviewSpan | undefined
    for (const span of group.toSorted((a, b) => a.from - b.from)) {
      if (last && records.length > lanes && axis(span.from) <= axis(last.to) + join) {
        if (span.to >= last.to) {
          last.to = span.to
          last.end = span.end
        }
        last.failed ||= span.failed
        continue
      }
      last = { ...span }
      spans.push(last)
    }
  }
  return { lanes, spans, counts }
}

/**
 * The mark ← or → goes to from `from`, in its Run's order: the mark before or after a
 * mark, the first or last mark from the Run itself, and from a tool call the mark next to
 * it. A mark and a call the Program made share the Run's ordinals. A call a tool's body
 * made has an ordinal among that body's calls only, so its own start places it, and the
 * ordinal of the Program's call around it only breaks a tie. Null when the Run has no mark
 * on show; undefined at either end.
 */
export function adjacentMark(
  store: ActivityTraceStore | null,
  rows: readonly WaterfallRow[],
  from: ActivityRow | undefined,
  step: 1 | -1,
): EventRow | null | undefined {
  const run = from?.kind === 'output' || from?.kind === 'invocation' ? from.run : from?.name
  const record = recordRows(rows).find((row) => row.row.name === run)
  if (!from || !record?.marks.length) return null
  const marks = record.marks
  const index = marks.findIndex((mark) => mark.row.name === from.name)
  if (index >= 0) return marks[index + step]?.row
  if (from.kind !== 'invocation') return marks.at(step > 0 ? 0 : -1)?.row
  // The Program's own call around `from`, or `from` itself, when the trace holds it.
  const outermost = enclosingRecords(store, from).at(-recordParents(from).length)
  const outer = from.parent ? store && outermost && traceRecord(store, outermost) : from
  const ordinal = outer && outer.kind === 'invocation' && !outer.parent ? outer.position : undefined
  const started = from.parent ? at(from.startedAt) : undefined
  /** Whether `mark` comes after the call. Undefined when nothing places them. */
  const after = (mark: (typeof marks)[number]) => {
    if (started !== undefined && mark.at !== started) return mark.at > started
    if (ordinal === undefined || mark.row.position === undefined) return undefined
    return mark.row.position > ordinal
  }
  return step > 0
    ? marks.find((mark) => after(mark) === true)?.row
    : marks.findLast((mark) => after(mark) === false)?.row
}

/**
 * Where ↑ and ↓ go in a waterfall: its records in order. Children the trace does not hold,
 * not read yet or not readable, are boundaries, so the keys never skip a record.
 */
export const waterfallStops = (rows: readonly WaterfallRow[]): ActivityStop[] =>
  rows.map((row) =>
    row.kind === 'gap'
      ? { kind: 'boundary' }
      : { kind: 'record', row: row.row, parents: row.parents, expanded: row.expanded },
  )

/**
 * The records that enclose `row`, nearest first. A record names only the tool call whose
 * body made it, so the calls around that one come from the trace, as far as it holds them.
 */
export function enclosingRecords(store: ActivityTraceStore | null, row: ActivityRow): string[] {
  const [nearest, ...rest] = recordParents(row)
  if (row.kind !== 'invocation' || !row.parent || !store || !nearest) return recordParents(row)
  const calls = [nearest]
  for (let depth = 0; depth < 64; depth++) {
    const holder = traceRecord(store, calls.at(-1)!)
    if (holder?.kind !== 'invocation' || !holder.parent) break
    calls.push(holder.parent.invocation)
  }
  return [...calls, ...rest]
}

/** The record rows of a waterfall, which Space and the marks act on. */
export const recordRows = (rows: readonly WaterfallRow[]) =>
  rows.filter((row): row is WaterfallRecord => row.kind === 'record')

/** The top-level record `row` sits under: its Task, else its Run, else itself. */
export function rootOf(row: ActivityRow): string {
  if (row.kind === 'task') return row.name
  const task = row.task ?? ''
  if (task) return task
  return row.kind === 'invocation' || row.kind === 'output' ? row.run : row.name
}

/**
 * The top-level record whose waterfall shows: the one the selected record sits under,
 * else the top-level record that was expanded last. Null when the address names neither.
 */
export function openRoot(page: ActivityPage): string | null {
  if (page.selectedRow) return rootOf(page.selectedRow)
  const listed = new Set(page.rows.map((row) => row.name))
  const children = new Set(
    Object.values(page.branches).flatMap((branch) => branch.rows.map((row) => row.name)),
  )
  const tasks = page.expanded.some((name) => /\/tasks\/[^/]+$/.test(name))
  return (
    page.expanded.findLast(
      (name) =>
        /\/tasks\/[^/]+$/.test(name) ||
        listed.has(name) ||
        // A Run the list does not hold is top-level only when no Task is open with it.
        (!children.has(name) && !tasks),
    ) ?? null
  )
}
