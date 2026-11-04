import { endInstant, type TimelineEnd } from '@/wax/components/timeline'
import type { ActivityRow, ActivityTrace } from './types'

/** One record of a waterfall, with its interval in epoch milliseconds. */
export interface TimelineSpan {
  record: ActivityRow
  depth: number
  /** The record it sits under; null for the root. */
  parent: string | null
  start: number
  end: TimelineEnd
  /** A tool call's wait before dispatch, from `start`. */
  wait?: { until: number } | { until: 'ongoing' }
  /** A Run's Events, as points on its row. */
  marks: { record: Extract<ActivityRow, { kind: 'output' }>; at: number }[]
}

export interface Timeline {
  /** The axis, in epoch milliseconds. */
  from: number
  to: number
  /** Every record but Events, in tree order: each record, then the records under it. Events
   * are marks on their Run. */
  spans: TimelineSpan[]
}

const at = (instant: string) => Date.parse(instant)

/** The instant Coral recorded `record` ending, if any. */
export function recordEnd(record: ActivityRow): string | null {
  return record.kind === 'tool' ? record.completedAt : 'endedAt' in record ? record.endedAt : null
}

function endOf(record: ActivityRow): TimelineEnd {
  const end = recordEnd(record)
  if (end) return { kind: 'ended', at: at(end) }
  return isOngoing(record) ? { kind: 'ongoing' } : { kind: 'unknown' }
}

/** Coral's records say the work goes on. A missing end alone does not. */
export function isOngoing(record: ActivityRow) {
  switch (record.kind) {
    case 'task':
      return record.outcome === 'open'
    case 'run':
      return record.state === 'running'
    case 'invocation':
      return record.state === 'pending' || record.state === 'dispatching'
    default:
      return false
  }
}

function waitOf(record: ActivityRow): TimelineSpan['wait'] {
  if (record.kind !== 'invocation') return undefined
  // A call that never dispatched waited all its life: still, or until it ended.
  const until = record.dispatchedAt ?? record.endedAt
  if (!until) return isOngoing(record) ? { until: 'ongoing' } : undefined
  return at(until) > at(record.startedAt) ? { until: at(until) } : undefined
}

/** Lays out a trace on one time axis. `now` ends the axis while a Run or a call goes on. */
export function timeline(trace: ActivityTrace, now: number): Timeline {
  const spans: TimelineSpan[] = []
  const visit = (record: ActivityRow, depth: number, parent: string | null) => {
    const children = trace.children[record.name] ?? []
    spans.push({
      record,
      depth,
      parent,
      start: at(record.startedAt),
      end: endOf(record),
      ...(waitOf(record) && { wait: waitOf(record) }),
      marks: children.flatMap((child) =>
        child.kind === 'output' ? [{ record: child, at: at(child.startedAt) }] : [],
      ),
    })
    for (const child of children) if (child.kind !== 'output') visit(child, depth + 1, record.name)
  }
  visit(trace.root, 0, null)
  const from = Math.min(...spans.map((span) => span.start))
  // Work that goes on runs the axis to now. An open Task is a goal, not work: its bar runs
  // on past the edge, and the axis ends with its last record.
  const ends = spans.flatMap((span) =>
    span.end.kind === 'ended'
      ? [span.end.at]
      : span.end.kind === 'ongoing' && span.record.kind !== 'task'
        ? [now]
        : [span.start],
  )
  // An axis of no width would divide by zero wherever it is drawn.
  return { from, to: Math.max(...ends, from + 1), spans }
}

/** Every record of a trace by name, Events included: each record, then the records under it. */
export function recordOrder(trace: ActivityTrace): string[] {
  const visit = (record: ActivityRow): string[] => [
    record.name,
    ...(trace.children[record.name] ?? []).flatMap(visit),
  ]
  return visit(trace.root)
}

type Interval = { from: number; to: number }

/** Overlapping or touching intervals joined, in order. */
function union(intervals: Interval[]): Interval[] {
  const joined: Interval[] = []
  for (const next of intervals.toSorted((a, b) => a.from - b.from)) {
    const last = joined.at(-1)
    if (last && next.from <= last.to) last.to = Math.max(last.to, next.to)
    else if (next.to > next.from) joined.push({ ...next })
  }
  return joined
}
const length = (intervals: Interval[]) => intervals.reduce((sum, i) => sum + i.to - i.from, 0)

/** Where a root's time on the axis went, in milliseconds. The three parts add up to it. */
export interface TimeShare {
  /** Time no call or program of the root covered: the agent working between calls. */
  agent: number
  /**
   * Time calls waited before dispatch, by what they waited for: an approval, an account
   * connection, or only to start. Waits that overlap count once, under the first reason.
   */
  waiting: Record<WaitReason, number>
  /** The rest of the time calls and programs took. */
  running: number
  /** The stretches of agent time, in order. */
  gaps: Interval[]
}

/** What a call waited for before it ran. */
export type WaitReason = 'approval' | 'account connection' | 'start'

/**
 * Splits the axis of `layout` into agent time between calls, waiting, and running. `waitedFor`
 * names the request each call waited on; any other wait was only to start.
 */
export function timeShare(
  layout: Timeline,
  now: number,
  waitedFor: Record<string, Exclude<WaitReason, 'start'>> = {},
): TimeShare {
  const children = layout.spans.filter((span) => span.parent !== null)
  const covered = union(
    children.map((span) => ({ from: span.start, to: endInstant(span.start, span.end, now) })),
  )
  const waitedSpan = (span: TimelineSpan) =>
    span.wait
      ? [{ from: span.start, to: span.wait.until === 'ongoing' ? now : span.wait.until }]
      : []
  const reasonOf = (span: TimelineSpan): WaitReason => waitedFor[span.record.name] ?? 'start'
  const waits = union(children.flatMap(waitedSpan))
  // Each reason in turn takes the waits no earlier reason covered, so overlaps count once.
  const waiting = { approval: 0, 'account connection': 0, start: 0 }
  let counted: Interval[] = []
  for (const reason of ['approval', 'account connection', 'start'] as const) {
    const all = union([
      ...counted,
      ...children.filter((span) => reasonOf(span) === reason).flatMap(waitedSpan),
    ])
    waiting[reason] = length(all) - length(counted)
    counted = all
  }
  const gaps: Interval[] = []
  let cursor = layout.from
  for (const interval of covered) {
    if (interval.from > cursor) gaps.push({ from: cursor, to: interval.from })
    cursor = Math.max(cursor, interval.to)
  }
  if (layout.to > cursor) gaps.push({ from: cursor, to: layout.to })
  return { agent: length(gaps), waiting, running: length(covered) - length(waits), gaps }
}
