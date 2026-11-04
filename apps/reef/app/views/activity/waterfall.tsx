import { useContext, useMemo, useRef, useState } from 'react'
import { Link, useNavigate } from 'react-router'
import { Banner } from '@/wax/components/banner'
import * as Card from '@/wax/components/card'
import * as Tabs from '@/wax/components/tabs'
import * as Menu from '@/wax/components/menu'
import { SegmentedControl } from '@/wax/components/segmented-control'
import { ToolName } from '@/wax/components/tool-name'
import * as Button from '@/wax/components/button'
import * as DescriptionList from '@/wax/components/description-list'
import { Pill } from '@/wax/components/pill'
import * as Table from '@/wax/components/table'
import type { RowStatus } from '@/wax/components/table/row'
import { Tooltip } from '@/wax/components/tooltip'
import { InfoTip } from '@/wax/components/tooltip/info-tip'
import { Typography } from '@/wax/components/typography'
import {
  endInstant,
  focus,
  zoomAt,
  foldedTicks,
  foldTime,
  TimelineAxis,
  TimelineBar,
  TimelineGap,
  TimelineGrid,
  TimelineMark,
  TimelineMarkKey,
  TimelineMinimap,
  unfolded,
  useTimelineNow,
  useTimelineWheel,
  useTimelineWidth,
  type TimeRange,
  type TimelineEnd,
  type TimelineTone,
} from '@/wax/components/timeline'
import { visuallyHidden } from '@/wax/components/visually-hidden.css'
import { ProviderMarksContext, useToolOwner } from './call-tool'
import { contextFlow, type RecordFlow } from './context-flow'
import { formatBytes } from './context-impact'
import { offset, preciseDuration } from './duration'
import { UtcTime } from './instant'
import { RecordLabel, recordIcon, rowLabel, rowStatus } from './record'
import { ContextBySteps, SavedFunctionsTable } from './run-lenses'
import { timeline, timeShare, type TimelineSpan } from './timeline'
import {
  callsByProvider,
  reportedIssues,
  savedFunctionsOf,
  type ReportedIssue,
} from './trace-summary'
import type { ActivityRow, ActivityTrace } from './types'
import * as styles from './waterfall.css'

/** The tree column's width. Names state what the agent meant to do, so they take the room
 * first; folded gaps leave the axis little to fill. */
const NAME_WIDTH = 360
/** With the detail open beside it, names give way and wrap, so the axis keeps its room. */
const NAME_WIDTH_BESIDE_DETAIL = 280
/** The duration and context columns' widths. */
const DURATION_WIDTH = 116
const CONTEXT_WIDTH = 128
/** A program with more calls than this starts folded. */
const FOLDED_CALLS = 8
/** The shortest wait drawn on a bar. A call waits a few milliseconds to be admitted as a rule,
 * and folded gaps leave room to see it on every call; the hover card still states it. */
const VISIBLE_WAIT = 50
/** The shortest wait a bar's text names, and the shortest agent gap labelled, in milliseconds. */
const NOTICEABLE_WAIT = 1000

const kindTones: Record<ActivityRow['kind'], TimelineTone> = {
  task: 'neutral',
  tool: 'purple',
  run: 'blue',
  // Calls run like their program does; amber means only a wait.
  invocation: 'blue',
  output: 'neutral',
}
/**
 * A bar's color: red only where a call or a program ran and failed. A root's bar stays
 * neutral whatever its outcome; its status and the failure summary say how it ended.
 */
function toneOf(record: ActivityRow): TimelineTone {
  if (record.kind === 'task') return kindTones.task
  return rowStatus(record)?.tone === 'error' ? 'red' : kindTones[record.kind]
}
/** The pixels a gap label takes per character, and around its text. */
const LABEL_CHARACTER = 6.5
const LABEL_PADDING = 12

/** A status as a pill: its own color and its text, never the color alone. */
const statusPill = { success: 'green', error: 'red', warning: 'amber', neutral: 'gray' } as const

/** A row's status when it needs a mark: an error or a warning. A plain success, or an outcome
 * only its provider can judge, would mark every row of a trace that went well. */
function marked(record: ActivityRow) {
  const status = rowStatus(record)
  return status?.tone === 'error' || status?.tone === 'warning' ? status : undefined
}

/** The record failed, by its own status. */
const failed = (span: TimelineSpan) => rowStatus(span.record)?.tone === 'error'

function counted(value: number, one: string, many: string) {
  return `${value} ${value === 1 ? one : many}`
}

/** An instant to the millisecond, as UTC wall time. */
const exact = (instant: number) => `${new Date(instant).toISOString().slice(11, 23)} UTC`

/** A mark's shape: a log is a diamond, output a circle, a lifecycle Event a square. */
function markShape(record: Extract<ActivityRow, { kind: 'output' }>) {
  if (record.eventKind === 'program.logged') return 'diamond'
  // A Run's return value is output it gave back, though Coral records it with the Run's end.
  if (record.eventKind === 'program.emitted' || record.name.endsWith('/final')) return 'circle'
  return 'square'
}

/** What a record did to the agent's context, as lines of its hover card. */
function flowFacts(flow?: RecordFlow): BarFact[] {
  if (!flow) return []
  if (flow.result !== undefined)
    return [{ label: 'Tool result', value: `${formatBytes(flow.result)}, read by its program` }]
  return [
    ...(flow.into !== undefined ? [{ label: 'Into context', value: formatBytes(flow.into) }] : []),
    ...(flow.keptOut ? [{ label: 'Kept out', value: formatBytes(flow.keptOut) }] : []),
  ]
}

/** Who did the root's work: you, or the Agent its Runs name. A Task row carries no label. */
function rootActor(trace: ActivityTrace) {
  if (!trace.root.agent) return 'You'
  const runs = trace.root.kind === 'run' ? [trace.root] : (trace.children[trace.root.name] ?? [])
  const labelled = runs.find((row) => row.kind === 'run' && row.actorLabel)
  return (labelled?.kind === 'run' && labelled.actorLabel) || 'Unknown agent'
}

/** A record's duration for its column: how long it took, or how long so far. A wait and what
 * it waited for show in the bar's hatch and its hover card. */
function durationText(span: TimelineSpan, now: number) {
  if (span.end.kind === 'unknown') return '—'
  const total = preciseDuration(endInstant(span.start, span.end, now) - span.start)
  return span.end.kind === 'ongoing' ? `${total} so far` : total
}

/** One line of a bar's breakdown. */
type BarFact = { label: string; value: string }

/**
 * A bar's breakdown, for its hover card and a screen reader: when it started, what it waited for
 * and how long, how long it ran, how it ended, and what it cost. `origin` is the root's start.
 */
function barFacts(
  span: TimelineSpan,
  now: number,
  origin: number,
  extra: { waitedFor?: string; failedCalls?: number; flow?: RecordFlow } = {},
): BarFact[] {
  const end = endInstant(span.start, span.end, now)
  const waiting = span.wait?.until === 'ongoing'
  const ran = span.wait ? (waiting ? now : (span.wait.until as number)) : span.start
  const facts: BarFact[] = [
    { label: 'Started', value: `${offset(span.start - origin)} · ${exact(span.start)}` },
  ]
  if (span.wait)
    facts.push({
      label: 'Waited',
      value: `${preciseDuration(ran - span.start)}${extra.waitedFor ? ` for ${extra.waitedFor}` : ''}${waiting ? ' so far' : ''}`,
    })
  if (!waiting && span.end.kind !== 'unknown')
    facts.push({
      label: 'Ran',
      value: `${preciseDuration(end - ran)}${span.end.kind === 'ongoing' ? ' so far' : ''}`,
    })
  facts.push({
    label: 'Ended',
    value:
      span.end.kind === 'ended'
        ? exact(span.end.at)
        : span.end.kind === 'unknown'
          ? 'Unknown'
          : waiting
            ? 'Still waiting'
            : 'Still running',
  })
  if (extra.failedCalls) facts.push({ label: 'Failed calls', value: String(extra.failedCalls) })
  return [...facts, ...flowFacts(extra.flow)]
}

/** A bar's breakdown on hover: the record, then one line for each fact. */
function BarCard({ title, facts }: { title: string; facts: BarFact[] }) {
  return (
    <div className={styles.barCard}>
      <Typography.BodySmallStrong color="primary">{title}</Typography.BodySmallStrong>
      <DescriptionList.List density="compact">
        {facts.map((fact) => (
          <DescriptionList.Row key={fact.label} label={fact.label}>
            {fact.value}
          </DescriptionList.Row>
        ))}
      </DescriptionList.List>
    </div>
  )
}

/**
 * One Task or standalone Run as a waterfall: a header, a minimap of the whole root, a toolbar,
 * and its records as a tree beside one shared time axis. A row opens its record at `href`;
 * a double-click on its bar fits the axis to it.
 */
export function ActivityWaterfall({
  trace,
  selected,
  href,
  loadedAt,
}: {
  trace: ActivityTrace
  selected: string | null
  href: (record: string) => string
  /** When the page read the trace, in epoch milliseconds. */
  loadedAt: number
}) {
  const providers = useContext(ProviderMarksContext)
  const failureOwner = useToolOwner(trace.failure?.label ?? '')
  const navigate = useNavigate()
  const running = useMemo(
    () => timeline(trace, loadedAt).spans.some((span) => span.end.kind === 'ongoing'),
    [trace, loadedAt],
  )
  const now = useTimelineNow(running, loadedAt)
  const layout = useMemo(() => timeline(trace, now), [trace, now])
  const bounds = useMemo(() => ({ from: layout.from, to: layout.to }), [layout.from, layout.to])
  const share = timeShare(layout, now, trace.waitedFor)
  // The agent's long gaps fold to short breaks, so the time calls take is what the axis shows.
  // Wall time draws every gap to scale. Everything drawn is in the folded axis's units.
  const [wallTime, setWallTime] = useState(false)
  // Only gaps a reader notices fold; a short pause between calls keeps its scale.
  const fold = wallTime
    ? unfolded(bounds)
    : foldTime(
        bounds,
        share.gaps.filter((gap) => gap.to - gap.from >= NOTICEABLE_WAIT),
      )
  const axis = fold.bounds
  const at = fold.fold
  const atEnd = (end: TimelineEnd): TimelineEnd =>
    end.kind === 'ended' ? { kind: 'ended', at: at(end.at) } : end
  // A fitted view follows the root as it grows; a zoomed one keeps its range.
  const [zoomed, setZoomed] = useState<TimeRange | null>(null)
  const range = zoomed ?? axis
  const setRange = (next: TimeRange) =>
    setZoomed(next.from <= axis.from && next.to >= axis.to ? null : next)
  const fitTo = (span: TimelineSpan) =>
    setRange(focus({ from: at(span.start), to: at(endInstant(span.start, span.end, now)) }, axis))
  // A program of many calls starts folded, unless the selected record is one of them, so the
  // long tail of a parallel batch does not push the rest of the run out of view.
  const [collapsed, setCollapsed] = useState<ReadonlySet<string>>(
    () =>
      new Set(
        Object.entries(trace.children)
          .filter(
            ([name, rows]) =>
              name !== trace.root.name &&
              rows.filter((row) => row.kind === 'invocation').length > FOLDED_CALLS &&
              !rows.some((row) => row.name === selected),
          )
          .map(([name]) => name),
      ),
  )
  const [showMarks, setShowMarks] = useState(true)
  const [issuesOnly, setIssuesOnly] = useState(false)
  const [lens, setLens] = useState<RunLens>('time')
  const [groupBy, setGroupBy] = useState<'step' | 'provider'>('step')
  const area = useRef<HTMLDivElement>(null)
  const track = useRef<HTMLDivElement>(null)
  const trackWidth = useTimelineWidth(track)
  useTimelineWheel(area, track, axis, (update) => setRange(update(range)))

  // The tools the agent's summary reports a problem with: their calls carry a warning.
  const issues = reportedIssues(trace)
  const flagged = new Set(issues.flatMap((issue) => issue.calls))
  /** A row's mark: its own error or warning, else the agent's report on its tool. */
  const statusOf = (record: ActivityRow): RowStatus | undefined =>
    marked(record) ??
    (flagged.has(record.name) ? { tone: 'warning', label: 'flagged by the agent' } : undefined)
  const issueSpans = layout.spans.filter((span) => span.parent && statusOf(span.record))
  // Issues keeps each record with an issue and the records that lead to it.
  const leadingToIssue = new Set<string>()
  for (const span of issueSpans)
    for (let name: string | null = span.record.name; name;) {
      leadingToIssue.add(name)
      name = layout.spans.find((item) => item.record.name === name)?.parent ?? null
    }
  const visible = layout.spans.filter((span) => {
    if (issuesOnly && span.parent && !leadingToIssue.has(span.record.name)) return false
    for (let parent = span.parent; parent;) {
      if (collapsed.has(parent)) return false
      parent = layout.spans.find((item) => item.record.name === parent)?.parent ?? null
    }
    return true
  })
  const children = (name: string) => layout.spans.filter((span) => span.parent === name).length
  const ticks = foldedTicks(fold, range, bounds.from, trackWidth)
  const breaks = fold.breaks.map((item) => item.folded)
  /** A place on the axis as time since the root started. */
  const label = (place: number) => offset(fold.unfold(place) - bounds.from)
  const selectedSpan = layout.spans.find((span) => span.record.name === selected)
  const root = layout.spans[0]!
  // The root's own outcome is in its status; the count is of the calls under it.
  const failedCalls = layout.spans.filter((span) => span.parent && failed(span)).length
  /** Calls under a Run that failed, which its own state does not show. */
  const failedUnder = (name: string) =>
    layout.spans.filter((span) => span.parent === name && failed(span)).length
  const rootStatus = rowStatus(trace.root)
  // Logs and output are marks on their Run's bar, not rows, so they are counted apart. A
  // lifecycle Event, such as a Run's start, is neither.
  const shapes = layout.spans.flatMap((span) => span.marks.map((mark) => markShape(mark.record)))
  const logs = shapes.filter((shape) => shape === 'diamond').length
  const outputs = shapes.filter((shape) => shape === 'circle').length
  // The last gap ran to the end of the axis: what the agent did after its last call.
  const lastGap = share.gaps.at(-1)
  const tail =
    lastGap && lastGap.to === layout.to && lastGap.to - lastGap.from >= NOTICEABLE_WAIT
      ? lastGap
      : undefined
  const trailing =
    root.end.kind !== 'ended'
      ? 'no calls since'
      : failed(root)
        ? 'no calls before failure'
        : 'final answer'
  // Each gap a reader notices gets its length as a label, where the label has room: the tail
  // first, then the longest. A label may run past a short break, never over another label.
  const pixels = trackWidth / (range.to - range.from)
  const gapLabels = new Map<(typeof share.gaps)[number], string>()
  const taken: { left: number; right: number }[] = []
  for (const gap of share.gaps
    .filter((item) => item.to - item.from >= NOTICEABLE_WAIT)
    .toSorted((a, b) => (a === tail ? -1 : b === tail ? 1 : b.to - b.from - (a.to - a.from)))) {
    const text = preciseDuration(gap.to - gap.from) + (gap === tail ? ` · ${trailing}` : '')
    const width = text.length * LABEL_CHARACTER + LABEL_PADDING
    const end = (at(gap.to) - range.from) * pixels
    const middle = ((at(gap.from) + at(gap.to)) / 2 - range.from) * pixels
    const left = gap === tail ? end - width : middle - width / 2
    const right = left + width
    // The tail's label always shows: it is placed first, at the axis's end.
    if (gap !== tail && (left < 0 || right > trackWidth)) continue
    if (taken.some((item) => left < item.right && right > item.left)) continue
    taken.push({ left, right })
    gapLabels.set(gap, text)
  }
  // A later save of a function revises it: "rev 2" on the second, and so on.
  const revisions = new Map(
    savedFunctionsOf(trace).flatMap(({ saves }) =>
      saves.slice(1).map((row, index) => [row.name, `rev ${index + 2}`] as const),
    ),
  )
  // What reached the agent and what programs kept from it, apart: Coral's signed total nets one
  // against the other. Bars compare each record with the record that moved the most.
  const flow = contextFlow(trace)
  const records = [...flow.records.values()]
  const mostInto = Math.max(1, ...records.map((item) => item.into ?? 0))
  const counts = [
    counted(layout.spans.length, 'record', 'records'),
    logs > 0 && counted(logs, 'log', 'logs'),
    outputs > 0 && counted(outputs, 'program output', 'program outputs'),
    counted(failedCalls, 'failed call', 'failed calls'),
  ].filter(Boolean)

  /** A record's row, then, under the root, the agent's time between calls. */
  const spanRows = (span: TimelineSpan, depth: number) => {
    const count = children(span.record.name)
    const open = !collapsed.has(span.record.name)
    const unreadable = trace.unavailable.includes(span.record.name)
    const facts = barFacts(span, now, bounds.from, {
      waitedFor: trace.waitedFor?.[span.record.name],
      failedCalls: span.record.kind === 'run' ? failedUnder(span.record.name) : 0,
      flow: flow.records.get(span.record.name),
    })
    const wait = span.wait && (span.wait.until === 'ongoing' ? now : span.wait.until)
    // The root's column holds the total; its children's hold their own bytes. A call's
    // result reached its program, not the agent: its panel states its size.
    const into = span.parent === null ? flow.total.into : flow.records.get(span.record.name)?.into
    const failedHere = span.record.kind === 'run' ? failedUnder(span.record.name) : 0
    const marker = [
      unreadable && 'calls hidden',
      failedHere > 0 && counted(failedHere, 'call failed', 'calls failed'),
      count > 0 && !open && counted(count, 'call', 'calls'),
      revisions.get(span.record.name),
    ]
      .filter(Boolean)
      .join(' · ')
    // The agent's time between calls sits under the root, where it is spent.
    const gaps = span.parent === null && share.gaps.length > 0 && (
      <Table.Row key="agent-gaps">
        <Table.Cell>
          <Table.Record wrap depth={1} icon={{ name: 'Bot', label: 'agent' }}>
            Agent · between calls
          </Table.Record>
        </Table.Cell>
        <Table.Cell>
          <div className={styles.track}>
            <span className={visuallyHidden}>
              {`${preciseDuration(share.agent)} between calls in ${share.gaps.length} stretches`}
            </span>
            <TimelineGrid range={range} ticks={ticks} />
            {share.gaps.map((gap) => (
              <TimelineGap
                key={gap.from}
                range={range}
                from={at(gap.from)}
                to={at(gap.to)}
                label={gapLabels.get(gap)}
                labelAt={gap === tail ? 'end' : 'middle'}
                emphasis={gap === tail}
              />
            ))}
          </div>
        </Table.Cell>
        <Table.Cell>{preciseDuration(share.agent)}</Table.Cell>
        <Table.Cell />
      </Table.Row>
    )
    return [
      <Table.Row
        key={span.record.name}
        to={href(span.record.name)}
        selected={span.record.name === selected}
        status={statusOf(span.record)}
        recordKey={span.record.name}
        preventScrollReset
      >
        <Table.Cell>
          <Table.Record
            wrap
            depth={depth}
            expanded={count ? open : undefined}
            onToggle={
              count
                ? () =>
                    setCollapsed((current) => {
                      const next = new Set(current)
                      if (open) next.add(span.record.name)
                      else next.delete(span.record.name)
                      return next
                    })
                : undefined
            }
            icon={recordIcon(span.record, providers)}
            marker={marker || undefined}
          >
            <Table.RowLink to={href(span.record.name)}>
              <RecordLabel row={span.record} wrap />
            </Table.RowLink>
          </Table.Record>
        </Table.Cell>
        <Table.Cell>
          {/* The breakdown shows on hover; a screen reader reads it with the row. */}
          <Tooltip
            delay={150}
            maxWidth={360}
            content={<BarCard title={rowLabel(span.record)} facts={facts} />}
          >
            <div className={styles.track} onDoubleClick={() => fitTo(span)}>
              <span className={visuallyHidden}>
                {facts.map((fact) => `${fact.label} ${fact.value}.`).join(' ')}
              </span>
              <TimelineGrid range={range} ticks={ticks} />
              <TimelineBar
                range={range}
                start={at(span.start)}
                end={atEnd(span.end)}
                now={at(now)}
                waitUntil={
                  wait !== undefined && wait - span.start >= VISIBLE_WAIT ? at(wait) : undefined
                }
                tone={toneOf(span.record)}
                // A program spans its calls as a bracket over them.
                shape={span.record.kind === 'run' ? 'bracket' : 'solid'}
              />
              {showMarks &&
                span.marks.map((mark) => (
                  <TimelineMark
                    key={mark.record.name}
                    range={range}
                    at={at(mark.at)}
                    shape={markShape(mark.record)}
                    label={`${mark.record.label} at ${offset(mark.at - bounds.from)}`}
                    onSelect={() => navigate(href(mark.record.name), { preventScrollReset: true })}
                  />
                ))}
            </div>
          </Tooltip>
        </Table.Cell>
        <Table.Cell>{durationText(span, now)}</Table.Cell>
        <Table.Cell>
          <BytesCell
            bytes={into}
            most={span.parent === null ? undefined : mostInto}
            meaning="into the agent's context"
          />
        </Table.Cell>
      </Table.Row>,
      gaps,
    ]
  }
  /** A provider's calls under one heading, with a bracket from its first call to its last. */
  const groupRow = (row: Extract<WaterfallRow, { kind: 'group' }>) => {
    const open = !collapsed.has(row.key)
    return (
      <Table.Row key={row.key}>
        <Table.Cell>
          <Table.Record
            wrap
            depth={1}
            expanded={open}
            onToggle={() =>
              setCollapsed((current) => {
                const next = new Set(current)
                if (open) next.add(row.key)
                else next.delete(row.key)
                return next
              })
            }
            marker={
              counted(row.count, 'call', 'calls') + (row.failed ? ` · ${row.failed} failed` : '')
            }
          >
            <ProviderName callPath={row.callPath} />
          </Table.Record>
        </Table.Cell>
        <Table.Cell>
          <div className={styles.track}>
            <TimelineGrid range={range} ticks={ticks} />
            <TimelineBar
              range={range}
              start={at(row.from)}
              end={{ kind: 'ended', at: at(row.to) }}
              now={at(now)}
              tone="blue"
              shape="bracket"
            />
          </div>
        </Table.Cell>
        {/* The time its calls ran, overlaps counted once: not the stretch from first to last. */}
        <Table.Cell>{preciseDuration(row.busy)}</Table.Cell>
        <Table.Cell />
      </Table.Row>
    )
  }
  // By program step, each record sits under the Run or Task that made it. By provider, the
  // calls sit under their provider instead, and the programs that made them leave the tree.
  const rows: WaterfallRow[] =
    groupBy === 'step'
      ? visible.map((span) => ({ kind: 'span', span, depth: span.depth }))
      : [
          ...visible
            .filter((span) => span.record.kind !== 'run' && span.record.kind !== 'invocation')
            .map((span): WaterfallRow => ({ kind: 'span', span, depth: span.depth })),
          ...callsByProvider(trace).flatMap((provider): WaterfallRow[] => {
            const calls = layout.spans
              .filter(
                (span) =>
                  span.record.kind === 'invocation' &&
                  span.record.callPath.startsWith(`${provider.callRoot}.`) &&
                  (!issuesOnly || statusOf(span.record)),
              )
              .toSorted((a, b) => a.start - b.start)
            if (!calls.length) return []
            const key = `provider:${provider.callRoot}`
            return [
              {
                kind: 'group',
                key,
                callPath: provider.callPath,
                from: calls[0]!.start,
                to: Math.max(...calls.map((span) => endInstant(span.start, span.end, now))),
                count: calls.length,
                failed: calls.filter(failed).length,
                busy: busyTime(
                  calls.map((span) => ({
                    from: span.start,
                    to: endInstant(span.start, span.end, now),
                  })),
                ),
              },
              ...(collapsed.has(key)
                ? []
                : calls.map((span): WaterfallRow => ({ kind: 'span', span, depth: 2 }))),
            ]
          }),
        ]

  const functions = savedFunctionsOf(trace)
  const keptOut = flow.total.providerData - flow.total.output
  const missing = [
    flow.total.running > 0 &&
      counted(flow.total.running, 'program still running', 'programs still running'),
    flow.total.unmeasured > 0 &&
      counted(flow.total.unmeasured, 'step not measured', 'steps not measured'),
  ].filter((part): part is string => Boolean(part))
  const metrics: (Tabs.MetricTabProps & { value: RunLens })[] = [
    {
      value: 'time',
      icon: 'Clock',
      label: 'Time',
      metric: `${preciseDuration(layout.to - layout.from)}${root.end.kind === 'ongoing' ? ' so far' : ''}`,
      details: ['Start to end'],
      // The agent's model and Coral, apart: the run's time is mostly the model's.
      parts: [
        { metric: preciseDuration(share.agent), details: ['Agent model'] },
        { metric: preciseDuration(share.running), details: ['Coral'] },
        // A wait names what it waited for, as the failure and the bars do.
        ...(
          [
            [share.waiting.approval, 'Waiting for approval'],
            [share.waiting['account connection'], 'Waiting for an account'],
            [share.waiting.start, 'Waiting to start'],
          ] as const
        )
          .filter(([value]) => value > 0)
          .map(([value, reason]) => ({ metric: preciseDuration(value), details: [reason] })),
      ],
    },
    {
      value: 'context',
      icon: 'Layers',
      label: 'Context kept out',
      metric: flow.records.size ? formatBytes(Math.max(0, keptOut)) : '—',
      variant: keptOut > 0 ? 'success' : undefined,
      details: flow.records.size
        ? [
            ...(flow.total.providerData > 0
              ? [
                  `${formatBytes(flow.total.providerData)} read · ${formatBytes(flow.total.output)} sent`,
                ]
              : []),
            `${formatBytes(flow.total.into)} into agent context`,
            ...missing,
          ]
        : ['Not measured'],
    },
    ...(functions.length
      ? [
          {
            value: 'functions' as const,
            icon: 'Save' as const,
            label: 'Saved functions',
            metric: String(functions.length),
            details: [
              `${functions.reduce((sum, item) => sum + item.saves.length - 1, 0)} revised · ${functions.filter((item) => item.tested).length} tested`,
            ],
          },
        ]
      : []),
  ]
  return (
    <section className={styles.waterfall} aria-label="Timeline">
      <header className={styles.header}>
        <div className={styles.title}>
          <Typography.HeadingSmall as="h2" color="primary" className={styles.heading}>
            {rowLabel(trace.root)}
          </Typography.HeadingSmall>
          {rootStatus && (
            <Pill color={statusPill[rootStatus.tone]} size="small">
              {rootStatus.label}
            </Pill>
          )}
        </div>
        {/* What the agent said it did: the record's answer, so it leads. */}
        {trace.feedback && (
          <section aria-label="Agent summary">
            <Typography.Body color="secondary" className={styles.agentText}>
              {trace.feedback}
            </Typography.Body>
          </section>
        )}
        <Typography.BodySmall>
          <UtcTime value={trace.root.startedAt} /> UTC · {rootActor(trace)} · {counts.join(' · ')}
        </Typography.BodySmall>
        {!trace.complete && (
          <Typography.BodySmall color="warning" role="status">
            This timeline shows only the first records.
          </Typography.BodySmall>
        )}
        {issues.map((issue) => (
          <IssueBanner
            key={issue.tool}
            issue={issue}
            highlighted={issuesOnly}
            onHighlight={() => setIssuesOnly(!issuesOnly)}
          />
        ))}
        {trace.failure && (
          <Banner
            variant="error"
            title={
              failureOwner
                ? `${failureOwner.relativePath} failed in ${failureOwner.provider.displayName}`
                : `${trace.failure.label} failed`
            }
          >
            <div className={styles.failure}>
              <Typography.CodeSmallInline className={styles.error}>
                {trace.failure.code}: {trace.failure.message}
              </Typography.CodeSmallInline>
              {trace.failure.sameBefore !== undefined && (
                <Typography.BodySmall>
                  {trace.failure.sameBefore === 1
                    ? 'The earlier call of this tool failed with the same error.'
                    : `${trace.failure.sameBefore} earlier calls of this tool failed with the same error.`}
                </Typography.BodySmall>
              )}
              {trace.failure.waitedFor && (
                <Typography.BodySmall>
                  It waited for {trace.failure.waitedFor} before it ran.
                </Typography.BodySmall>
              )}
              {failed(root) && tail && (
                <Typography.BodySmall>
                  The agent then worked {preciseDuration(tail.to - tail.from)} without another call.
                </Typography.BodySmall>
              )}
              <span className={styles.failureActions}>
                <Button.InternalLink
                  to={href(trace.failure.record)}
                  variant="secondary"
                  size="small"
                >
                  {trace.failure.record.includes('/invocations/') ? 'Show call' : 'Show program'}
                </Button.InternalLink>
                <Button.CopyButton
                  textToCopy={`${trace.failure.code}: ${trace.failure.message}`}
                  size="22"
                  variant="secondary"
                >
                  Copy error
                </Button.CopyButton>
              </span>
            </div>
          </Banner>
        )}
      </header>
      {/* The run's measures, as Overview heads its summary: each tab opens its breakdown. */}
      <Card.Container flush className={styles.lensCard}>
        <Tabs.Root
          value={lens}
          onValueChange={(value) => setLens(value as RunLens)}
          className={styles.lensRoot}
        >
          <Tabs.MetricList aria-label="This run">
            {metrics.map((metric) => (
              <Tabs.MetricTab key={metric.value} {...metric} />
            ))}
          </Tabs.MetricList>
          <Tabs.Panel value="time" className={styles.lensPanel}>
            <div className={styles.minimap}>
              <TimelineMinimap
                bounds={axis}
                range={range}
                onRange={setRange}
                lanes={layout.spans.map((span) => ({
                  start: at(span.start),
                  end: at(endInstant(span.start, span.end, now)),
                  tone: toneOf(span.record),
                }))}
                ticks={foldedTicks(fold, axis, bounds.from, trackWidth)}
                tickLabel={label}
                label={`${label(range.from)} to ${label(range.to)}`}
              />
            </div>
            <div className={styles.toolbar}>
              <Button.Container variant="secondary" size="22" onClick={() => setZoomed(null)}>
                <Button.Text>Fit</Button.Text>
              </Button.Container>
              <span role="group" aria-label="Zoom" className={styles.zoom}>
                <Button.IconButton
                  name="Minus"
                  size="22"
                  variant="secondary"
                  ariaLabel="Zoom out"
                  disabled={!zoomed}
                  onClick={() => setRange(zoomAt(range, axis, 0.5, 2))}
                />
                <Typography.BodySmall className={styles.zoomValue}>
                  {`${((axis.to - axis.from) / (range.to - range.from)).toFixed(2)}×`}
                </Typography.BodySmall>
                <Button.IconButton
                  name="Plus"
                  size="22"
                  variant="secondary"
                  ariaLabel="Zoom in"
                  onClick={() => setRange(zoomAt(range, axis, 0.5, 0.5))}
                />
              </span>
              {selectedSpan && (
                <Button.Container variant="secondary" size="22" onClick={() => fitTo(selectedSpan)}>
                  <Button.Text>Zoom to selection</Button.Text>
                </Button.Container>
              )}
              <SegmentedControl
                label="Time scale"
                size="small"
                value={wallTime ? 'wall' : 'call'}
                options={[
                  { value: 'call', label: 'Call time' },
                  { value: 'wall', label: 'Wall time' },
                ]}
                onChange={(value) => {
                  setWallTime(value === 'wall')
                  setZoomed(null)
                }}
              />
              <Button.Container
                variant="secondary"
                size="22"
                aria-pressed={showMarks}
                onClick={() => setShowMarks(!showMarks)}
              >
                <Button.Icon name={showMarks ? 'Eye' : 'EyeOff'} />
                <Button.Text>Logs and output</Button.Text>
              </Button.Container>
              <Menu.Container>
                <Menu.Trigger render={<Button.Container variant="secondary" size="22" />}>
                  <Button.Text>
                    Group by: {groupBy === 'step' ? 'Program step' : 'Provider'}
                  </Button.Text>
                  <Button.Icon name="ChevronDown" />
                </Menu.Trigger>
                <Menu.Content align="start">
                  <Menu.RadioGroup
                    value={groupBy}
                    onValueChange={(value) => setGroupBy(value as 'step' | 'provider')}
                  >
                    <Menu.RadioItem value="step">Program step</Menu.RadioItem>
                    <Menu.RadioItem value="provider">Provider</Menu.RadioItem>
                  </Menu.RadioGroup>
                </Menu.Content>
              </Menu.Container>
              <Button.Container
                variant="secondary"
                size="22"
                aria-pressed={issuesOnly}
                disabled={!issueSpans.length}
                onClick={() => setIssuesOnly(!issuesOnly)}
              >
                <Button.Text>Issues {issueSpans.length}</Button.Text>
              </Button.Container>
              <Typography.BodySmall role="status">
                Showing {label(range.from)} to {label(range.to)}
              </Typography.BodySmall>
              <InfoTip ariaLabel="How to zoom">
                Call time shows long agent gaps as short breaks. Select Wall time to show them to
                scale. Hold Cmd or Ctrl and scroll to zoom. Hold Shift and scroll to move.
                Double-click a bar to fit it.
              </InfoTip>
            </div>
            <div className={styles.rows} ref={area}>
              <Table.Container
                ariaLabel="Timeline records"
                layout="fixed"
                bounded
                density="compact"
                columns={[
                  { label: 'Record', width: selected ? NAME_WIDTH_BESIDE_DETAIL : NAME_WIDTH },
                  { ariaLabel: 'Time', width: 'fill' },
                  { label: 'Duration', width: DURATION_WIDTH },
                  { label: 'Agent context', width: CONTEXT_WIDTH },
                ]}
              >
                <Table.Head>
                  <Table.Row>
                    <Table.Heading>Record</Table.Heading>
                    <Table.Heading ariaLabel="Time">
                      <div className={styles.axisHeading} ref={track}>
                        <TimelineAxis range={range} ticks={ticks} label={label} breaks={breaks} />
                      </div>
                    </Table.Heading>
                    <Table.Heading>Duration</Table.Heading>
                    <Table.Heading>Agent context</Table.Heading>
                  </Table.Row>
                </Table.Head>
                <Table.Body>
                  {rows.flatMap((row) =>
                    row.kind === 'group' ? [groupRow(row)] : spanRows(row.span, row.depth),
                  )}
                </Table.Body>
              </Table.Container>
            </div>
            {/* What each shape means, once, under the rows it explains. */}
            <Typography.BodySmall className={styles.legend}>
              <span className={styles.legendItem}>
                <span aria-hidden className={styles.legendBracket} /> program, from its start to its
                end
              </span>
              {showMarks && (
                <>
                  <span className={styles.legendItem}>
                    <TimelineMarkKey shape="circle" /> output
                  </span>
                  <span className={styles.legendItem}>
                    <TimelineMarkKey shape="diamond" /> log
                  </span>
                </>
              )}
              <span className={styles.legendItem}>
                <span aria-hidden className={styles.legendDots} /> agent working between calls
              </span>
              <span>Agent context: bytes that reached the agent</span>
            </Typography.BodySmall>
          </Tabs.Panel>
          <Tabs.Panel value="context" className={styles.lensPanel}>
            <ContextBySteps spans={layout.spans} flow={flow} href={href} />
          </Tabs.Panel>
          {functions.length > 0 && (
            <Tabs.Panel value="functions" className={styles.lensPanel}>
              <SavedFunctionsTable
                functions={functions}
                workspace={trace.root.workspace}
                origin={bounds.from}
                href={href}
              />
            </Tabs.Panel>
          )}
        </Tabs.Root>
      </Card.Container>
    </section>
  )
}

/**
 * Bytes in a context column, over a bar in proportion to the `most` of any record in it. The
 * root's totals have no bar: they are the sums of the rest. The record that moved the most
 * reads strongest.
 */
function BytesCell({ bytes, most, meaning }: { bytes?: number; most?: number; meaning: string }) {
  if (!bytes) return null
  return (
    <div className={styles.context}>
      <Typography.CodeSmallInline
        variant={bytes === most || most === undefined ? 'primary' : 'secondary'}
        aria-label={`${formatBytes(bytes)} ${meaning}`}
        title={`${bytes.toLocaleString('en-US')} bytes ${meaning}. This measures payload bytes, not tokens.`}
        className={styles.contextValue}
      >
        {formatBytes(bytes)}
      </Typography.CodeSmallInline>
      {most !== undefined && (
        <span
          aria-hidden
          className={styles.contextBar}
          style={{ width: `${(bytes / most) * 100}%` }}
        />
      )}
    </div>
  )
}

/** A row of the timeline: a record, or a heading over one provider's calls. */
type WaterfallRow =
  | { kind: 'span'; span: TimelineSpan; depth: number }
  | {
      kind: 'group'
      key: string
      /** One of the provider's calls, to name the provider by. */
      callPath: string
      from: number
      to: number
      count: number
      failed: number
      /** Milliseconds any of its calls was running. */
      busy: number
    }

/** How long any of `intervals` covers, overlaps counted once. */
function busyTime(intervals: { from: number; to: number }[]) {
  let total = 0
  let reach = -Infinity
  for (const { from, to } of intervals.toSorted((a, b) => a.from - b.from)) {
    if (to <= reach) continue
    total += to - Math.max(from, reach)
    reach = to
  }
  return total
}

/** A provider by its logo and name, from one of its calls. */
function ProviderName({ callPath }: { callPath: string }) {
  const owner = useToolOwner(callPath)
  return (
    <ToolName
      provider={
        owner ? { name: owner.provider.displayName, icons: owner.provider.icons } : undefined
      }
    >
      {owner?.provider.displayName ?? callPath.split('.').slice(0, 3).join('.')}
    </ToolName>
  )
}

/**
 * A problem the agent reported with one tool, on one line: the tool and its provider, and a
 * control that shows only the records with an issue. Details hold the agent's own sentence and
 * a link to the provider.
 */
function IssueBanner({
  issue,
  highlighted,
  onHighlight,
}: {
  issue: ReportedIssue
  highlighted: boolean
  onHighlight: () => void
}) {
  const [open, setOpen] = useState(false)
  const owner = useToolOwner(issue.callPath)
  const provider = owner?.provider.displayName ?? issue.callPath.split('.').slice(0, 3).join('.')
  return (
    <Banner
      variant="warning"
      title={
        <span className={styles.issueTitle}>
          <Button.IconButton
            name={open ? 'ChevronDown' : 'ChevronRight'}
            size="22"
            variant="bare"
            ariaLabel={open ? 'Hide the agent’s report' : 'Show the agent’s report'}
            aria-expanded={open}
            onClick={() => setOpen(!open)}
          />
          {`Provider issue reported by the agent · ${provider} · ${issue.tool}`}
        </span>
      }
      action={
        <Button.Container
          variant="secondary"
          size="22"
          aria-pressed={highlighted}
          onClick={onHighlight}
        >
          <Button.Text>
            {highlighted
              ? 'Show all records'
              : `Highlight ${counted(issue.calls.length, 'call', 'calls')}`}
          </Button.Text>
        </Button.Container>
      }
    >
      {open && (
        <div className={styles.failure}>
          <Typography.BodySmall color="primary">“{issue.sentence}”</Typography.BodySmall>
          <Typography.BodySmall>
            <Link to={`/${issue.provider}`}>Open {provider}</Link>
          </Typography.BodySmall>
        </div>
      )}
    </Banner>
  )
}

/** The run's measures, each a tab that opens its breakdown. */
type RunLens = 'time' | 'context' | 'functions'
