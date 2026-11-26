import classNames from 'classnames'
import {
  memo,
  useEffect,
  useImperativeHandle,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type MouseEvent,
  type ReactNode,
  type RefObject,
} from 'react'
import { Link } from 'react-router'
import type { ActivityTraceReader } from '@/lib/activity-trace-reader'
import { openTrace, traceProgress, traceRuns, type ActivityTraceStore } from '@/lib/activity-trace'
import { Button, Typography } from '@/wax/components'
import { Icon } from '@/wax/components/icon'
import { SegmentedControl } from '@/wax/components/segmented-control'
import { Tooltip } from '@/wax/components/tooltip'
import * as Table from '@/wax/components/table'
import * as Timeline from '@/wax/components/timeline'
import { visuallyHidden } from '@/wax/components/visually-hidden.css'
import { ContextImpact, describeContextImpact } from './context-impact'
import { preciseDuration } from './duration'
import { TimeBreakdown, useTaskTools, useTimeSummary } from './time-breakdown'
import { RecordLabel, rowDuration, rowLabel, rowStatus } from './record'
import {
  compressedTime,
  overview,
  pendingWaitReason,
  recordFailed,
  waterfall,
  type EventRow,
  type OverviewCounts,
  type SpanEnd,
  type WaterfallGap,
  type WaterfallRecord,
  type WaterfallRow,
} from './trace'
import type { ActivityRow } from './types'
import * as styles from './waterfall.css'

/** How the axis spends its width: on the time Coral worked, or on the clock. */
type TimeMode = 'call' | 'wall'
const timeModes = [
  { value: 'call', label: 'Call time' },
  { value: 'wall', label: 'Wall time' },
] as const

const NONE: ReadonlySet<string> = new Set()
/** What the waterfall lays out while no top-level record is open: nothing. */
const NO_TRACE = openTrace('')
const utcClock = (time: number) => new Date(time).toISOString().slice(11, 23)

/** The current time, read again each second while `ticking`, so work in progress grows. */
function useNow(ticking: boolean) {
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    if (!ticking) return
    const timer = setInterval(() => setNow(Date.now()), 1000)
    return () => clearInterval(timer)
  }, [ticking])
  return now
}

const starts = {
  task: 'Start',
  tool: 'Start',
  run: 'Start (acceptance)',
  invocation: 'Start (admission)',
} as const

/** A record's exact timing, as its bar says it on hover. */
function timingTitle({ row, start, end, wait }: WaterfallRecord) {
  const parts = [`${starts[row.kind]}: ${utcClock(start)} UTC.`]
  if (end.kind === 'ended') {
    const nanos =
      'durationNanos' in row ? ` (${BigInt(row.durationNanos).toLocaleString('en-US')} ns)` : ''
    parts.push(
      `End: ${utcClock(end.at)} UTC.`,
      `Duration: ${preciseDuration(end.at - start)}${nanos}.`,
    )
  } else parts.push(end.kind === 'ongoing' ? 'In progress.' : 'No recorded end.')
  if (wait)
    parts.push(
      wait.to === null
        ? 'It waits before dispatch.'
        : `Wait before dispatch: ${preciseDuration(wait.to - wait.from)}.`,
    )
  return parts.join(' ')
}

/** A mark's exact time, and the context bytes its Event sent, as it says them on hover. */
function markTitle(row: EventRow, at: number) {
  const impact =
    row.contextImpactBytes === undefined
      ? ''
      : ` · ${describeContextImpact(BigInt(row.contextImpactBytes), false).meaning}`
  return `${row.label} at ${utcClock(at)} UTC${impact}`
}

const waitReasons = {
  approval: 'Waiting for approval',
  account_connection: 'Waiting for account connection',
} as const

/** A plain left click; other clicks keep the link's own behaviour, such as a new tab. */
const plainClick = (event: MouseEvent) =>
  event.button === 0 && !event.metaKey && !event.ctrlKey && !event.shiftKey && !event.altKey

/** How a row opens its record: the address its link shows, and the pick a plain click makes. */
export interface RecordOpener {
  href: (row: ActivityRow) => string
  select: (row: ActivityRow) => void
}

/** What every row shares. It changes only when you change how the waterfall shows. */
interface RowContext {
  marks: boolean
  open: RecordOpener
  toggle: (name: string) => void
  fit: (record: WaterfallRecord) => void
}

const barColors = { task: 'neutral', tool: 'green', run: 'purple', invocation: 'blue' } as const

interface RecordRowProps {
  record: WaterfallRecord
  context: RowContext
  scale: Timeline.TimeScale
  /** Where the record's bar starts and ends on the axis. */
  from: number
  to: number
  selected: boolean
  /** The Event of this Run that is selected. The Run's row then holds the selection. */
  mark: string | null
  active: boolean
  /** What a call that still waits is waiting for, when its Run says. */
  reason: ReturnType<typeof pendingWaitReason>
}

/**
 * Whether a row shows the same thing. A read lays every record out again, and the clock
 * moves the axis of work in progress, so a row renders again only when its own record,
 * its place on the axis or its part in the selection changed. A long Run then costs one
 * row for each step of the keys, not all of them.
 */
function sameRecordRow(a: RecordRowProps, b: RecordRowProps) {
  const [before, after] = [a.record, b.record]
  if (
    before.row !== after.row ||
    before.depth !== after.depth ||
    before.expandable !== after.expandable ||
    before.expanded !== after.expanded ||
    before.end.kind !== after.end.kind ||
    before.marks.length !== after.marks.length ||
    before.marks.some((mark, index) => mark.row !== after.marks[index]!.row)
  )
    return false
  if (
    a.context !== b.context ||
    a.selected !== b.selected ||
    a.mark !== b.mark ||
    a.active !== b.active ||
    a.reason !== b.reason ||
    a.from !== b.from ||
    a.to !== b.to
  )
    return false
  // Only a wait and the marks read the scale for more than the bar's own ends.
  return a.scale === b.scale || (!after.wait && !after.marks.length)
}

/** What a record's row is, whether it is a plain row or heads the records under it. */
function recordRow({ record, context, selected, mark, active }: RecordRowProps) {
  const { row } = record
  return {
    recordKey: row.name,
    // A Run starts a block of its own: its calls touch under it.
    className: record.depth === 0 ? classNames(styles.denseRow, styles.runStart) : styles.denseRow,
    'aria-level': record.depth + 1,
    selected: selected || mark !== null,
    active,
    status: rowStatus(row),
    onClick: (event: MouseEvent<HTMLDivElement>) => {
      if ((event.target as HTMLElement).closest('a,button,input,select')) return
      // Focus rests on the clicked row, as a click on its link leaves it.
      event.currentTarget.querySelector<HTMLElement>('a[href]')?.focus({ preventScroll: true })
      context.open.select(row)
    },
    onDoubleClick: (event: MouseEvent<HTMLDivElement>) => {
      if ((event.target as HTMLElement).closest('button,[data-timeline-mark]')) return
      context.fit(record)
    },
  } satisfies Table.RowProps
}

const RecordRowView = memo(function RecordRowView(props: RecordRowProps) {
  return (
    <Table.Row {...recordRow(props)}>
      <RecordCells {...props} />
    </Table.Row>
  )
}, sameRecordRow)

/**
 * A tool call with the calls its body made under it. Its row heads them and pins while
 * they scroll, so you always see which call made the rows you read.
 */
function CallGroup({ children, ...props }: RecordRowProps & { children: ReactNode }) {
  return (
    <Table.Group
      label={rowLabel(props.record.row)}
      heading={<RecordCells {...props} />}
      headingRow={recordRow(props)}
    >
      {children}
    </Table.Group>
  )
}

const RecordCells = memo(function RecordCells({
  record,
  context,
  scale,
  from,
  to,
  selected,
  mark: selectedMark,
  reason,
}: RecordRowProps) {
  const { row, end, wait } = record
  const { open } = context
  const failed = recordFailed(row)
  const actionNeeded = row.kind === 'invocation' && row.requiresAction
  return (
    <>
      {/* The record's name, for a reader of the table and for the keys: its note shows it. */}
      <Table.Cell className={styles.readerCell}>
        <Link
          to={open.href(row)}
          data-record-link
          tabIndex={-1}
          preventScrollReset
          onClick={(event) => {
            if (!plainClick(event)) return
            event.preventDefault()
            // Safari does not focus a clicked link; focus rests on the picked row everywhere.
            event.currentTarget.focus({ preventScroll: true })
            open.select(row)
          }}
        >
          <RecordLabel row={row} />
        </Link>
      </Table.Cell>
      <Table.Cell className={styles.trackLine}>
        <div className={styles.trackBox}>
          <Timeline.Track className={classNames(styles.trackCell, Timeline.bandedTrack)}>
            {/* A bar says its timing on hover; this says it to a reader of the row. */}
            <span className={visuallyHidden}>{timingTitle(record)}</span>
            <Timeline.Bar
              from={from}
              to={to}
              end={end.kind}
              color={failed ? 'red' : end.kind === 'unknown' ? 'amber' : barColors[row.kind]}
              thin={row.kind === 'task'}
              selected={selected}
              edges
              title={timingTitle(record)}
              after={
                (failed ||
                  end.kind === 'unknown' ||
                  reason ||
                  wait?.to === null ||
                  actionNeeded) && (
                  <>
                    {failed && <Icon name="CircleX" size="14" color="error" />}
                    {end.kind === 'unknown' && <Icon name="CircleQuestionMark" size="14" />}
                    {actionNeeded && <Typography.BodySmall>needs action</Typography.BodySmall>}
                    {wait?.to === null && (
                      <>
                        <Icon name="Hourglass" size="14" />
                        <Typography.BodySmall>
                          {reason ? waitReasons[reason] : 'Waiting'}
                        </Typography.BodySmall>
                      </>
                    )}
                  </>
                )
              }
            />
            {wait && (
              <Timeline.Bar
                wait
                from={scale.toAxis(wait.from)}
                to={wait.to === null ? scale.extent : scale.toAxis(wait.to)}
                end={wait.to === null ? 'ongoing' : 'ended'}
                title={timingTitle(record)}
              />
            )}
            {record.marks.map((mark) => {
              const current = mark.row.name === selectedMark
              if (!context.marks && !current) return null
              return (
                <Timeline.Mark
                  key={mark.row.name}
                  at={scale.toAxis(mark.at)}
                  shape={
                    mark.row.eventKind === 'program.emitted'
                      ? 'diamond'
                      : mark.row.eventKind === 'program.logged'
                        ? 'circle'
                        : 'triangle'
                  }
                  label={mark.row.label}
                  title={markTitle(mark.row, mark.at)}
                  current={current}
                  href={open.href(mark.row)}
                  onClick={(event) => {
                    event.stopPropagation()
                    if (!plainClick(event)) return
                    event.preventDefault()
                    open.select(mark.row)
                  }}
                />
              )
            })}
          </Timeline.Track>
          {/* What the row's cells say, for the eye: on hover, at the cursor and when selected. */}
          <Timeline.Note at={from} width={LEAD_WIDTH} className={styles.rowNote}>
            <RecordLabel row={row} /> · {rowDuration(row)} ·{' '}
            <ContextImpact
              bytes={row.contextImpactBytes}
              partial={row.kind === 'task' && row.contextImpactPartial}
            />
          </Timeline.Note>
        </div>
      </Table.Cell>
      <Table.Cell mono className={styles.readerCell}>
        {rowDuration(row)}
      </Table.Cell>
      <Table.Cell className={styles.readerCell}>
        <ContextImpact
          bytes={row.contextImpactBytes}
          partial={row.kind === 'task' && row.contextImpactPartial}
        />
      </Table.Cell>
    </>
  )
}, sameRecordRow)

/** Where a record's bar ends on the axis: an open end reaches the axis's end. */
function endAxis(scale: Timeline.TimeScale, start: number, end: SpanEnd) {
  if (end.kind === 'ended') return scale.toAxis(end.at)
  return end.kind === 'ongoing' ? scale.extent : scale.toAxis(start)
}

/**
 * Children the trace does not hold. In view, it asks the reader for the ones next to the
 * rows you read: its first ones when you come from above, its last from below.
 */
function GapRow({
  gap,
  reader,
  port,
}: {
  gap: WaterfallGap
  reader?: ActivityTraceReader
  /** The table's scroll port, which says where in view the gap is. */
  port: RefObject<HTMLDivElement | null>
}) {
  const cell = useRef<HTMLDivElement>(null)
  const prefer = reader?.prefer
  const { parent, from, to, reason } = gap
  useEffect(() => {
    const element = cell.current
    if (!element || !prefer || reason !== 'unloaded') return
    if (typeof IntersectionObserver === 'undefined') return
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (!entry?.isIntersecting) return
        const view = entry.rootBounds ?? entry.boundingClientRect
        const below = entry.boundingClientRect.top > view.top + view.height / 2
        prefer([{ parent, at: below ? from : Math.max(from, to - 1) }])
      },
      { root: port.current },
    )
    observer.observe(element)
    return () => observer.disconnect()
  }, [prefer, port, parent, from, to, reason])
  const count = to - from
  return (
    <Table.Row aria-level={gap.depth + 1}>
      <Table.Cell fullWidth ref={cell}>
        <Table.Record depth={gap.depth}>
          {reason === 'unavailable' ? (
            'This history is unavailable.'
          ) : (
            // The line above the table says that records load. A record whose children no
            // read has counted yet says it again only to a reader of its rows.
            <span role={reader && count > 0 ? 'status' : undefined}>
              {reader
                ? count === 0
                  ? 'Loading records…'
                  : count === 1
                    ? 'Loading 1 record…'
                    : `Loading ${count.toLocaleString('en-US')} records…`
                : count === 0
                  ? 'Not loaded.'
                  : `Not loaded: ${count === 1 ? '1 record' : `${count.toLocaleString('en-US')} records`}.`}
            </span>
          )}
        </Table.Record>
      </Table.Cell>
    </Table.Row>
  )
}

/** When the selected record, or mark, is on the axis. Null when the trace does not hold it. */
function selectedTime(rows: readonly WaterfallRow[], selected: string | null, axisEnd: number) {
  for (const row of rows) {
    if (row.kind !== 'record') continue
    if (row.row.name === selected)
      return {
        from: row.start,
        to:
          row.end.kind === 'ended' ? row.end.at : row.end.kind === 'ongoing' ? axisEnd : row.start,
      }
    const mark = row.marks.find((item) => item.row.name === selected)
    if (mark) return { from: mark.at, to: mark.at }
  }
  return null
}

/** The span of time the timeline shows, in words. It reads the view, so it is always true. */
function RangeLabel({ controller }: { controller: Timeline.TimelineController }) {
  const { scale, view } = Timeline.useTimelineView(controller)
  const [from, to] = [scale.toTime(view.from), scale.toTime(view.to)]
  const folds = Timeline.foldsIn(scale, view)
  const hidden = compressedTime(folds, from, to)
  return (
    <Typography.BodySmall className={visuallyHidden}>
      {rangeText(from, to)}
      {folds.length > 0 &&
        ` · ${folds.length === 1 ? '1 gap' : `${folds.length} gaps`} compressed (${preciseDuration(hidden)})`}
    </Typography.BodySmall>
  )
}

/** "14:00:01.200 – 14:00:09.800 UTC · 8.60s". */
export const rangeText = (from: number, to: number) =>
  `${utcClock(from)} – ${utcClock(to)} UTC · ${preciseDuration(to - from)}`

export interface WaterfallProps {
  /** Null while no top-level record is open: the table then holds no record. */
  store: ActivityTraceStore | null
  /** Reads what the trace does not hold yet. Absent, the waterfall shows what `store` holds. */
  reader?: ActivityTraceReader
  /** The rows to show, as `waterfall` lays them out for the records you collapsed. */
  rows: readonly WaterfallRow[]
  selected: string | null
  /** The Run whose Event is selected. Its row shows the selection the Event has no row for. */
  holder: string | null
  /** The record the keyboard cursor rests on, while focus is in the table. */
  active: string | null
  open: RecordOpener
  toggle: (name: string) => void
  /** The record to bring into view once, when its row is there: a selection, or a key's move. */
  reveal?: string
  /** The records are concealed for now, so the reading position waits for them. */
  paused?: boolean
  tableRef: React.Ref<HTMLDivElement>
  /** Where the single-pane flow goes back to: the root list. */
  listHref: string
  /**
   * The root Task's own details, above its waterfall, in place of its name. The table then
   * holds the records under the Task, and no row of the Task itself.
   */
  header?: ReactNode
}

/**
 * One top-level record on a time axis: every record under it on a row of its own, with a
 * Run's Events as marks on the Run. Above it, where its time went, the controls of the axis
 * and its overview.
 * The table stays the same element whatever record is open, or none, so focus in it stays.
 */
export function Waterfall({
  store: opened,
  reader,
  rows,
  selected,
  holder,
  active,
  open,
  toggle,
  reveal,
  paused,
  tableRef,
  listHref,
  header,
}: WaterfallProps) {
  const store = opened ?? NO_TRACE
  const [mode, setMode] = useState<TimeMode>('call')
  const [marks, setMarks] = useState(true)
  const [ticking, setTicking] = useState(false)
  const now = useNow(ticking)
  // Every record counts for the axis, also the ones a collapsed record hides.
  const timing = useMemo(() => waterfall(store, NONE, now), [store, now])
  if (timing.ongoing !== ticking) setTicking(timing.ongoing)
  // The axis only grows during a visit, so no bar jumps when a window loads.
  const [bounds, setBounds] = useState({ root: store.root, from: timing.from, to: timing.to })
  const known = bounds.root === store.root ? bounds : timing
  const from = Math.min(known.from, timing.from)
  const to = Math.max(known.to, timing.to)
  if (bounds.root !== store.root || bounds.from !== from || bounds.to !== to)
    setBounds({ root: store.root, from, to })
  const scale = useMemo(
    () =>
      mode === 'call'
        ? Timeline.foldedScale({ from, to }, timing.busy)
        : Timeline.wallScale({ from, to }),
    [mode, from, to, timing.busy],
  )
  const controller = Timeline.useTimeline(scale)
  // From the trace alone, so the clock of work in progress renders no row again for it.
  const runs = useMemo(
    () => new Map(traceRuns(store).map((run) => [run.name, run] as const)),
    [store],
  )
  // The latest scale, for an action that reads it when you act.
  const fitting = useRef({ scale, controller })
  useEffect(() => {
    fitting.current = { scale, controller }
  })
  const context = useMemo<RowContext>(
    () => ({
      marks,
      open,
      toggle,
      fit: ({ start, end }) =>
        fitting.current.controller.focus({
          from: start,
          to:
            end.kind === 'ended'
              ? end.at
              : end.kind === 'ongoing'
                ? fitting.current.scale.to
                : start,
        }),
    }),
    [marks, open, toggle],
  )
  const selection = selectedTime(timing.rows, selected, to)
  const port = useRef<HTMLDivElement>(null)
  /** The rows [first, last). An open tool call heads a group of the rows under it. */
  const rowElements = (list: readonly WaterfallRow[], first: number, last: number) => {
    const elements: ReactNode[] = []
    for (let index = first; index < last; index++) {
      const listed = list[index]!
      // The header above states the root Task, so the table holds no row of it.
      if (taskHeads && listed.kind !== 'gap' && listed.depth === 0) continue
      // Its records then stand one level up: nothing in the table holds them.
      const row = taskHeads ? { ...listed, depth: listed.depth - 1 } : listed
      if (row.kind === 'gap') {
        elements.push(<GapRow key={row.key} gap={row} reader={reader} port={port} />)
        continue
      }
      const props = {
        record: row,
        context,
        scale,
        from: scale.toAxis(row.start),
        to: endAxis(scale, row.start, row.end),
        selected: row.row.name === selected,
        mark: row.row.name === holder ? selected : null,
        active: row.row.name === active,
        reason:
          row.row.kind === 'invocation' && row.wait?.to === null
            ? pendingWaitReason(row.row, runs.get(row.row.run))
            : null,
      }
      if (row.row.kind !== 'invocation' || !row.expandable || !row.expanded) {
        elements.push(<RecordRowView key={row.row.name} {...props} />)
        continue
      }
      let end = index + 1
      while (end < last && list[end]!.depth > listed.depth) end++
      elements.push(
        <CallGroup key={row.row.name} {...props}>
          {rowElements(list, index + 1, end)}
        </CallGroup>,
      )
      index = end - 1
    }
    return elements
  }
  useImperativeHandle(tableRef, () => port.current!)
  const progress = traceProgress(store)
  // The status line says that records load. The breakdown then does not say it again.
  const loadingShown =
    !store.failed && Boolean(reader) && !progress.complete && !progress.unavailable
  const root = store.rootRow
  // The axis and the breakdown need a record that Coral returned.
  const shown = opened !== null && !store.unavailable
  // Where the rows' tracks start and end on the page: inside the table's border and the
  // rows' padding, short of its scroll bar. The overview and the axis above the table take
  // the same span, measured, since the table's box need not line up with theirs.
  const overviewLine = useRef<HTMLDivElement>(null)
  // A Task at the root heads the waterfall from the header above it.
  const taskHeads = Boolean(header) && store.rootRow?.kind === 'task'
  const [trackSpan, setTrackSpan] = useState<{ start: number; end: number } | null>(null)
  useLayoutEffect(() => {
    // The overview mounts once the root shows.
    if (!shown) return
    const table = port.current
    const line = overviewLine.current
    if (!table || !line?.parentElement) return
    const measure = () => {
      const box = table.getBoundingClientRect()
      const around = line.parentElement!.getBoundingClientRect()
      const style = getComputedStyle(table)
      const [borderStart, borderEnd] = [style.borderLeftWidth, style.borderRightWidth].map(
        parseFloat,
      )
      const scrollBar = Math.max(
        0,
        Math.round(box.width - table.clientWidth - borderStart - borderEnd),
      )
      const span = {
        start: box.left + borderStart + ROW_PADDING_INLINE - around.left,
        end: around.right - (box.right - borderEnd - scrollBar - ROW_PADDING_INLINE),
      }
      setTrackSpan((held) =>
        held && Math.abs(held.start - span.start) < 0.01 && Math.abs(held.end - span.end) < 0.01
          ? held
          : span,
      )
    }
    measure()
    const observer = new ResizeObserver(measure)
    observer.observe(table)
    observer.observe(line.parentElement)
    return () => observer.disconnect()
  }, [shown])
  const timeSummary = useTimeSummary(store, now)
  const taskTools = useTaskTools(store)
  return (
    <Timeline.Root
      controller={controller}
      className={classNames(styles.pane, Timeline.subtleFolds)}
      data-activity-waterfall=""
    >
      {opened && (
        <div className={styles.heading} key="heading">
          <Button.InternalLink
            to={listHref}
            variant="secondary"
            size="x-small"
            className={styles.back}
            preventScrollReset
          >
            Back to list
          </Button.InternalLink>
          {!header && (
            <Typography.HeadingSmall as="h2" truncate className={styles.title}>
              {root ? rowLabel(root) : 'Timeline'}
            </Typography.HeadingSmall>
          )}
        </div>
      )}
      {opened && header}
      {shown && (
        <div className={styles.breakdown} key="breakdown">
          <TimeBreakdown
            root={store.root}
            summary={timeSummary}
            taskTools={taskTools}
            rootRow={root ?? undefined}
            announced={loadingShown}
          />
        </div>
      )}
      {shown && (
        <div className={styles.toolbar} key="toolbar" role="group" aria-label="Timeline controls">
          <SegmentedControl
            label="Time axis"
            size="small"
            options={timeModes}
            value={mode}
            onChange={setMode}
          />
          <Tooltip content={marks ? 'Hide the marks.' : 'Show the marks.'} side="bottom">
            <Button.TextButton
              size="22"
              variant="secondary"
              // The icon's shape shows the state too, not the pressed fill alone.
              icon={marks ? 'Eye' : 'EyeOff'}
              aria-pressed={marks}
              onClick={() => setMarks((value) => !value)}
            >
              Marks
            </Button.TextButton>
          </Tooltip>
          <span className={styles.zoom} role="group" aria-label="Zoom">
            <Button.IconButton
              name="Minus"
              size="22"
              variant="bare"
              tooltipText="Zoom out"
              onClick={() => controller.zoom(1.5)}
            />
            <Button.IconButton
              name="Plus"
              size="22"
              variant="bare"
              tooltipText="Zoom in"
              onClick={() => controller.zoom(1 / 1.5)}
            />
            <Button.IconButton
              name="Maximize2"
              size="22"
              variant="bare"
              tooltipText="Fit the whole timeline"
              onClick={() => controller.fit()}
            />
            <Button.IconButton
              name="ScanSearch"
              size="22"
              variant="bare"
              tooltipText="Zoom to the selected record"
              disabled={!selection}
              focusableWhenDisabled
              onClick={() => selection && controller.focus(selection)}
            />
          </span>
          {/* What loads, or why nothing does. The range the view shows, and the time it
              compresses, are for a reader of the page: the axis and the overview show them. */}
          <span className={styles.range}>
            {store.failed ? (
              <ReadFailed retry={reader?.retry} />
            ) : (
              loadingShown && (
                <Typography.BodySmall role="status" variant="secondary">
                  {progress.total
                    ? `Loading records… ${progress.loaded.toLocaleString('en-US')} of ${Math.max(progress.total, progress.loaded).toLocaleString('en-US')}`
                    : 'Loading records…'}
                </Typography.BodySmall>
              )
            )}
            <RangeLabel controller={controller} />
          </span>
        </div>
      )}
      {shown && (
        // Above the table, at the tracks' span, inside its border.
        <div
          key="minimap"
          ref={overviewLine}
          className={styles.overviewLine}
          style={
            trackSpan
              ? { marginInlineStart: trackSpan.start - 1, marginInlineEnd: trackSpan.end - 1 }
              : undefined
          }
        >
          <MinimapStrip
            controller={controller}
            rows={timing.rows}
            scale={scale}
            folded={mode === 'call' && scale.folds.length > 0}
          />
        </div>
      )}
      <div className={styles.table} key="table">
        <Table.Container
          ariaLabel="Activity records"
          className={styles.foldsTable}
          ref={port}
          // The rows you read stay where they are while records load above them, and each
          // top-level record you return to opens where you left it.
          readingKey={opened ? store.root : undefined}
          readingPaused={paused}
          revealRecord={reveal}
          focusable
          density="compact"
          groupHeadingHeight={DENSE_ROW_HEIGHT}
          layout="scroll"
          columns={[
            // Each row is its track: the record's name, duration and context are cells a
            // reader of the table reads, and its note shows them. Their columns take no room.
            { width: '0px', ariaLabel: 'Record' },
            {
              width: '1fr',
              ariaLabel: 'Timeline',
              label: shown && (
                <Timeline.Track className={styles.headTrack}>
                  <Timeline.Axis
                    controller={controller}
                    format={(time) => `+${preciseDuration(time - scale.from)}`}
                  />
                  <Timeline.Folds
                    controller={controller}
                    label={(fold) => `${preciseDuration(fold.to - fold.from)} compressed`}
                  />
                </Timeline.Track>
              ),
            },
            { width: '0px', ariaLabel: 'Duration' },
            { width: '0px', ariaLabel: 'Context impact' },
          ]}
        >
          <Table.Head />
          <Table.Body>
            {!shown && (
              <Table.Status>
                <Typography.Body>
                  {opened
                    ? 'This history is unavailable.'
                    : 'Select a task or a run to see its timeline.'}
                </Typography.Body>
              </Table.Status>
            )}
            {shown && rowElements(rows, 0, rows.length)}
          </Table.Body>
        </Table.Container>
      </div>
    </Timeline.Root>
  )
}

/** A row's height: one bar. A call that heads the calls its body made pins at it too. */
const DENSE_ROW_HEIGHT = 16
/** Where a row's note starts, before its bar. */
const LEAD_WIDTH = 0
/** A scroll table row's inline padding, as Wax sets it: where its track starts and ends. */
const ROW_PADDING_INLINE = 12

/** A read failed, so nothing more loads until you try again. */
function ReadFailed({ retry }: { retry?: () => void }) {
  return (
    <Typography.BodySmall role="alert">
      The records did not load.{' '}
      <Button.TextButton size="22" variant="link" onClick={retry}>
        Try again
      </Button.TextButton>
    </Typography.BodySmall>
  )
}

/**
 * The whole axis in a strip, with every Run and tool call drawn on it as a thin line, one
 * above the other in the order of the rows. A long waterfall shares the lanes.
 */
function MinimapStrip({
  controller,
  rows,
  scale,
  folded,
}: {
  controller: Timeline.TimelineController
  rows: readonly WaterfallRow[]
  scale: Timeline.TimeScale
  /** The axis compresses long gaps, which the strip says to a reader of the page. */
  folded: boolean
}) {
  const { view } = Timeline.useTimelineView(controller)
  // Spans closer than the strip can tell apart show as one.
  const { lanes, spans, counts } = useMemo(
    () => overview(rows, scale.to, scale.extent / 200, scale.toAxis),
    [rows, scale],
  )
  return (
    <Timeline.Minimap
      controller={controller}
      label="Visible time range"
      valueText={rangeText(scale.toTime(view.from), scale.toTime(view.to))}
      description={[
        overviewText(counts),
        folded && 'Call time compresses long gaps between records. A shaded band marks each gap.',
      ]
        .filter(Boolean)
        .join(' ')}
    >
      {spans.map((span, index) => (
        <Timeline.MinimapSpan
          key={index}
          from={scale.toAxis(span.from)}
          to={span.end === 'ongoing' ? scale.extent : scale.toAxis(span.to)}
          lane={span.lane}
          lanes={lanes}
          color={span.failed ? 'red' : barColors[span.kind]}
          end={span.end}
          flagged={span.failed}
        />
      ))}
    </Timeline.Minimap>
  )
}

const plural = (value: number, one: string, many: string) =>
  `${value.toLocaleString('en-US')} ${value === 1 ? one : many}`

/** What the strip draws, in words: its lines are too small to read one by one. */
function overviewText({ runs, calls, failed, ongoing, unknownEnds }: OverviewCounts) {
  return [
    `${plural(runs, 'run', 'runs')} and ${plural(calls, 'tool call', 'tool calls')}.`,
    failed ? `${failed.toLocaleString('en-US')} failed.` : '',
    ongoing ? `${ongoing.toLocaleString('en-US')} in progress.` : '',
    unknownEnds ? `${plural(unknownEnds, 'has', 'have')} no recorded end.` : '',
  ]
    .filter(Boolean)
    .join(' ')
}
