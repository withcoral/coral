// Story-only snapshot from tree 78baa17366f1bce6d57e64c702f82fd2a3030036.
// Historical comparison; not used by the production Activity page.
import {
  memo,
  useContext,
  useEffect,
  useImperativeHandle,
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
import { ProviderMarksContext } from './call-tool'
import { ContextImpact, describeContextImpact } from './context-impact'
import { contextColumn } from './task-activity.css'
import { preciseDuration } from './duration'
import { TimeBreakdown, useTaskTools, useTimeSummary } from './time-breakdown'
import { RecordLabel, recordIcon, rowDuration, rowLabel, rowStatus } from './record'
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
import * as styles from './one-line-waterfall.css'

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
  const providers = useContext(ProviderMarksContext)
  const { row, end, wait } = record
  const { open } = context
  const failed = recordFailed(row)
  return (
    <>
      <Table.Cell>
        <Table.Record
          depth={record.depth}
          // The table is one Tab stop; its keys reach the records.
          tabbable={false}
          icon={recordIcon(row, providers)}
          expanded={record.expanded}
          onToggle={record.expandable ? () => context.toggle(row.name) : undefined}
          marker={row.kind === 'invocation' && row.requiresAction ? 'needs action' : undefined}
        >
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
        </Table.Record>
      </Table.Cell>
      <Table.Cell>
        <Timeline.Track className={styles.trackCell}>
          {/* A bar says its timing on hover; this says it to a reader of the row. */}
          <span className={visuallyHidden}>{timingTitle(record)}</span>
          <Timeline.Bar
            from={from}
            to={to}
            end={end.kind}
            color={failed ? 'red' : end.kind === 'unknown' ? 'amber' : barColors[row.kind]}
            thin={row.kind === 'task'}
            selected={selected}
            title={timingTitle(record)}
            after={
              (failed || end.kind === 'unknown' || reason || wait?.to === null) && (
                <>
                  {failed && <Icon name="CircleX" size="14" color="error" />}
                  {end.kind === 'unknown' && <Icon name="CircleQuestionMark" size="14" />}
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
      </Table.Cell>
      <Table.Cell mono>{rowDuration(row)}</Table.Cell>
      <Table.Cell>
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
    <Typography.BodySmall className={styles.range}>
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
      const row = list[index]!
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
      while (end < last && list[end]!.depth > row.depth) end++
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
  const timeSummary = useTimeSummary(store, now)
  const taskTools = useTaskTools(store)
  return (
    <Timeline.Root controller={controller} className={styles.pane} data-activity-waterfall="">
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
          <Typography.HeadingSmall as="h2" truncate className={styles.title}>
            {root ? rowLabel(root) : 'Timeline'}
          </Typography.HeadingSmall>
        </div>
      )}
      {shown && (
        <div className={styles.breakdown} key="breakdown">
          <TimeBreakdown
            root={store.root}
            summary={timeSummary}
            taskTools={taskTools}
            // Adapt the historical live flag to the current TimeBreakdown API.
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
          <RangeLabel controller={controller} />
        </div>
      )}
      {shown && (
        <MinimapStrip key="minimap" controller={controller} rows={timing.rows} scale={scale} />
      )}
      {/* Always one line, so the table never moves when a read ends or a gap folds. */}
      {shown && (
        <div className={styles.status} key="status">
          {store.failed ? (
            <ReadFailed retry={reader?.retry} />
          ) : loadingShown ? (
            <Typography.BodySmall role="status" variant="secondary">
              {progress.total
                ? `Loading records… ${progress.loaded.toLocaleString('en-US')} of ${Math.max(progress.total, progress.loaded).toLocaleString('en-US')}`
                : 'Loading records…'}
            </Typography.BodySmall>
          ) : (
            mode === 'call' &&
            scale.folds.length > 0 && (
              <Typography.BodySmall variant="secondary" truncate>
                Call time compresses long gaps between records. A shaded band marks each gap.
              </Typography.BodySmall>
            )
          )}
        </div>
      )}
      <div className={styles.table} key="table">
        <Table.Container
          ariaLabel="Activity records"
          ref={port}
          // The rows you read stay where they are while records load above them, and each
          // top-level record you return to opens where you left it.
          readingKey={opened ? store.root : undefined}
          readingPaused={paused}
          revealRecord={reveal}
          focusable
          density="compact"
          layout="scroll"
          columns={[
            { width: 'minmax(220px, 280px)', label: 'RECORD' },
            {
              width: 'minmax(140px, 1fr)',
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
            { width: 72, label: 'DUR' },
            {
              // The page narrows this column to nothing where the waterfall has no room.
              width: contextColumn,
              label: 'CONTEXT',
              ariaLabel: 'Context impact',
              align: 'right',
            },
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
}: {
  controller: Timeline.TimelineController
  rows: readonly WaterfallRow[]
  scale: Timeline.TimeScale
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
      description={overviewText(counts)}
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
