import { useEffect, useId, useRef, useState, type KeyboardEvent, type ReactNode } from 'react'
import { Link, useLocation, useNavigate } from 'react-router'
import { CopyMcpEndpoint } from '@/components/copy-mcp-endpoint'
import { EmptyPage } from '@/components/empty-page'
import * as Button from '@/wax/components/button'
import * as Dialog from '@/wax/components/dialog'
import { Pill } from '@/wax/components/pill'
import * as Table from '@/wax/components/table'
import { Typography } from '@/wax/components/typography'
import { activityDays } from '@/lib/activity-list'
import { visuallyHidden } from '@/wax/components/visually-hidden.css'
import { describeContextImpact } from './context-impact'
import { ActivityDetail, runContextOf } from './detail'
import { duration, ElapsedDuration } from './duration'
import { ExecutionSourceSplit } from './execution-source-split'
import { dayLabel, UtcTime } from './instant'
import { ActivityConnectedContext, restoreFirst, useActivityArchived } from './live'
import { revealHref, shownRecordOf, useActivityNavigation } from './navigation'
import { PanelClose } from './panel'
import * as panelStyles from './panel.css'
import { RecordLabel, rowStatus } from './record'
import * as styles from './task-activity.css'
import type { ActivityLoadControl, ActivityPage, ActivityRow, ActivityViewProps } from './types'
import { isOngoing, recordEnd, recordOrder } from './timeline'
import { ActivityWaterfall } from './waterfall'
import { useUnsentComment } from '../actions/review'

/** Controls that read the arrows themselves keep them. */
const ARROW_CONTROLS =
  'input, textarea, select, [contenteditable="true"], [role="slider"], [role="tablist"], [role="radiogroup"], [role="menu"], [role="listbox"], [role="combobox"]'

/** How long a root took, or has taken so far while it runs. */
function rowDuration(row: ActivityRow) {
  if ('durationNanos' in row)
    return (
      <ElapsedDuration
        milliseconds={Number(row.durationNanos) / 1_000_000}
        running={isOngoing(row)}
      />
    )
  const end = recordEnd(row)
  return end ? duration((Date.parse(end) - Date.parse(row.startedAt)) * 1_000_000) : '—'
}

function LoadRecords({
  control,
  direction,
}: {
  control: ActivityLoadControl
  direction: 'older' | 'newer'
}) {
  return (
    <Table.LoadMore
      onLoad={control.load}
      loading={control.loading}
      automatic={control.automatic}
      error={control.failed && 'The records did not load.'}
      retryLabel="Try again"
      loadingLabel={`Loading ${direction} records…`}
    >
      {`Load ${direction} records`}
    </Table.LoadMore>
  )
}

/**
 * One root in the list: its outcome and name on one line, then its start, its duration and the
 * context it added or saved, so a row stays two lines high.
 */
function RootRow({ row, href, open }: { row: ActivityRow; href: string; open: boolean }) {
  return (
    <Table.Row
      to={href}
      selected={open}
      status={rowStatus(row)}
      recordKey={row.name}
      preventScrollReset
    >
      <Table.Cell>
        {/* Each root is one outcome to scan, so its status mark leads, not its kind. */}
        <Table.Record everyStatus stacked metadata={<RootFacts row={row} />}>
          <Table.RowLink to={href}>
            <RecordLabel row={row} />
          </Table.RowLink>
        </Table.Record>
      </Table.Cell>
    </Table.Row>
  )
}

/** A root's start, then its duration and its context impact as chips. */
function RootFacts({ row }: { row: ActivityRow }) {
  const bytes = row.kind === 'task' || row.kind === 'run' ? row.contextImpactBytes : undefined
  const impact =
    bytes === undefined
      ? undefined
      : describeContextImpact(BigInt(bytes), row.kind === 'task' && !!row.contextImpactPartial)
  // A root with no measured duration and no end has none to show.
  const timed = 'durationNanos' in row || recordEnd(row) !== null
  return (
    <span className={styles.rootFacts}>
      <UtcTime value={row.startedAt} />
      {timed && (
        <Pill size="small" color="graySubtle">
          {rowDuration(row)}
        </Pill>
      )}
      {impact && (
        // Saved context is good news; added context is the cost of the work, not a failure.
        <Pill
          size="small"
          color={BigInt(bytes!) < 0n ? 'green' : 'graySubtle'}
          title={impact.title}
        >
          <span aria-hidden>{impact.label}</span>
          <span className={visuallyHidden}>{impact.meaning}</span>
        </Pill>
      )}
    </span>
  )
}

/** The list's rows, under the UTC day each started. */
function DayGroups({
  rows,
  href,
  open = null,
}: {
  rows: ActivityRow[]
  href: (row: ActivityRow) => string
  /** The root whose timeline is open. */
  open?: string | null
}) {
  return activityDays(rows).map(({ day, key, rows: dayRows }) => {
    const label = dayLabel(day)
    return (
      <Table.Group
        key={key}
        label={label}
        // The day orients; the records stay the strongest text.
        heading={
          <Table.Cell fullWidth>
            {/* The rows under it give their start as a UTC time, as the detail does. */}
            <Typography.BodySmall color="secondary">{label} · UTC</Typography.BodySmall>
          </Table.Cell>
        }
      >
        {dayRows.map((row) => (
          <RootRow key={row.name} row={row} href={href(row)} open={row.name === open} />
        ))}
      </Table.Group>
    )
  })
}

function EmptyActivity({ page, connect }: Pick<ActivityViewProps, 'page' | 'connect'>) {
  const archived = useActivityArchived()
  return (
    <EmptyPage
      fullSize={false}
      iconName="Activity"
      title={archived ? 'No activity' : 'No activity yet'}
      description={
        archived
          ? restoreFirst(archived, 'start new runs')
          : connect
            ? 'Connect an MCP client to this workspace. Its calls show here.'
            : page.agent
              ? "The agent's calls show here."
              : 'Your calls show here.'
      }
      action={connect && <CopyMcpEndpoint {...connect} />}
    />
  )
}

/** A short list of the latest roots, each opening on its Activity page. */
function Glimpse({
  page,
  more,
  connect,
}: Required<Pick<ActivityViewProps, 'more'>> & Pick<ActivityViewProps, 'page' | 'connect'>) {
  const roots = page.rows.slice(0, more.roots)
  return (
    <div className={styles.list}>
      <Table.Container
        ariaLabel="Activity records"
        density="compact"
        layout="fixed"
        columns={[{ label: 'Record', width: 'fill' }]}
      >
        <Table.Head />
        <Table.Body>
          <DayGroups rows={roots} href={(row) => revealHref(row)} />
          {!page.rows.length && !page.listUnavailable && (
            <Table.Status>
              <EmptyActivity page={page} connect={connect} />
            </Table.Status>
          )}
        </Table.Body>
      </Table.Container>
      {(page.hasAfter || page.rows.length > roots.length) && (
        <div className={styles.filters}>
          <Button.InternalLink to={more.to} variant="bare" size="x-small">
            {more.label}
          </Button.InternalLink>
        </div>
      )}
    </div>
  )
}

/**
 * The list of Tasks and standalone Runs behind one button, so the open timeline keeps the
 * page's width. The button counts the runs and the failed ones. Opening another run, Escape or
 * a click outside closes it. Key it by the open run, so it closes when that changes.
 */
function RecentRuns({ rows, children }: { rows: ActivityRow[]; children: ReactNode }) {
  const [open, setOpen] = useState(false)
  const box = useRef<HTMLDivElement>(null)
  const id = useId()
  useEffect(() => {
    if (!open) return
    const outside = (event: PointerEvent) => {
      if (!box.current?.contains(event.target as Node)) setOpen(false)
    }
    document.addEventListener('pointerdown', outside)
    return () => document.removeEventListener('pointerdown', outside)
  }, [open])
  const failed = rows.filter((row) => rowStatus(row)?.tone === 'error').length
  return (
    <div
      ref={box}
      className={styles.recentRuns}
      onKeyDown={(event) => {
        if (event.key === 'Escape' && open) {
          event.stopPropagation()
          setOpen(false)
        }
      }}
    >
      <Button.Container
        variant="secondary"
        size="32"
        aria-expanded={open}
        aria-controls={id}
        onClick={() => setOpen(!open)}
      >
        <Button.Text>
          Recent runs {rows.length}
          {failed > 0 && ` · ${failed} failed`}
        </Button.Text>
        <Button.Icon name={open ? 'ChevronUp' : 'ChevronDown'} />
      </Button.Container>
      {open && (
        <div id={id} role="region" aria-label="Recent runs" className={styles.recentPanel}>
          {children}
        </div>
      )}
    </div>
  )
}

/** Where the selected call or Event sits among its Run's records, for the detail's slide. */
function runPosition(page: ActivityPage) {
  const row = page.selectedRow
  if (row?.kind !== 'invocation' && row?.kind !== 'output') return undefined
  const index = page.trace?.children[row.run]?.findIndex((item) => item.name === row.name)
  return index === undefined || index < 0 ? undefined : index
}

/**
 * The Activity page: the list of Tasks and standalone Runs, the open one's timeline beside it,
 * and the selected record's detail docked on the right.
 */
export function Activity({
  page,
  live,
  list,
  windowKey,
  revealRecord,
  connect,
  more,
}: ActivityViewProps) {
  const nav = useActivityNavigation(page)
  const navigate = useNavigate()
  const { search } = useLocation()
  // The loader's own clock sample, so the server and the browser draw ongoing bars alike.
  const [renderedAt] = useState(() => Date.now())
  const loadedAt = page.readAt ?? renderedAt
  const cursor = Table.useRowCursor()
  // The records a close covers: the one the rail shows, and the one a key step is about to
  // show. A step changes the address before its page renders, so Escape in that gap must keep
  // the step's page from reopening the rail. Another selection ends it.
  const [closing, setClosing] = useState<readonly string[] | null>(null)
  if (closing !== null && !(page.selected !== null && closing.includes(page.selected)))
    setClosing(null)
  const showing = page.selected !== null && closing === null
  const stepping = useRef<string | null>(null)
  const unsent = useUnsentComment()
  // A closing rail keeps showing its record while it plays its exit.
  const [shown, setShown] = useState(page)
  if (showing && shown !== page) setShown(page)
  const [lostPosition, setLostPosition] = useState<string>()
  if (more) return <Glimpse page={page} more={more} connect={connect} />

  const detailPage = showing ? page : shown
  const record = shownRecordOf(detailPage.selected, new URLSearchParams(search).get('record'))
  // Every detail panel names its own record, so it replaces the rail's generic header.
  const ownHeader = !detailPage.unavailable && Boolean(detailPage.detail)
  const reviewingAction =
    ownHeader &&
    ((detailPage.detail?.kind === 'invocation' &&
      detailPage.detail.request?.kind === 'approval' &&
      Boolean(detailPage.detail.context)) ||
      (detailPage.detail?.kind === 'run' &&
        detailPage.detail.request?.kind === 'account_connection'))
  // A record inside a Run shows that Run's accepted source beside its own detail.
  const runContext = detailPage.unavailable ? undefined : runContextOf(detailPage.detail)
  const close = () => {
    // With an unsent comment, the rail stays while the review asks whether to discard it.
    if (!unsent)
      setClosing([page.selected, stepping.current].filter((name): name is string => name !== null))
    stepping.current = null
    void navigate(nav.backHref, { preventScrollReset: true })
  }
  const railControls = (
    <span className={panelStyles.railControls}>
      <Dialog.Close ariaLabel="Close details" inline />
    </span>
  )
  const open = page.trace?.root.name ?? null
  const listBlock = (
    <>
      {live && !live.connected && (
        <Typography.BodySmall role="status">
          Activity is unavailable. Reconnecting…
        </Typography.BodySmall>
      )}
      {page.listUnavailable && <Typography.Body>This list is unavailable.</Typography.Body>}
      {page.anchorUnavailable && (
        <Typography.Body>
          This page is unavailable.{' '}
          <Link to={nav.href({ before: null, after: null, anchor: null })}>
            Open the first page.
          </Link>
        </Typography.Body>
      )}
      {list && (
        <Typography.BodySmall aria-live="polite" className={visuallyHidden}>
          {list.count === 1 ? 'The list shows 1 record.' : `The list shows ${list.count} records.`}
        </Typography.BodySmall>
      )}
      {windowKey && lostPosition === windowKey && (
        <Typography.BodySmall role="status">
          Some records are no longer available.
        </Typography.BodySmall>
      )}
      <Table.Container
        readingKey={windowKey}
        readingPaused={live && !live.connected}
        readingFollowStart={live?.followRoot}
        onReadingStartChange={live?.atStart}
        revealRecord={revealRecord}
        onReadingPositionLost={() => setLostPosition(windowKey)}
        onVisibleRecordsChange={live?.visible}
        ariaLabel="Activity records"
        density="compact"
        layout="scroll"
        columns={[{ label: 'Record', width: 'minmax(180px, 1fr)' }]}
      >
        <Table.Head />
        <Table.Body>
          {list?.newer && <LoadRecords control={list.newer} direction="newer" />}
          <DayGroups rows={page.rows} href={(row) => nav.rootHref(row.name)} open={open} />
          {list?.older && <LoadRecords control={list.older} direction="older" />}
          {!page.rows.length && !page.listUnavailable && !page.anchorUnavailable && (
            <Table.Status>
              <EmptyActivity page={page} connect={connect} />
            </Table.Status>
          )}
        </Table.Body>
      </Table.Container>
    </>
  )
  // ↑ and ↓ in the detail step through the open timeline's records, Events included.
  const order = page.trace ? recordOrder(page.trace) : []
  const step = (event: KeyboardEvent<HTMLDivElement>) => {
    if (event.defaultPrevented || event.altKey || event.ctrlKey || event.metaKey || event.shiftKey)
      return
    const by = event.key === 'ArrowDown' ? 1 : event.key === 'ArrowUp' ? -1 : 0
    if (!by || (event.target as Element).closest(ARROW_CONTROLS)) return
    const at = order.indexOf(record ?? '')
    const next = at < 0 ? undefined : order[at + by]
    if (!next) return
    event.preventDefault()
    stepping.current = next
    void navigate(nav.recordHref(next), { preventScrollReset: true })
  }

  return (
    <ActivityConnectedContext value={live?.connected ?? true}>
      {/* ↑ and ↓ move through the list's rows and then the timeline's, as one list. */}
      <div className={styles.root} ref={cursor}>
        {/* With no timeline open, the list stands on its own; with one, it opens from the
            timeline's title, so the timeline keeps the width. */}
        {!page.trace && <div className={styles.rootList}>{listBlock}</div>}
        <div className={styles.timeline}>
          {page.trace ? (
            <ActivityWaterfall
              key={page.trace.root.name}
              trace={page.trace}
              selected={record}
              href={nav.recordHref}
              loadedAt={loadedAt}
              picker={
                <RecentRuns key={open} rows={page.rows}>
                  {listBlock}
                </RecentRuns>
              }
            />
          ) : page.traceUnavailable ? (
            <Typography.Body>This timeline is unavailable.</Typography.Body>
          ) : null}
        </div>
        {/* One rail for every record: another selection replaces its content in place.
          On a wide screen it docks beside the timeline, which shrinks to make room. */}
        <Dialog.Rail docked open={showing} onOpenChange={(next) => !next && close()}>
          <Dialog.Portal>
            <Dialog.Backdrop />
            <Dialog.Popup size="xl" placement="right" fullWidthOnMobile onKeyDown={step}>
              {ownHeader ? (
                <Dialog.Title visuallyHidden>
                  {reviewingAction ? 'Review action' : 'Activity details'}
                </Dialog.Title>
              ) : (
                <Dialog.Header>
                  <Dialog.Title>Activity details</Dialog.Title>
                  <Dialog.Close ariaLabel="Close details" />
                </Dialog.Header>
              )}
              {detailPage.unavailable ? (
                <Typography.Body>This history is unavailable.</Typography.Body>
              ) : runContext ? (
                // Records of one Run share one split, so it keeps its sizes and its source
                // while the selection moves.
                <ExecutionSourceSplit
                  key={runContext.run.name}
                  context={runContext}
                  locatable={detailPage.detail?.kind === 'run'}
                  selectedRow={detailPage.selectedRow}
                  position={runPosition(detailPage)}
                  close={railControls}
                >
                  <ActivityDetail key={record ?? ''} page={detailPage} />
                </ExecutionSourceSplit>
              ) : (
                <PanelClose value={ownHeader ? railControls : null}>
                  <ActivityDetail key={record ?? ''} page={detailPage} />
                </PanelClose>
              )}
            </Dialog.Popup>
          </Dialog.Portal>
        </Dialog.Rail>
      </div>
    </ActivityConnectedContext>
  )
}
