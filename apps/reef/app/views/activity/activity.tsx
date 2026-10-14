import { createContext, memo, useContext, useLayoutEffect, useMemo, useRef, useState } from 'react'
import { Link, useLocation, useNavigate, useNavigation } from 'react-router'
import { CopyMcpEndpoint } from '@/components/copy-mcp-endpoint'
import { EmptyPage } from '@/components/empty-page'
import { Button, Typography } from '@/wax/components'
import { Elapsed } from '@/wax/components/elapsed'
import * as Dialog from '@/wax/components/dialog'
import { Icon, type IconName } from '@/wax/components/icon'
import { Pill } from '@/wax/components/pill'
import * as Table from '@/wax/components/table'
import {
  MAX_EXPANDED_RECORDS,
  type ActivityLoadControl,
  type ActivityPage,
  type ActivityViewProps,
  type ActivityRow,
  type RunState,
} from './types'
import { revealHref, selectionKeys, useActivityNavigation } from './navigation'
import { ActivityDetail } from './detail'
import { ActivityConnectedContext } from './live'
import { PendingActions } from './pending'
import { useUnsentComment } from '../actions/review'
import { ContextImpact } from './context-impact'
import { ACTIVITY_CHILD_WINDOW, activityRowIdentity } from '@/lib/activity-live-types'
import { activityDays, utcDay } from '@/lib/activity-list'
import { visuallyHidden } from '@/wax/components/visually-hidden.css'
import * as styles from './task-activity.css'

export function rowLabel(row: ActivityRow): string {
  switch (row.kind) {
    case 'task':
      return row.intent
    case 'tool':
      return row.intent
    case 'run':
      return row.submission?.intent || row.actor || 'exec'
    case 'invocation':
      return row.callPath || row.operation
    case 'output':
      return row.label
  }
}
const toolIcons = new Map<string, IconName>([
  ['search', 'Search'],
  ['describe', 'BookOpen'],
  ['exec', 'Play'],
  ['save_function', 'Save'],
  ['delete_function', 'Trash'],
])
function rowIcon(row: ActivityRow): { name: IconName; label: string } {
  switch (row.kind) {
    case 'task':
      return { name: 'Flag', label: 'task' }
    case 'tool':
      return { name: toolIcons.get(row.tool) ?? 'CircleQuestionMark', label: row.tool }
    case 'run':
      return { name: 'Play', label: 'exec' }
    case 'invocation':
      return { name: 'Zap', label: 'operation' }
    case 'output':
      return row.eventKind === 'program.logged'
        ? { name: 'Terminal', label: 'log' }
        : { name: 'Radio', label: 'output' }
  }
}
type RowStatus = NonNullable<Table.RowProps['status']>
const runTones = {
  running: 'neutral',
  succeeded: 'success',
  failed: 'error',
  timed_out: 'error',
  canceled: 'neutral',
  interrupted: 'warning',
  unknown: 'warning',
} satisfies Record<RunState, RowStatus['tone']>
function rowStatus(row: ActivityRow): RowStatus {
  switch (row.kind) {
    case 'task':
      return {
        tone:
          row.outcome === 'success' ? 'success' : row.outcome === 'failure' ? 'error' : 'neutral',
        label: row.outcome,
      }
    case 'tool':
      if (row.outcome === 'unknown') return { tone: 'warning', label: 'unknown / incomplete' }
      if (row.acceptedRun) return { tone: 'neutral', label: 'accepted · run unavailable' }
      return {
        tone:
          row.outcome === 'succeeded'
            ? 'success'
            : row.outcome === 'rejected'
              ? 'error'
              : 'neutral',
        label: row.outcome,
      }
    case 'run':
      return {
        tone: runTones[row.state],
        label: row.state.replaceAll('_', ' '),
      }
    case 'invocation':
      if (row.state !== 'ended')
        return {
          tone: row.requiresAction || row.state === 'unknown' ? 'warning' : 'neutral',
          label: row.state,
        }
      // An upstream outcome is a response, not a Coral success judgment.
      return {
        tone:
          row.outcome === 'coral_failure'
            ? 'error'
            : row.outcome === 'indeterminate' || row.outcome === 'unknown'
              ? 'warning'
              : 'neutral',
        label: row.outcome.replaceAll('_', ' '),
      }
    case 'output':
      return {
        tone: 'neutral',
        label: row.eventKind === 'program.logged' ? 'diagnostic' : 'retained',
      }
  }
}
export function duration(nanos: string | number) {
  const ms = Number(nanos) / 1_000_000
  if (ms < 1000) return `${Math.max(0, Math.round(ms))}ms`
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`
  if (ms < 3_600_000) return `${Math.floor(ms / 60_000)}m ${Math.floor(ms / 1000) % 60}s`
  return `${Math.floor(ms / 3_600_000)}h ${Math.floor(ms / 60_000) % 60}m`
}
function rowDuration(row: ActivityRow) {
  if ('durationNanos' in row)
    return (
      <Elapsed
        milliseconds={Number(row.durationNanos) / 1_000_000}
        running={['running', 'pending', 'dispatching'].includes(row.state)}
      >
        {(ms) => duration(ms * 1_000_000)}
      </Elapsed>
    )
  const end = row.kind === 'task' ? row.endedAt : row.kind === 'tool' ? row.completedAt : null
  return end ? duration((Date.parse(end) - Date.parse(row.startedAt)) * 1_000_000) : '—'
}
// Activity reads time in UTC, as its column heading says. An ISO instant splits
// into the UTC time without a time zone lookup.
const utcTime = (instant: string) => instant.slice(11, 19)
const dayParts = new Intl.DateTimeFormat('en-US', {
  timeZone: 'UTC',
  weekday: 'short',
  day: 'numeric',
  month: 'short',
  year: 'numeric',
})
/**
 * Names a UTC calendar day (`YYYY-MM-DD`): "Tue 22 Sep 2026". Never "Today": the
 * reader's today is often another UTC day, and the record alone decides its label.
 */
function dayLabel(day: string) {
  const part = Object.fromEntries(
    dayParts
      .formatToParts(new Date(`${day}T00:00:00.000Z`))
      .map(({ type, value }) => [type, value]),
  )
  return `${part.weekday} ${part.day} ${part.month} ${part.year}`
}
/** The time column shows the UTC time only; the full instant is on hover. */
function StartTime({ startedAt }: { startedAt: string }) {
  return (
    <time dateTime={startedAt} title={`${utcDay(startedAt)} ${utcTime(startedAt)} UTC`}>
      {utcTime(startedAt)}
    </time>
  )
}
function emptyBranchMessage(row: Extract<ActivityRow, { kind: 'task' | 'run' }>) {
  if (row.kind === 'task')
    return row.endedAt ? 'This task made no calls.' : 'This task has not made a call yet.'
  // Reef cannot tell whether a Run in an unknown state has ended.
  if (row.state === 'unknown') return 'This execution has no recorded operations.'
  return row.state === 'running'
    ? 'This execution has not called an operation yet.'
    : 'This execution called no operations.'
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
 * What every record row needs from the page beyond its own record. It changes on
 * navigation, not on scroll or live snapshots, so memoized rows skip those renders.
 */
interface RowScope {
  selected: string | null
  expanded: Set<string>
  /** The expansion bound is reached; collapsed records cannot expand. */
  full: boolean
  /** A selected Run whose detail found a final value. */
  finalValueRun: string | null
  /**
   * The address a row's link shows: this URL with that record selected, or for a
   * glimpse, the record on its own Activity page.
   */
  href: (row: ActivityRow) => string
  /** Child windows report their viewport to a live reader. */
  observed: boolean
  /** A glimpse lists only its roots, so it stays as short as it says. */
  glimpse: boolean
}
/** Navigation that reads the reader's latest position when it runs, not when rows rendered. */
interface RowActions {
  select: (row: ActivityRow) => void
  toggle: (name: string) => void
  viewport: (parent: string, value: Table.WindowViewport) => void
}
const RowActionsContext = createContext<RowActions>({
  select() {},
  toggle() {},
  viewport() {},
})
/** The rail token a navigation's state carries, if any. */
const railToken = (state: unknown) =>
  typeof state === 'object' && state && 'activityRail' in state ? state.activityRail : undefined
/**
 * Tells whether a pending navigation is the rail's own and history has not moved
 * under it, so replacing the address adds no entry and loses none.
 *
 * Each of the rail's navigations carries a fresh token. A token stops matching once
 * its navigation ends, because Back or Forward can later reach the entry it made.
 * Back or Forward reaches a location this view has already shown, so once one is
 * under way, nothing replaces until navigation is idle again. Tokens are unique to
 * this mount, so an entry an earlier rail left in history never matches.
 */
function railTokens() {
  const mount = Math.random().toString(36).slice(2)
  const shown = new Set<string>()
  let count = 0
  let latest: string | undefined
  let moved = false
  return {
    next: () => ({ activityRail: (latest = `${mount}-${++count}`) }),
    current: (token: unknown) => !moved && latest !== undefined && token === latest,
    /** Records the shown location and the pending one, if a navigation is under way. */
    observe: (location: string, pending: string | undefined) => {
      shown.add(location)
      if (pending === undefined) {
        latest = undefined
        moved = false
      } else if (shown.has(pending)) moved = true
    },
  }
}
/** A plain left click; other clicks keep the link's own behaviour, such as a new tab. */
const plainClick = (event: React.MouseEvent) =>
  event.button === 0 && !event.metaKey && !event.ctrlKey && !event.shiftKey && !event.altKey
/** Picks a record from its link on a plain click. */
function pickFromLink(event: React.MouseEvent<HTMLAnchorElement>, pick: () => void) {
  if (!plainClick(event)) return
  event.preventDefault()
  // Safari does not focus a clicked link; focus rests on the picked row everywhere.
  event.currentTarget.focus({ preventScroll: true })
  pick()
}

const RecordRow = memo(function RecordRow({
  row,
  depth,
  branches,
  scope,
}: {
  row: ActivityRow
  depth: number
  /** Only an expanded record gets the branches, so other rows skip branch updates. */
  branches?: ActivityPage['branches']
  scope: RowScope
}) {
  const actions = useContext(RowActionsContext)
  const expandable = !scope.glimpse && (row.kind === 'task' || row.kind === 'run')
  const branch = scope.glimpse ? undefined : branches?.[row.name]
  const expanded = scope.expanded.has(row.name)
  const recordRowProps = {
    recordKey: row.name,
    selected: row.name === scope.selected,
    status: rowStatus(row),
    onClick: (event: React.MouseEvent<HTMLElement>) => {
      if ((event.target as HTMLElement).closest('a,button,input,select')) return
      // Focus rests on the clicked row, as a click on its link leaves it.
      event.currentTarget.querySelector<HTMLElement>('a[href]')?.focus({ preventScroll: true })
      actions.select(row)
    },
  }
  const cells = (
    <>
      <Table.Cell mono>
        <StartTime startedAt={row.startedAt} />
      </Table.Cell>
      <Table.Cell mono={row.kind === 'invocation'}>
        <Table.Record
          depth={depth}
          icon={rowIcon(row)}
          expanded={expanded}
          disabled={!expanded && scope.full}
          onToggle={expandable ? () => actions.toggle(row.name) : undefined}
          marker={row.kind === 'invocation' && row.requiresAction ? 'needs action' : undefined}
          metadata={
            row.kind === 'run' &&
            (row.hasFinalValue || scope.finalValueRun === row.name) && (
              <Pill
                color="graySubtle"
                role="img"
                aria-label="final value"
                title="Open this execution to inspect its final value"
              >
                <Icon name="FileOutput" size="14" />
              </Pill>
            )
          }
        >
          <Link
            to={scope.href(row)}
            preventScrollReset
            onClick={(event) => pickFromLink(event, () => actions.select(row))}
          >
            {rowLabel(row)}
          </Link>
        </Table.Record>
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
  // Only a Task or Run expands; anything else stays a plain row.
  if (!branch || (row.kind !== 'task' && row.kind !== 'run'))
    return <Table.Row {...recordRowProps}>{cells}</Table.Row>
  // The expanded record heads its children and pins while they scroll.
  return (
    <Table.Group label={rowLabel(row)} heading={cells} headingRow={recordRowProps}>
      {branch.unavailable ? (
        <Table.Row>
          <Table.Cell fullWidth>This history is unavailable.</Table.Cell>
        </Table.Row>
      ) : (
        <Table.Window
          label={`${rowLabel(row)} records`}
          total={Math.max(
            Number(branch.total ?? branch.rows.length),
            ...branch.rows.map(
              (child, index) => (child.position ?? (branch.offset ?? 0) + index) + 1,
            ),
          )}
          size={ACTIVITY_CHILD_WINDOW}
          onViewportChange={
            scope.observed ? (value) => actions.viewport(row.name, value) : undefined
          }
          items={branch.rows.map((child, index) => ({
            key: activityRowIdentity(child),
            index: child.position ?? (branch.offset ?? 0) + index,
            content: (
              <RecordRow
                row={child}
                depth={depth + 1}
                branches={branches?.[child.name] ? branches : undefined}
                scope={scope}
              />
            ),
          }))}
        />
      )}
      {!branch.unavailable && !branch.rows.length && !Number(branch.total ?? 0) && (
        <Table.Status scope="group">{emptyBranchMessage(row)}</Table.Status>
      )}
    </Table.Group>
  )
})
/** Every record the table shows: the loaded list and the children of expanded records. */
function shownRecords(page: ActivityPage, roots: ActivityRow[], glimpse: boolean) {
  const shown = new Set<string>()
  const visit = (rows: ActivityRow[]) => {
    for (const row of rows) {
      shown.add(row.name)
      const branch = page.branches[row.name]
      if (!glimpse && branch && (row.kind === 'task' || row.kind === 'run')) visit(branch.rows)
    }
  }
  visit(roots)
  return shown
}
/** This URL with `name` selected, as a link shows it. A click navigates with more state. */
function selectionHref(listPath: string, search: string, name: string) {
  const query = new URLSearchParams(search)
  for (const key of selectionKeys) query.delete(key)
  query.set('record', name)
  return `${listPath}?${query}`
}
export function Activity({
  page,
  live,
  list,
  windowKey,
  revealRecord,
  connect,
  more,
}: ActivityViewProps) {
  const [lostPosition, setLostPosition] = useState<string>()
  const nav = useActivityNavigation(page)
  const navigate = useNavigate()
  const { search, key: locationKey } = useLocation()
  const selected = page.selected ?? null
  const transition = useNavigation()
  // The record a navigation in this list is about to select: null when it clears
  // the selection, undefined when no navigation is under way.
  const pending =
    transition.location?.pathname === page.listPath
      ? new URLSearchParams(transition.location.search).get('record')
      : undefined
  // The rail's own navigation carries a fresh token in its state; Back and Forward do not.
  const pendingToken = railToken(transition.location?.state)
  // The record whose rail is closing. It plays its exit while the address catches
  // up, and a load that lands meanwhile cannot reopen it. A new selection ends it.
  const [closing, setClosing] = useState<string | null>(null)
  if (closing !== null && closing !== selected) setClosing(null)
  // A closing rail keeps showing its record until its exit ends.
  const [shown, setShown] = useState(page)
  if (selected && shown !== page) setShown(page)
  const detailPage = selected ? page : shown
  const unsent = useUnsentComment()
  // The token of the rail's latest pick or close. A pending navigation with this
  // token is the rail's own and has no history entry yet.
  const [tokens] = useState(() => railTokens())
  const close = () => {
    // With an unsent comment, the rail stays while the review asks whether to discard it.
    if (!unsent) setClosing(selected)
    void navigate(nav.backHref, { preventScrollReset: true, state: tokens.next() })
  }
  const listRef = useRef<HTMLDivElement>(null)
  const popupRef = useRef<HTMLDivElement>(null)
  // Another record starts at its top, in the same rail.
  useLayoutEffect(() => {
    if (selected && popupRef.current) popupRef.current.scrollTop = 0
  }, [selected])
  // Rows navigate with the reader's latest position, read when the click happens.
  const latest = useRef({ nav, navigate, live, more, selected, pending, pendingToken, closing })
  useLayoutEffect(() => {
    latest.current = { nav, navigate, live, more, selected, pending, pendingToken, closing }
    tokens.observe(locationKey, transition.state === 'idle' ? undefined : transition.location?.key)
  })
  const [actions] = useState<RowActions>(() => ({
    select: (row) => {
      const current = latest.current
      // A glimpse opens the record on its Activity page, at the top of that page.
      if (current.more) return void current.navigate(revealHref(row))
      // The record the rail shows, or is about to show. Picking it again does nothing.
      const target =
        current.closing !== null
          ? null
          : current.pending !== undefined
            ? current.pending
            : current.selected
      if (row.name === target) return
      if (row.name === current.closing) setClosing(null)
      // Going back to the selected record cancels the rail's own pending navigation,
      // which has no history entry yet, so the address is replaced. Back or Forward
      // already moved through history, so a pick after either is a new entry.
      const replace = row.name === current.selected && tokens.current(current.pendingToken)
      void current.navigate(current.nav.recordHref(row.name), {
        preventScrollReset: true,
        replace,
        state: tokens.next(),
      })
    },
    toggle: (name) =>
      void latest.current.navigate(latest.current.nav.toggleHref(name), {
        preventScrollReset: true,
      }),
    viewport: (parent, value) => latest.current.live?.viewport(parent, value),
  }))
  const expandedKey = page.expanded.join('\n')
  const finalValueRun =
    page.selectedRow?.kind === 'run' && page.selectedRow.hasFinalValue
      ? page.selectedRow.name
      : null
  const observed = live !== undefined
  const glimpse = more !== undefined
  // A glimpse never loads more, and lists only the latest roots a live update merged in.
  const roots = more ? page.rows.slice(0, more.roots) : page.rows
  const loadable = more ? undefined : list
  const scope = useMemo<RowScope>(
    () => ({
      selected: page.selected,
      expanded: new Set(expandedKey ? expandedKey.split('\n') : []),
      full: (expandedKey ? expandedKey.split('\n').length : 0) >= MAX_EXPANDED_RECORDS,
      finalValueRun,
      href: glimpse ? revealHref : (row) => selectionHref(page.listPath, search, row.name),
      observed,
      glimpse,
    }),
    [page.selected, expandedKey, finalValueRun, page.listPath, search, observed, glimpse],
  )
  const rows = (items: ActivityRow[]) =>
    items.map((row) => (
      <RecordRow
        key={row.name}
        row={row}
        depth={0}
        branches={page.branches[row.name] ? page.branches : undefined}
        scope={scope}
      />
    ))
  // Top-level records sit under the UTC day they started; children stay with their parent.
  const tableRows = activityDays(roots).map(({ day, key, rows: dayRows }) => {
    const label = dayLabel(day)
    return (
      <Table.Group
        key={key}
        label={label}
        // The day orients; the records stay the strongest text.
        heading={
          <Table.Cell fullWidth>
            <Typography.BodySmall color="secondary">{label}</Typography.BodySmall>
          </Table.Cell>
        }
      >
        {rows(dayRows)}
      </Table.Group>
    )
  })
  const outside =
    page.selectedRow && !shownRecords(page, roots, glimpse).has(page.selectedRow.name)
      ? page.selectedRow
      : null
  return (
    <RowActionsContext value={actions}>
      <ActivityConnectedContext value={live?.connected ?? true}>
        <div className={styles.root}>
          <div className={styles.toolbar}>
            {live && !live.connected && (
              <Typography.BodySmall role="status">
                Activity is unavailable. Reconnecting…
              </Typography.BodySmall>
            )}
            {page.expanded.length >= MAX_EXPANDED_RECORDS && (
              <Typography.BodySmall>
                Collapse a record before you expand another.
              </Typography.BodySmall>
            )}
          </div>
          <PendingActions page={page} />
          <div className={styles.list} ref={listRef}>
            {!page.agent && (
              <Typography.HeadingSmall as="h2">Your recent activity</Typography.HeadingSmall>
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
            {loadable && (
              <Typography.BodySmall aria-live="polite" className={visuallyHidden}>
                {loadable.count === 1
                  ? 'The list shows 1 record.'
                  : `The list shows ${loadable.count} records.`}
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
              columns={[
                { width: 104, label: 'TIME · UTC' },
                { width: 'minmax(320px, 1fr)', label: 'RECORD' },
                { width: 88, label: 'DUR' },
                { width: 150, label: 'CONTEXT', ariaLabel: 'Context impact', align: 'right' },
              ]}
            >
              <Table.Head />
              <Table.Body>
                {loadable?.newer && <LoadRecords control={loadable.newer} direction="newer" />}
                {tableRows}
                {loadable?.older && <LoadRecords control={loadable.older} direction="older" />}
                {!page.rows.length && !page.listUnavailable && !page.anchorUnavailable && (
                  <Table.Status>
                    <EmptyPage
                      fullSize={false}
                      iconName="Activity"
                      title="No activity yet"
                      description={
                        connect
                          ? 'Connect an MCP client to this workspace. Its calls show here.'
                          : page.agent
                            ? "The agent's calls show here."
                            : 'Your calls show here.'
                      }
                      action={connect && <CopyMcpEndpoint {...connect} />}
                    />
                  </Table.Status>
                )}
                {outside && (
                  <Table.Row recordKey={outside.name} selected status={rowStatus(outside)}>
                    <Table.Cell mono>
                      <StartTime startedAt={outside.startedAt} />
                    </Table.Cell>
                    <Table.Cell mono={outside.kind === 'invocation'}>
                      <Table.Record
                        icon={rowIcon(outside)}
                        metadata={
                          <>
                            <span>Outside this workspace or page:</span>
                            {/* This row sits outside every day group, so it names its own day. */}
                            <span>{dayLabel(utcDay(outside.startedAt))}</span>
                          </>
                        }
                      >
                        <Link
                          to={nav.recordHref(outside.name)}
                          preventScrollReset
                          onClick={(event) => pickFromLink(event, () => actions.select(outside))}
                        >
                          {rowLabel(outside)}
                        </Link>
                      </Table.Record>
                    </Table.Cell>
                    <Table.Cell mono>{rowDuration(outside)}</Table.Cell>
                    <Table.Cell>
                      <ContextImpact
                        bytes={outside.contextImpactBytes}
                        partial={outside.kind === 'task' && outside.contextImpactPartial}
                      />
                    </Table.Cell>
                  </Table.Row>
                )}
              </Table.Body>
            </Table.Container>
            {more && (page.hasAfter || page.rows.length > roots.length) && (
              <div className={styles.filters}>
                <Button.InternalLink to={more.to} variant="bare" size="x-small">
                  {more.label}
                </Button.InternalLink>
              </div>
            )}
          </div>
          {/* One rail for every record: another selection replaces its content in place. */}
          <Dialog.Rail
            open={selected !== null && closing === null}
            onOpenChange={(open) => !open && close()}
          >
            <Dialog.Portal>
              <Dialog.Backdrop />
              <Dialog.Popup
                size="xl"
                placement="right"
                fullWidthOnMobile
                ref={popupRef}
                // Closing returns focus to the table: the record's row, or its first row.
                finalFocus={() =>
                  (detailPage.selected &&
                    listRef.current?.querySelector<HTMLElement>(
                      `[data-record-key="${CSS.escape(detailPage.selected)}"] a[href]`,
                    )) ||
                  listRef.current?.querySelector<HTMLElement>('[data-record-key] a[href]') ||
                  true
                }
              >
                <Dialog.Header>
                  <Dialog.Title>
                    {(detailPage.detail?.kind === 'invocation' &&
                      detailPage.detail.request?.kind === 'approval') ||
                    (detailPage.detail?.kind === 'run' &&
                      detailPage.detail.request?.kind === 'account_connection')
                      ? 'Review action'
                      : 'Activity details'}
                  </Dialog.Title>
                  <Dialog.Close ariaLabel="Close details" />
                </Dialog.Header>
                {detailPage.unavailable ? (
                  <Typography.Body>This history is unavailable.</Typography.Body>
                ) : (
                  <ActivityDetail key={detailPage.selected} page={detailPage} />
                )}
                {outside &&
                  page.detail?.kind !== 'run' &&
                  !(page.detail?.kind === 'invocation' && page.detail.context) && (
                    <Button.InternalLink
                      size="small"
                      variant="bare"
                      to={nav.revealHref(outside)}
                      preventScrollReset
                    >
                      Reveal record
                    </Button.InternalLink>
                  )}
              </Dialog.Popup>
            </Dialog.Portal>
          </Dialog.Rail>
        </div>
      </ActivityConnectedContext>
    </RowActionsContext>
  )
}
