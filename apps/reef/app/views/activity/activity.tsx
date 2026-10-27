import { duration } from './duration'
import {
  createContext,
  memo,
  useContext,
  useEffect,
  useId,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from 'react'
import { Link, useFetcher, useLocation, useNavigate, useNavigation } from 'react-router'
import { CopyMcpEndpoint } from '@/components/copy-mcp-endpoint'
import { EmptyPage } from '@/components/empty-page'
import { Button, Typography } from '@/wax/components'
import { Elapsed } from '@/wax/components/elapsed'
import * as Dialog from '@/wax/components/dialog'
import { Icon, type IconName } from '@/wax/components/icon'
import { Pill } from '@/wax/components/pill'
import { Tooltip } from '@/wax/components/tooltip'
import * as Table from '@/wax/components/table'
import {
  MAX_EXPANDED_RECORDS,
  type ActivityLoadControl,
  type ActivityPage,
  type ActivityViewProps,
  type ActivityRow,
  type LiveScope,
  type RunState,
} from './types'
import {
  activityStops,
  childWindow,
  liveRun,
  liveStep,
  liveTarget,
  MIN_LIVE_RECORD_STAY_MS,
  orderedRecordPosition,
  recordParents,
  resolveStop,
  revealHref,
  selectionKeys,
  shownRecordOf,
  stepStop,
  stopPosition,
  useActivityNavigation,
  windowAt,
  type StopPosition,
} from './navigation'
import { ActivityDetail, runContextOf } from './detail'
import { ExecutionSourceSplit } from './execution-source-split'
import { PanelClose } from './panel'
import { ActivityConnectedContext, restoreFirst, useActivityArchived } from './live'
import { PendingActions } from './pending'
import { useUnsentComment } from '../actions/review'
import type { action as resolve } from '@/routes/activity-resolve'
import { ContextImpact } from './context-impact'
import { ACTIVITY_CHILD_WINDOW, activityRowIdentity } from '@/lib/activity-live-types'
import { activityDays, utcDay } from '@/lib/activity-list'
import { visuallyHidden } from '@/wax/components/visually-hidden.css'
import * as styles from './task-activity.css'
import { invocationOutcomeLabels, resultUnknown } from './labels'
import * as panelStyles from './panel.css'

export function rowLabel(row: ActivityRow): string {
  switch (row.kind) {
    case 'task':
      return row.intent
    case 'tool':
      return row.intent
    case 'run':
      return row.submission?.intent || row.actor || 'exec'
    case 'invocation':
      return row.callPath || row.tool
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
      return {
        name: toolIcons.get(row.tool) ?? 'CircleQuestionMark',
        label: row.tool,
      }
    case 'run':
      return { name: 'Play', label: 'exec' }
    case 'invocation':
      return { name: 'Zap', label: 'tool' }
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
function rowStatus(row: ActivityRow): RowStatus | undefined {
  switch (row.kind) {
    case 'task':
      return {
        tone:
          row.outcome === 'success' ? 'success' : row.outcome === 'failure' ? 'error' : 'neutral',
        label: row.outcome,
      }
    case 'tool':
      if (row.outcome === 'unknown') return { tone: 'warning', label: resultUnknown }
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
      // A complete response stays neutral whether the Program received it or was rejected with it
      // as a Tool Error: the row holds no response to tell, so it says only that one came.
      return {
        tone:
          row.outcome === 'coral_failure'
            ? 'error'
            : row.outcome === 'indeterminate' || row.outcome === 'unknown'
              ? 'warning'
              : 'neutral',
        label: invocationOutcomeLabels[row.outcome],
      }
    case 'output':
      // Its icon and label name it; a state would say nothing more.
      return undefined
  }
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
  if (row.state === 'unknown') return 'This execution has no recorded tools.'
  return row.state === 'running'
    ? 'This execution has not called a tool yet.'
    : 'This execution called no tools.'
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
  /** The record the detail shows, by its exact identity. */
  selected: string | null
  /** The record the keyboard cursor rests on, while focus is in the table. */
  active: string | null
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
  /** `keyboard`: Enter in the table, which keeps focus in the table. */
  select: (row: ActivityRow, keyboard?: boolean) => void
  /** From the keyboard, a collapse that also changes the selection replaces the address. */
  toggle: (name: string, keyboard?: boolean) => void
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
/** The Task and Run that lead to `row` in the table, which live mode keeps expanded. */
const liveChain = (row: ActivityRow | null) =>
  row?.kind === 'task'
    ? [row.name]
    : row?.kind === 'tool'
      ? [row.task]
      : (liveRun(row)?.chain ?? [])
/** The advance a navigation's state says it read for, if any. */
const advanceMark = (state: unknown) =>
  typeof state === 'object' && state && 'activityAdvance' in state
    ? state.activityAdvance
    : undefined
/**
 * Tells whether a pending navigation is the rail's own and history has not moved
 * under it, so replacing the address adds no entry and loses none.
 *
 * Each of the rail's navigations carries a fresh token. A token stops matching once
 * its navigation commits, because Back or Forward can later reach the entry it made.
 * A token whose navigation never commits, such as one cancelled at the discard
 * prompt, is on no entry, so nothing can match it. Back or Forward reaches a
 * location this view has already shown, so once one is under way, nothing replaces
 * until the next location commits. Tokens are unique to this mount, so an entry an
 * earlier rail left in history never matches.
 */
function railTokens() {
  const mount = Math.random().toString(36).slice(2)
  const shown = new Set<string>()
  let committed: string | undefined
  let count = 0
  let latest: string | undefined
  let moved = false
  return {
    next: () => ({ activityRail: (latest = `${mount}-${++count}`) }),
    /** How many navigations the rail has started. */
    issued: () => count,
    current: (token: unknown) => !moved && latest !== undefined && token === latest,
    /**
     * Records the shown location, with its token, and the pending one, if a
     * navigation is under way. Only a commit settles anything: the router can look
     * idle while the rail's navigation waits to start or waits on a blocker.
     */
    observe: (location: string, token: unknown, pending: string | undefined) => {
      if (location !== committed) {
        committed = location
        shown.add(location)
        moved = false
        if (token !== undefined && token === latest) latest = undefined
      }
      if (pending !== undefined && shown.has(pending)) moved = true
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
    active: row.name === scope.active,
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
          // The full table is one Tab stop; its keys reach the records.
          tabbable={scope.glimpse}
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
            // The table's cursor row stands for it. Only a glimpse, which takes no
            // keys, gives each record a Tab stop.
            data-record-link
            tabIndex={scope.glimpse ? undefined : -1}
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
  const children = childWindow(branch)
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
          total={children.total}
          size={ACTIVITY_CHILD_WINDOW}
          onViewportChange={
            scope.observed ? (value) => actions.viewport(row.name, value) : undefined
          }
          items={children.items.map(({ row: child, index }) => ({
            key: activityRowIdentity(child),
            index,
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
/** Data that marks a live toggle, so focus can return to it when the record changes. */
const liveToggleAttribute = 'data-activity-live'
/** Each live scope's toggle: its name, and its tooltip while off, on, or blocked. */
const liveToggles: Record<LiveScope, { label: string; off: string; on: string; blocked: string }> =
  {
    run: {
      label: 'Follow latest record',
      off: 'Follow the latest record of this execution.',
      on: 'Stop following the latest record.',
      blocked: 'Collapse a record before you follow this execution.',
    },
    task: {
      label: 'Follow this task',
      off: 'Follow the runs of this task in order.',
      on: 'Stop following this task.',
      blocked: 'Collapse a record before you follow this task.',
    },
    latest: {
      label: 'Follow latest activity',
      off: 'Follow the newest task or run.',
      on: 'Stop following the latest activity.',
      blocked: 'Collapse a record before you follow the latest activity.',
    },
  }
/**
 * A live mode toggle: beside the drawer's close control for the shown Run or Task,
 * and above the list for the newest activity. Pressed, live mode follows its `scope`.
 * The icon shape shows the state too. Above the list it also reads "Live" in both
 * states, the pressed style showing on, and its tooltip also describes it.
 */
function LiveToggle({
  scope,
  on,
  blocked,
  onToggle,
  text = false,
}: {
  scope: LiveScope
  on: boolean
  /** Following needs more expanded records than the table allows, or did while on. */
  blocked: boolean
  onToggle: () => void
  /** Labels the toggle "Live" rather than with an icon alone. */
  text?: boolean
}) {
  const copy = liveToggles[scope]
  const tooltip = blocked ? copy.blocked : on ? copy.on : copy.off
  const icon = on ? 'LocateFixed' : 'Locate'
  const descriptionId = useId()
  const toggle = {
    size: '22',
    'aria-pressed': on,
    // Pressed, it still turns live mode off.
    disabled: blocked && !on,
    focusableWhenDisabled: true,
    onClick: blocked && !on ? undefined : onToggle,
    [liveToggleAttribute]: '',
  } as const
  if (!text)
    return (
      <Button.IconButton
        {...toggle}
        name={icon}
        variant="bare"
        ariaLabel={copy.label}
        tooltipText={tooltip}
        tooltipSide="bottom"
      />
    )
  return (
    <>
      <Tooltip content={tooltip} side="bottom">
        <Button.TextButton
          {...toggle}
          variant="secondary"
          icon={icon}
          aria-describedby={descriptionId}
        >
          Live
        </Button.TextButton>
      </Tooltip>
      {/* The name is the visible label; the tooltip's text describes the toggle. */}
      <span id={descriptionId} className={visuallyHidden}>
        {tooltip}
      </span>
    </>
  )
}

/**
 * A step live mode takes: `key` names it, so it never repeats. `last` ends an advance.
 * `record` is the record it selects.
 */
type FollowStep = { key: string; href: string; last?: boolean; record?: string }
/**
 * The step live mode, or an advance with live mode off, takes next in `scope`: the
 * followed Run's oldest action, else its newest record. 'done' rests on the record
 * shown; null waits.
 */
function liveFollow(
  page: ActivityPage,
  nav: ReturnType<typeof useActivityNavigation>,
  {
    scope,
    actionable,
    shownRecord,
    reviewed,
  }: {
    scope: LiveScope | null
    actionable: boolean
    shownRecord: string | null
    /** The request the rail reviews. */
    reviewed: string | undefined
  },
): FollowStep | 'done' | 'blocked' | null {
  // The records that lead to the selected one stay open until the selection moves.
  const selectedChain = liveChain(page.selectedRow)
  // An advance follows the reviewed Run with live mode off.
  const target = liveTarget(page, scope ?? 'run', Date.now(), actionable)
  if (target.kind === 'wait') return null
  if (target.kind === 'newest')
    return {
      key: 'newest',
      href: nav.href({ anchor: null, before: null, after: null }),
    }
  if (target.kind === 'load')
    return {
      key: `load ${target.parent} ${target.offset}`,
      href: nav.followWindowHref(target.parent, target.offset, scope),
    }
  if (target.kind === 'select')
    return target.record === shownRecord
      ? 'done'
      : {
          key: `select ${target.record}`,
          href: nav.followHref(target.record, undefined, scope, target.chain),
          last: true,
          record: target.record,
        }
  if (target.kind === 'run') {
    const step = liveStep(page, target.run, actionable, target.action)
    if (step.kind === 'wait') return null
    if (step.kind === 'select') {
      if (step.record === shownRecord && (!step.request || step.request === reviewed)) return 'done'
      const action = step.request
        ? { run: target.run, record: step.record, request: step.request }
        : undefined
      return {
        key: `select ${step.record} ${step.request ?? ''}`,
        // An advance keeps live mode as it was, and ends here.
        href: nav.followHref(step.record, action, scope, target.chain),
        last: true,
        record: step.record,
      }
    }
    if (step.kind === 'load')
      return {
        key: `load ${target.run} ${step.total}`,
        href: nav.followWindowHref(target.run, step.total, scope),
      }
  }
  // Expand the target's records, while the selected record's stay open.
  return nav.liveRoom(target.chain, selectedChain)
    ? {
        key: `expand ${target.chain.join(' ')}`,
        href: nav.liveHref(scope, target.chain, selectedChain),
      }
    : 'blocked'
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
  const archived = useActivityArchived()
  const [lostPosition, setLostPosition] = useState<string>()
  const nav = useActivityNavigation(page)
  const navigate = useNavigate()
  const { search, key: locationKey, state: locationState } = useLocation()
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
  // The record the rail shows, exactly: a Run's final value has its own name,
  // although its Run's detail shows it.
  const shownRecord = shownRecordOf(selected, new URLSearchParams(search).get('record'))
  // A closing rail keeps showing its record until its exit ends.
  const [shown, setShown] = useState({ page, record: shownRecord })
  if (selected && (shown.page !== page || shown.record !== shownRecord))
    setShown({ page, record: shownRecord })
  const detail = selected ? { page, record: shownRecord } : shown
  const detailPage = detail.page
  // Every detail panel names its own record, so it replaces the rail's generic header.
  const ownHeader = !detailPage.unavailable && Boolean(detailPage.detail)
  // A request review narrows the rail to one decision; name it apart from a record glimpse.
  const reviewingAction =
    ownHeader &&
    ((detailPage.detail?.kind === 'invocation' &&
      detailPage.detail.request?.kind === 'approval' &&
      Boolean(detailPage.detail.context)) ||
      (detailPage.detail?.kind === 'run' &&
        detailPage.detail.request?.kind === 'account_connection'))
  // A record inside a Run shows that Run's accepted source beside its own detail.
  const runContext = detailPage.unavailable ? undefined : runContextOf(detailPage.detail)
  const unsent = useUnsentComment()
  // The token of the rail's latest pick or close. A pending navigation with this
  // token is the rail's own and has no history entry yet.
  const [tokens] = useState(() => railTokens())
  const close = () => {
    // With an unsent comment, the rail stays while the review asks whether to discard it.
    if (!unsent) setClosing(selected)
    void navigate(nav.backHref, {
      preventScrollReset: true,
      state: tokens.next(),
    })
  }
  const listRef = useRef<HTMLDivElement>(null)
  const tableRef = useRef<HTMLDivElement>(null)
  const popupRef = useRef<HTMLDivElement>(null)
  // Another record starts at its top, in the same rail.
  useLayoutEffect(() => {
    if (shownRecord && popupRef.current) popupRef.current.scrollTop = 0
  }, [shownRecord])
  const observed = live !== undefined
  const glimpse = more !== undefined
  // A glimpse never loads more, and lists only the latest roots a live update merged in.
  const roots = useMemo(
    () => (more ? page.rows.slice(0, more.roots) : page.rows),
    [more, page.rows],
  )
  // Where ↑ and ↓ go. A glimpse takes no keys.
  const stops = useMemo(() => (glimpse ? [] : activityStops(page, roots)), [glimpse, page, roots])
  const open = selected !== null && closing === null
  // Live mode follows the shown record's Run, the newest activity, or one Task's Runs.
  // Any other selection, and closing the rail, turns it off. A glimpse never follows.
  const liveScope = glimpse ? null : nav.liveScope
  const liveOn = liveScope !== null
  // The records that lead to the selected one stay expanded while live mode moves on.
  const selectedChain = liveChain(page.selectedRow)
  // With the rail open, the keys start from the record it shows, or is about to.
  const shownPosition: StopPosition | null = shownRecord
    ? {
        name: shownRecord,
        parents: page.selectedRow ? recordParents(page.selectedRow, shownRecord) : [],
      }
    : null
  const pendingStop =
    open && pending
      ? stops.find((stop) => stop.kind === 'record' && stop.row.name === pending)
      : undefined
  // With the rail closed, the keyboard cursor: the last record the rail showed, or
  // where ↑ and ↓ left it. A collapse that hides it moves it to that record; a
  // record that leaves the table takes it along.
  const [cursor, setCursor] = useState<StopPosition | null>(null)
  if (open) {
    if (shownPosition && cursor?.name !== shownPosition.name) setCursor(shownPosition)
  } else if (cursor) {
    const stop = stops[resolveStop(stops, cursor)]
    if (stop?.kind !== 'record') setCursor(null)
    else if (stop.row.name !== cursor.name) setCursor(stopPosition(stop))
  }
  // A key step, from the address the rail shows to where it is going. The router
  // reports it pending only later, so the next key starts from here meanwhile.
  // It holds only while that address is current: any navigation that commits,
  // the step's own included, ends it, and so does any other navigation the rail
  // starts, or a close.
  const [keyStep, setKeyStep] = useState<{
    at: string
    to: StopPosition
  } | null>(null)
  if (keyStep && (!open || keyStep.at !== locationKey)) setKeyStep(null)
  const stepping = open && keyStep?.at === locationKey ? keyStep.to : null
  const position: StopPosition | null = !open
    ? cursor
    : (stepping ?? (pendingStop?.kind === 'record' ? stopPosition(pendingStop) : shownPosition))
  const positionStop = position ? stops[resolveStop(stops, position)] : undefined
  const activeStop = positionStop?.kind === 'record' ? positionStop : null
  const [tableFocused, setTableFocused] = useState(false)
  // The table reveals the record a key moved to, until its route names another.
  const [reveal, setReveal] = useState({
    route: revealRecord,
    record: revealRecord,
  })
  if (reveal.route !== revealRecord) setReveal({ route: revealRecord, record: revealRecord })
  const full = page.expanded.length >= MAX_EXPANDED_RECORDS
  // Rows and keys navigate with the reader's latest position, read when they act.
  const now = {
    nav,
    navigate,
    live,
    more,
    selected,
    shownRecord,
    shownPosition,
    pending,
    pendingToken,
    closing,
    open,
    stops,
    cursor,
    position,
    activeStop,
    full,
    unsent,
    locationKey,
    expanded: page.expanded,
    selectedRow: page.selectedRow,
    liveOn,
    close,
  }
  const latest = useRef(now)
  useLayoutEffect(() => {
    latest.current = now
    tokens.observe(
      locationKey,
      railToken(locationState),
      transition.state === 'idle' ? undefined : transition.location?.key,
    )
  })
  // Whether Enter in the table, or live mode, opened the rail, which then leaves focus
  // where it was.
  const openedByKey = useRef(false)
  const [actions] = useState<RowActions>(() => ({
    select: (row, keyboard = false) => {
      const current = latest.current
      // A glimpse opens the record on its Activity page, at the top of that page.
      if (current.more) return void current.navigate(revealHref(row))
      // The record the rail shows, or is about to show. Picking it again does nothing.
      const target =
        current.closing !== null
          ? null
          : current.pending !== undefined
            ? current.pending
            : current.shownRecord
      if (row.name === target) return
      if (row.name === current.closing) setClosing(null)
      setKeyStep(null)
      openedByKey.current = keyboard
      // Going back to the shown record cancels the rail's own pending navigation,
      // which has no history entry yet, so the address is replaced. Back or Forward
      // already moved through history, so a pick after either is a new entry.
      const replace = row.name === current.shownRecord && tokens.current(current.pendingToken)
      void current.navigate(current.nav.recordHref(row.name), {
        preventScrollReset: true,
        replace,
        state: tokens.next(),
      })
    },
    toggle: (name, keyboard = false) => {
      const current = latest.current
      // A collapse that hides the record the rail shows opens that record instead.
      // From the keyboard it replaces the address, as a step does.
      const hides =
        current.open &&
        current.expanded.includes(name) &&
        Boolean(current.shownPosition?.parents.includes(name))
      // Collapsing the Run live mode follows, or its Task, stops following it.
      const stopLive =
        current.liveOn &&
        current.expanded.includes(name) &&
        liveChain(current.selectedRow).includes(name)
      setKeyStep(null)
      void current.navigate(
        hides ? current.nav.collapseHref(name) : current.nav.toggleHref(name, stopLive),
        {
          preventScrollReset: true,
          replace: keyboard && hides,
          // A switch to the collapsed record is the rail's own navigation.
          state: hides ? tokens.next() : undefined,
        },
      )
    },
    viewport: (parent, value) => latest.current.live?.viewport(parent, value),
  }))
  useEffect(() => {
    if (glimpse) return
    /** Moves the rail's record, or the cursor, one record up or down. */
    const move = (step: 1 | -1) => {
      const current = latest.current
      const next = stepStop(current.stops, current.open ? current.position : current.cursor, step)
      if (!next) return
      const target = stopPosition(next)
      // With an unsent comment the review asks first; the table and rail stay put.
      if (!current.unsent) setReveal((value) => ({ ...value, record: next.row.name }))
      if (!current.open) return setCursor(target)
      if (!current.unsent) setKeyStep({ at: current.locationKey, to: target })
      void current.navigate(current.nav.recordHref(next.row.name), {
        preventScrollReset: true,
        replace: true,
        state: tokens.next(),
      })
    }
    /** Space: whether it toggled a Task or Run. */
    const toggle = (owner: 'table' | 'rail') => {
      const current = latest.current
      const row = owner === 'rail' ? current.selectedRow : current.activeStop?.row
      // The rail toggles only a Task or Run it shows itself, never a final value.
      if (!row || (owner === 'rail' && (!current.open || row.name !== current.shownRecord)))
        return false
      if (row.kind === 'task' || row.kind === 'run') {
        if (!current.expanded.includes(row.name) && current.full) return false
        actions.toggle(row.name, true)
        return true
      }
      // In the table, any other record collapses the closest Task or Run around it.
      const parent = owner === 'table' && current.activeStop?.parents[0]
      if (!parent) return false
      actions.toggle(parent, true)
      return true
    }
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.defaultPrevented || event.isComposing) return
      if (event.altKey || event.ctrlKey || event.metaKey || event.shiftKey) return
      if (!['ArrowUp', 'ArrowDown', ' ', 'Enter', 'Escape'].includes(event.key)) return
      const table = tableRef.current
      const rail = popupRef.current
      const target = event.target
      if (!table || !(target instanceof Element)) return
      // Another dialog, such as the comment confirmation, keeps every key.
      const dialog = document.querySelector(
        '[role="dialog"][aria-modal="true"], [role="alertdialog"]',
      )
      if (dialog && dialog !== rail) return
      // Activity acts only on the table itself, the rail itself, and the page
      // background. Every other control keeps its keys.
      const owner =
        rail && target === rail
          ? 'rail'
          : target === table
            ? 'table'
            : target === document.body || target === document.documentElement
              ? 'page'
              : null
      if (!owner) return
      const current = latest.current
      const step = event.key === 'ArrowDown' ? 1 : event.key === 'ArrowUp' ? -1 : 0
      if (step) {
        event.preventDefault()
        // A closing rail takes no more steps.
        if (owner === 'rail' && !current.open) return
        // From the page, the open rail takes focus, or else the table.
        if (owner === 'page') (current.open ? rail : table)?.focus({ preventScroll: true })
        return move(step)
      }
      if (owner === 'page' || event.repeat) return
      if (event.key === ' ') {
        if (toggle(owner)) event.preventDefault()
      } else if (event.key === 'Enter') {
        if (owner !== 'table' || current.open || !current.activeStop) return
        event.preventDefault()
        actions.select(current.activeStop.row, true)
      } else if (event.key === 'Escape' && owner === 'table' && current.open) {
        event.preventDefault()
        current.close()
      }
    }
    // The rail's popup stops arrows from bubbling out of it, so listen before it does.
    // Only elements without keys of their own own these keys, so none is overtaken.
    window.addEventListener('keydown', onKeyDown, true)
    return () => window.removeEventListener('keydown', onKeyDown, true)
  }, [glimpse, actions, tokens])
  // Where focus was when live mode last changed the record. The new record's panel
  // replaces the old one, so focus that was in it returns to the same place.
  const refocus = useRef<{
    record: string | null
    on: 'toggle' | 'rail'
  } | null>(null)
  useLayoutEffect(() => {
    const noted = refocus.current
    const rail = popupRef.current
    if (!noted || noted.record === shownRecord) return
    refocus.current = null
    const active = document.activeElement
    if (!rail || (active && active !== document.body && document.contains(active))) return
    const target =
      noted.on === 'toggle' ? rail.querySelector<HTMLElement>(`[${liveToggleAttribute}]`) : null
    ;(target ?? rail).focus({ preventScroll: true })
  }, [shownRecord])
  // The step live mode last took, and the location it left. It takes each step
  // once, so a step that cannot land never repeats. Back, Forward or any other
  // navigation starts afresh.
  const followStep = useRef<{
    step: string
    token: unknown
    from: string
  } | null>(null)
  // When the rail started to show its record. Page live mode shows each record for
  // MIN_LIVE_RECORD_STAY_MS before it selects another. Turning it on asks for the newest
  // record now, so the record you had open does not wait.
  const presented = useRef<{ record: string | null; at: number } | null>(null)
  const presentedScope = useRef(liveScope)
  // The step whose wait ended, and when the record it replaces started to show.
  const [rested, setRested] = useState<{ step: string; since: number } | null>(null)
  // How many navigations the rail had started when a step began to wait for the record
  // shown since `since`, apart from live mode's own. A snapshot that makes it wait again
  // keeps it, so a navigation you started meanwhile still counts. `held` is the location
  // a step gave way at, for a navigation of yours that has not landed.
  const armed = useRef<{ since: number; issued: number; held?: string } | null>(null)
  useLayoutEffect(() => {
    if (presented.current?.record !== shownRecord)
      presented.current = { record: shownRecord, at: Date.now() }
  }, [shownRecord])
  useLayoutEffect(() => {
    if (presentedScope.current !== liveScope && liveScope === 'latest')
      presented.current = { record: shownRecord, at: -Infinity }
    presentedScope.current = liveScope
  }, [liveScope, shownRecord])
  const connected = live?.connected ?? true
  // The review guards a comment until its blocker updates, after the commit that
  // cleared it. Until the next frame, a step would prompt you to discard it.
  const [guarded, setGuarded] = useState(unsent)
  if (unsent && !guarded) setGuarded(true)
  useEffect(() => {
    if (unsent || !guarded) return
    const frame = requestAnimationFrame(() => setGuarded(false))
    return () => cancelAnimationFrame(frame)
  }, [unsent, guarded])
  // Whether the review guarded an unsent comment when live mode last looked.
  const wasGuarded = useRef(guarded)
  // The request the rail reviews, and the decision its approval review submits.
  const reviewed =
    page.detail?.kind === 'invocation' || page.detail?.kind === 'run' ? page.detail.request : null
  const reviewedApproval = reviewed?.kind === 'approval' ? reviewed.name : undefined
  const decision = useFetcher<typeof resolve>({
    key: reviewedApproval ?? 'activity:no-review',
  })
  // A decision you make in the review advances it once, when your decision wins and the
  // read after it lands. A decision already shown, another review's, or one that another
  // reviewer's opposite decision beat, never does.
  const [seen, setSeen] = useState<{
    key?: string
    data: unknown
    intent?: string
  }>({
    key: reviewedApproval,
    data: decision.data,
  })
  // The location where the advance started, which also marks the reads it takes first.
  const [advance, setAdvance] = useState<string | null>(null)
  const submitted = decision.formData?.get('intent')
  if (seen.key !== reviewedApproval) setSeen({ key: reviewedApproval, data: decision.data })
  else if (typeof submitted === 'string' && submitted !== seen.intent)
    setSeen({ ...seen, intent: submitted })
  else if (decision.state === 'idle' && seen.data !== decision.data) {
    setSeen({ key: reviewedApproval, data: decision.data })
    const state = decision.data && 'state' in decision.data ? decision.data.state : undefined
    const won = { approve: 'approved', decline: 'declined' }[seen.intent ?? '']
    if (state !== undefined && state === won) setAdvance(locationKey)
  }
  // It holds through the reads it takes first, and ends with its selection. Any other
  // navigation ends it, and Back or Forward to one of its reads never resumes it.
  const advancing =
    advance !== null && (advance === locationKey || advanceMark(locationState) === advance)
  if (advance !== null && !advancing) setAdvance(null)
  const following = liveOn || advancing
  // The step live mode, or an advance, takes next: the Run's oldest action, else its
  // newest record, in the Run its scope names. Null holds the record: an unsent
  // comment, a pending navigation or decision, or a lost connection. 'done' rests on
  // the record shown. Only the newest activity follows while the rail is closed.
  const follow = ((): FollowStep | 'done' | 'blocked' | null => {
    if (!following || glimpse || closing !== null || guarded || !connected) return null
    if (page.unavailable || (!open && liveScope !== 'latest')) return null
    if (transition.state !== 'idle' || decision.state !== 'idle') return null
    // In an archived workspace you cannot act, so only the newest record counts.
    return liveFollow(page, nav, {
      scope: liveScope,
      actionable: !archived,
      shownRecord,
      reviewed: reviewed?.name,
    })
  })()
  const step = typeof follow === 'object' ? follow : null
  if (advancing && (follow === 'done' || follow === 'blocked')) setAdvance(null)
  // Live mode cannot expand what it follows: its toggle says why.
  const stalled = follow === 'blocked'
  const followKey = step?.key
  const followTo = step?.href
  // Another record replaces the one the rail shows. An advance after your decision does not wait.
  const followReplaces =
    liveScope === 'latest' &&
    !advancing &&
    open &&
    step?.record !== undefined &&
    step.record !== shownRecord
  // A read an advance takes first carries its mark, so the advance holds through it.
  const followMark = advancing && step && !step.last ? advance : undefined
  // A selection that opens the rail leaves focus where it is.
  const followOpens = Boolean(step?.last) && !open
  // In the commit itself, so a pick or close that starts after this render always
  // starts after the step and wins.
  useLayoutEffect(() => {
    const last = followStep.current
    if (
      !following ||
      (last && last.from !== locationKey && railToken(locationState) !== last.token)
    )
      followStep.current = null
    // Only page live mode waits, so another mode, or an advance, keeps no count of yours.
    if (!following || liveScope !== 'latest' || advancing) armed.current = null
    // Your navigation landed, or the review stopped guarding a comment.
    const held = armed.current?.held
    if (
      armed.current &&
      ((held !== undefined && held !== locationKey) || (wasGuarded.current && !guarded))
    )
      armed.current = { since: armed.current.since, issued: tokens.issued() }
    wasGuarded.current = guarded
    if (!followKey || !followTo || followStep.current?.step === followKey) return
    // A step that replaces the record waits until the record showed long enough. A newer
    // step replaces the one that waits, and waits only for what is left. The timer only
    // renders again, so the step still starts in a commit.
    const since = presented.current?.at ?? -Infinity
    const waited = followReplaces && rested?.step === followKey && rested.since === since
    const wait = followReplaces && !waited ? since + MIN_LIVE_RECORD_STAY_MS - Date.now() : 0
    if (wait > 0) {
      if (armed.current?.since !== since) armed.current = { since, issued: tokens.issued() }
      const timer = setTimeout(() => setRested({ step: followKey, since }), wait)
      return () => clearTimeout(timer)
    }
    // A pick, a close or the live mode toggle you started while a step waited goes first,
    // though the router has not applied it yet: any step, even one that only expands,
    // would cancel it. Live mode holds until the location changes, which that navigation
    // does when it lands, or until the review stops guarding a comment.
    if (
      armed.current?.since === since &&
      (armed.current.held !== undefined || tokens.issued() !== armed.current.issued)
    ) {
      armed.current = { since, issued: tokens.issued(), held: locationKey }
      return
    }
    const state = { ...tokens.next(), ...(followMark && { activityAdvance: followMark }) }
    // Its own step is no navigation of yours.
    if (armed.current) armed.current = { ...armed.current, issued: armed.current.issued + 1 }
    followStep.current = { step: followKey, token: state.activityRail, from: locationKey }
    const rail = popupRef.current
    const active = document.activeElement
    refocus.current =
      rail && active && active !== rail && rail.contains(active)
        ? {
            record: shownRecord,
            on: active.closest(`[${liveToggleAttribute}]`) ? 'toggle' : 'rail',
          }
        : null
    if (followOpens) openedByKey.current = true
    // Following adds no history entry.
    void navigate(followTo, { replace: true, preventScrollReset: true, state })
  }, [
    following,
    liveScope,
    advancing,
    guarded,
    locationKey,
    locationState,
    followKey,
    followTo,
    followMark,
    followOpens,
    followReplaces,
    rested,
    shownRecord,
    navigate,
    tokens,
  ])
  /** Turns live mode on in `scope` from the shown record, or off when it already follows it. */
  const toggleLive = (scope: LiveScope, chain: string[], changes?: Record<string, string | null>) =>
    void navigate(
      liveScope === scope ? nav.liveHref(null) : nav.liveHref(scope, chain, selectedChain, changes),
      { preventScrollReset: true, state: tokens.next() },
    )
  const shownRow = glimpse ? null : detailPage.selectedRow
  // The rail offers the scope live mode follows, else the one its record can follow.
  const railScope: LiveScope | null =
    liveScope === 'latest' || liveScope === 'task'
      ? liveScope
      : shownRow?.kind === 'task'
        ? 'task'
        : liveRun(shownRow)
          ? 'run'
          : null
  const railChain = railScope === 'task' || railScope === 'run' ? liveChain(shownRow) : []
  const railToggle = railScope && (
    <LiveToggle
      scope={railScope}
      on={liveScope === railScope}
      blocked={
        liveScope === railScope
          ? stalled
          : // Following a Task also expands one of its Runs.
            !nav.liveRoom(
              railScope === 'task' ? [...railChain, `${railChain[0]}/run`] : railChain,
              selectedChain,
            )
      }
      onToggle={() =>
        toggleLive(
          railScope,
          railChain,
          // Following a Task starts at its first child.
          railScope === 'task' && railChain[0] ? windowAt(railChain[0], 0) : undefined,
        )
      }
    />
  )
  const railControls = (
    <span className={panelStyles.railControls}>
      {railToggle}
      <Dialog.Close ariaLabel="Close details" inline />
    </span>
  )
  // The cursor row shows while focus is in the table.
  const activeRecord = tableFocused ? (activeStop?.row.name ?? null) : null
  const expandedKey = page.expanded.join('\n')
  const finalValueRun =
    page.selectedRow?.kind === 'run' && page.selectedRow.hasFinalValue
      ? page.selectedRow.name
      : null
  const loadable = more ? undefined : list
  const scope = useMemo<RowScope>(
    () => ({
      selected: shownRecord,
      active: activeRecord,
      expanded: new Set(expandedKey ? expandedKey.split('\n') : []),
      full: (expandedKey ? expandedKey.split('\n').length : 0) >= MAX_EXPANDED_RECORDS,
      finalValueRun,
      href: glimpse ? revealHref : (row) => selectionHref(page.listPath, search, row.name),
      observed,
      glimpse,
    }),
    [
      shownRecord,
      activeRecord,
      expandedKey,
      finalValueRun,
      page.listPath,
      search,
      observed,
      glimpse,
    ],
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
          <div className={styles.main}>
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
            <div
              className={styles.list}
              ref={listRef}
              onFocus={(event) => {
                const table = tableRef.current
                // A record's link is not a Tab stop of its own. Focus that lands on one,
                // as a click on it leaves it, rests on the table, whose cursor row shows it.
                if (
                  !glimpse &&
                  table?.contains(event.target) &&
                  event.target.matches('[data-record-link]')
                )
                  return table.focus({ preventScroll: true })
                setTableFocused(Boolean(table?.contains(event.target)))
              }}
              onBlur={(event) =>
                setTableFocused(Boolean(tableRef.current?.contains(event.relatedTarget)))
              }
            >
              <div className={styles.listHeading}>
                {!page.agent && (
                  <Typography.HeadingSmall as="h2">Your recent activity</Typography.HeadingSmall>
                )}
                {!glimpse && (
                  <LiveToggle
                    text
                    scope="latest"
                    on={liveScope === 'latest'}
                    blocked={liveScope === 'latest' && stalled}
                    onToggle={() => toggleLive('latest', [])}
                  />
                )}
              </div>
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
                revealRecord={reveal.record}
                onReadingPositionLost={() => setLostPosition(windowKey)}
                onVisibleRecordsChange={live?.visible}
                ariaLabel="Activity records"
                ref={tableRef}
                focusable={!glimpse}
                density="compact"
                layout="scroll"
                columns={[
                  { width: 104, label: 'TIME · UTC' },
                  { width: 'minmax(320px, 1fr)', label: 'RECORD' },
                  { width: 88, label: 'DUR' },
                  {
                    width: 150,
                    label: 'CONTEXT',
                    ariaLabel: 'Context impact',
                    align: 'right',
                  },
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
                            // The rail shows this record already. Like every record link, it
                            // is no Tab stop, and focus on it rests on the table.
                            data-record-link
                            tabIndex={-1}
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
          </div>
          {/* One rail for every record: another selection replaces its content in place.
              On a wide screen it docks beside the list, which shrinks to make room. */}
          <Dialog.Rail
            docked
            open={selected !== null && closing === null}
            onOpenChange={(next) => !next && close()}
          >
            <Dialog.Portal>
              <Dialog.Backdrop />
              <Dialog.Popup
                size="xl"
                placement="right"
                fullWidthOnMobile
                ref={popupRef}
                // Enter in the table, or live mode, opens the rail and leaves focus where it
                // was. Otherwise focus goes to the close control, as before the live toggle
                // came first, so no toggle's tooltip opens and Escape closes the rail.
                initialFocus={() => {
                  const keep = openedByKey.current
                  openedByKey.current = false
                  if (keep) return false
                  return (
                    popupRef.current?.querySelector<HTMLElement>(
                      'button[aria-label="Close details"]',
                    ) ?? true
                  )
                }}
                // Closing returns focus to the table, where the cursor shows the record.
                finalFocus={() => tableRef.current ?? true}
              >
                {/* Each detail panel heads the rail with the record it names. */}
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
                  // Records of one Run share one split, so it keeps its sizes and its
                  // source while the selection moves.
                  <ExecutionSourceSplit
                    key={runContext.run.name}
                    context={runContext}
                    locatable={detailPage.detail?.kind === 'run'}
                    selectedRow={detailPage.selectedRow}
                    position={orderedRecordPosition(detailPage, detailPage.selectedRow)}
                    close={railControls}
                  >
                    <ActivityDetail key={detail.record} page={detailPage} sourceShown />
                  </ExecutionSourceSplit>
                ) : (
                  <PanelClose value={ownHeader ? railControls : null}>
                    <ActivityDetail key={detail.record} page={detailPage} />
                  </PanelClose>
                )}
                {/* A record inside the split leads back to history from its own detail. */}
                {outside && page.detail?.kind !== 'run' && !runContext && (
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

export { duration } from './duration'
