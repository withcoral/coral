import {
  memo,
  useCallback,
  useContext,
  useEffect,
  useId,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from 'react'
import { Link, useLocation, useNavigate, useNavigation } from 'react-router'
import { CopyMcpEndpoint } from '@/components/copy-mcp-endpoint'
import { EmptyPage } from '@/components/empty-page'
import { Button, Typography } from '@/wax/components'
import * as Dialog from '@/wax/components/dialog'
import { Tooltip } from '@/wax/components/tooltip'
import * as Table from '@/wax/components/table'
import {
  type ActivityLoadControl,
  type ActivityViewProps,
  type ActivityRow,
  type LiveScope,
  type ActivityPage,
} from './types'
import {
  liveRun,
  liveStep,
  liveTarget,
  MIN_LIVE_RECORD_STAY_MS,
  orderedRecordPosition,
  resolveStop,
  revealHref,
  selectionKeys,
  stepStop,
  stopPosition,
  useActivityNavigation,
  windowAt,
  type ActivityStop,
  type StopPosition,
} from './navigation'
import { ActivityDetail, runContextOf } from './detail'
import { TaskHeader } from './task-header'
import { ExecutionSourceSplit } from './execution-source-split'
import { PanelClose } from './panel'
import {
  ActivityConnectedContext,
  ActivityInspectContext,
  restoreFirst,
  useActivityArchived,
} from './live'
import { useCommentGuard, useDecisionOutcome, useUnsentComment } from '../actions/review'
import { ContextImpact } from './context-impact'
import { absorbPage, loneTrace, openTrace } from '@/lib/activity-trace'
import { activityDays, utcDay } from '@/lib/activity-list'
import { dayLabel, UtcTime } from './instant'
import { ProviderMarksContext } from './call-tool'
import { visuallyHidden } from '@/wax/components/visually-hidden.css'
import * as styles from './task-activity.css'
import * as panelStyles from './panel.css'
import { RecordLabel, recordIcon, rowDuration, rowStatus } from './record'
import {
  adjacentMark,
  enclosingRecords,
  openRoot,
  waterfall,
  waterfallStops,
  type WaterfallRow,
} from './trace'
import { Waterfall } from './waterfall'

export { rowLabel } from './record'

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
/** What every row of the root list needs from the page beyond its own record. */
interface RootScope {
  /** The top-level record whose waterfall shows. */
  open: string | null
  /**
   * Where a row leads: this list with that record's waterfall open, and a Task selected
   * too, or for a glimpse, the record on its own Activity page.
   */
  href: (row: ActivityRow) => string
  /** A glimpse opens each record at the top of its Activity page. */
  glimpse: boolean
}
/** Navigation that reads the reader's latest position when it runs, not when rows rendered. */
interface RowActions {
  /** `keyboard`: Enter or an arrow in the table, which keeps focus in the table. */
  select: (row: ActivityRow, keyboard?: boolean) => void
  /** From the keyboard, a collapse that also changes the selection replaces the address. */
  toggle: (name: string, keyboard?: boolean) => void
}
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

/** One top-level record of the list: a Task or a standalone Run. The whole row opens it. */
const RootRow = memo(function RootRow({ row, scope }: { row: ActivityRow; scope: RootScope }) {
  const providers = useContext(ProviderMarksContext)
  const to = scope.href(row)
  return (
    <Table.Row
      recordKey={row.name}
      selected={row.name === scope.open}
      status={rowStatus(row)}
      to={to}
      preventScrollReset={!scope.glimpse}
    >
      <Table.Cell mono>
        <UtcTime value={row.startedAt} />
      </Table.Cell>
      <Table.Cell>
        <Table.Record icon={recordIcon(row, providers)}>
          <Table.RowLink to={to}>
            <RecordLabel row={row} />
          </Table.RowLink>
        </Table.Record>
      </Table.Cell>
      <Table.Cell mono>{rowDuration(row)}</Table.Cell>
      <Table.Cell>
        <ContextImpact
          bytes={row.contextImpactBytes}
          partial={row.kind === 'task' && row.contextImpactPartial}
        />
      </Table.Cell>
    </Table.Row>
  )
})
/** Where the keys start from when the rail shows `row`. A Run's Event is a mark on its Run. */
function recordPosition(row: ActivityRow, parents: string[]): StopPosition {
  return row.kind === 'output'
    ? { name: row.run, parents: parents.slice(1) }
    : { name: row.name, parents }
}
const NO_RECORDS: ReadonlySet<string> = new Set()
const BOUNDARY: ActivityStop = { kind: 'boundary' }
/** A Task's name, as Coral names one: a workspace's Task, and nothing under it. */
const TASK_NAME = /^workspaces\/[^/]+\/tasks\/[^/]+$/
const NO_ROWS: readonly WaterfallRow[] = []
/** This URL with `name` selected, as a link shows it. A click navigates with more state. */
function selectionHref(listPath: string, search: string, name: string) {
  const query = new URLSearchParams(search)
  for (const key of selectionKeys) query.delete(key)
  query.set('record', name)
  return `${listPath}?${query}`
}
/** Stands for a root's name in an address built once for every root. It needs no encoding. */
const ROOT_PLACEHOLDER = 'activity-root-placeholder'
/** Data that marks a live toggle, so focus can return to it when the record changes. */
const liveToggleAttribute = 'data-activity-live'
/** Each live scope's toggle: its name, and its tooltip while off, on, or blocked. */
const liveToggles: Record<LiveScope, { label: string; off: string; on: string; blocked: string }> =
  {
    run: {
      label: 'Follow latest record',
      off: 'Follow the latest record of this execution.',
      on: 'Stop following the latest record.',
      blocked: 'Too many records are open. Open a task or run from the list first.',
    },
    task: {
      label: 'Follow this task',
      off: 'Follow the runs of this task in order.',
      on: 'Stop following this task.',
      blocked: 'Too many records are open. Open a task or run from the list first.',
    },
    latest: {
      label: 'Follow latest activity',
      off: 'Follow the newest task or run.',
      on: 'Stop following the latest activity.',
      blocked: 'Too many records are open. Open a task or run from the list first.',
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
  trace,
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
  // The records whose rail is closing: the one it shows, and the one a step is about
  // to show. It plays its exit while the address catches up, and a load that lands
  // meanwhile, the step's own included, cannot reopen it. A new selection ends it.
  const [closing, setClosing] = useState<readonly string[] | null>(null)
  if (closing !== null && !(selected !== null && closing.includes(selected))) setClosing(null)
  // A closing rail keeps showing its record until its exit ends, even when a step
  // the close covers renders meanwhile.
  // A Task heads its own waterfall, whose header states its details: selecting it, from
  // the list or by a link, opens no rail.
  const railSelected = selected !== null && TASK_NAME.test(selected) ? null : selected
  const [shown, setShown] = useState({ page, record: selected })
  const showing = railSelected !== null && closing === null
  if (showing && (shown.page !== page || shown.record !== selected))
    setShown({ page, record: selected })
  const detail = showing ? { page, record: selected } : shown
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
  const tableRef = useRef<HTMLDivElement>(null)
  // ↑, ↓ and Enter reach the list's records, as in every table whose rows open a page.
  // The waterfall, the rail and every other control keep their own arrows.
  const rootsRef = Table.useRowCursor({ reach: 'background' })
  const popupRef = useRef<HTMLDivElement>(null)
  // Another record starts at its top, in the same rail.
  useLayoutEffect(() => {
    if (selected && popupRef.current) popupRef.current.scrollTop = 0
  }, [selected])
  const glimpse = more !== undefined
  // A glimpse never loads more, and lists only the latest roots a live update merged in.
  const roots = useMemo(
    () => (more ? page.rows.slice(0, more.roots) : page.rows),
    [more, page.rows],
  )
  // The top-level record whose waterfall shows. A reader also names one the address does
  // not, so a wide screen never opens on an empty waterfall. A glimpse lists roots only.
  const namedRoot = glimpse ? null : openRoot(page)
  const rootName = glimpse ? null : (trace?.store.root ?? namedRoot)
  // Without a reader, the waterfall shows what the page itself holds.
  const pageStore = useMemo(
    () => (rootName && !trace ? absorbPage(openTrace(rootName), page) : null),
    [rootName, trace, page],
  )
  const rooted = trace?.store ?? pageStore
  // Coral names no top-level record for a selection outside this list's scope, such as an
  // Agent's call you review. The waterfall then shows that record alone, with no history.
  // Until the first read says whether Coral returns it, the record shows alone and its
  // history loads: nothing says "unavailable" before Coral does.
  const looking = Boolean(trace) && !rooted?.unavailable
  const lone = page.selectedRow && page.selectedRow.kind !== 'output' ? page.selectedRow : null
  const alone = useMemo(
    () => (lone ? { ...loneTrace(lone, looking), failed: Boolean(rooted?.failed) } : null),
    [lone, looking, rooted?.failed],
  )
  const outsideScope = Boolean(alone) && (!rooted?.rootRow || rooted.unavailable)
  const store = outsideScope ? alone : rooted
  // The records you collapsed in the open waterfall. Another root starts with none.
  const [folded, setFolded] = useState<{
    root: string | null
    names: ReadonlySet<string>
    selected: string | null
  }>({ root: rootName, names: NO_RECORDS, selected })
  let collapsed = folded.root === rootName ? folded.names : NO_RECORDS
  // A new selection opens the records around it, so it never starts hidden. A collapse
  // around the record the rail shows stays: the rail moves to the collapsed record.
  const around = useMemo(
    () => (page.selectedRow ? enclosingRecords(store, page.selectedRow) : []),
    [store, page.selectedRow],
  )
  if (folded.selected !== selected && around.some((name) => collapsed.has(name)))
    collapsed = new Set([...collapsed].filter((name) => !around.includes(name)))
  if (folded.root !== rootName || folded.names !== collapsed || folded.selected !== selected)
    setFolded({ root: rootName, names: collapsed, selected })
  // The dense timeline shows every record: nothing in it collapses.
  const layout = useMemo(() => (store ? waterfall(store, NO_RECORDS, 0) : null), [store])
  // Where ↑ and ↓ go: the rows of the waterfall, which stop at children it does not hold.
  // The Task heads the waterfall above its table, so the keys stop at the records under it:
  // its stop is a boundary, which keeps every other stop at its row's index.
  const stops = useMemo(
    () =>
      waterfallStops(layout?.rows ?? NO_ROWS).map((stop) =>
        stop.kind === 'record' && stop.row.kind === 'task' ? BOUNDARY : stop,
      ),
    [layout],
  )
  const open = railSelected !== null && closing === null
  // Live mode follows the shown record's Run, the newest activity, or one Task's Runs.
  // Any other selection, and closing the rail, turns it off. A glimpse never follows.
  const liveScope = glimpse ? null : nav.liveScope
  const liveOn = liveScope !== null
  // The records that lead to the selected one stay expanded while live mode moves on.
  const selectedChain = liveChain(page.selectedRow)
  // With the rail open, the keys start from the record it shows, or is about to.
  const shownPosition: StopPosition | null = selected
    ? page.selectedRow
      ? recordPosition(page.selectedRow, around)
      : { name: selected, parents: [] }
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
  const close = () => {
    // With an unsent comment, the rail stays while the review asks whether to discard it.
    // A step, pick or other navigation whose page has not rendered yet closes with
    // it, so it cannot reopen the rail when it renders. `pending` names one whose
    // record the table does not show, such as live mode's.
    if (!unsent)
      setClosing([...new Set([selected, pending, position?.name].filter((name) => name != null))])
    void navigate(nav.backHref, {
      preventScrollReset: true,
      state: tokens.next(),
    })
  }
  const [tableFocused, setTableFocused] = useState(false)
  // The table reveals the record a key moved to, until its route names another.
  const [reveal, setReveal] = useState({
    route: revealRecord,
    record: revealRecord,
  })
  if (reveal.route !== revealRecord) setReveal({ route: revealRecord, record: revealRecord })
  // Rows and keys navigate with the reader's latest position, read when they act.
  const now = {
    nav,
    navigate,
    live,
    more,
    selected,
    shownPosition,
    pending,
    pendingToken,
    closing,
    open,
    roots,
    stops,
    cursor,
    position,
    activeStop,
    collapsed,
    around,
    layout,
    store,
    trace,
    rootName,
    unsent,
    locationKey,
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
  // The record whose rail Enter in the table or the list, or live mode, is about to open.
  // That opening leaves focus where it was. Any other selection, any pointer press, and an
  // opening of the rail clear it, so it never reaches a later opening.
  const keepFocus = useRef<string | null>(null)
  const wasOpen = useRef(open)
  useLayoutEffect(() => {
    // A selection the open rail shows in place, or one that opens no rail, starts no opening.
    if (keepFocus.current !== null && (wasOpen.current || keepFocus.current !== selected))
      keepFocus.current = null
    wasOpen.current = open
  }, [selected, open])
  // A Task picked in the list is an explicit selection, as a pick in the table is: it opens
  // the Task whose rail is closing, though the address still names it until the close lands.
  // A Run there opens its waterfall alone, so it leaves a closing rail closed.
  useEffect(() => {
    if (glimpse) return
    const pick = (event: MouseEvent) => {
      const target = event.target
      if (!(target instanceof Element) || !rootsRef.current?.contains(target)) return
      const name = target.closest('[data-record-key]')?.getAttribute('data-record-key')
      const current = latest.current
      if (!name || !current.closing?.includes(name)) return
      if (!current.roots.some((row) => row.name === name && row.kind === 'task')) return
      setClosing(current.closing.filter((closed) => closed !== name))
    }
    window.addEventListener('click', pick, true)
    return () => window.removeEventListener('click', pick, true)
  }, [glimpse, rootsRef])
  useEffect(() => {
    const press = () => void (keepFocus.current = null)
    window.addEventListener('pointerdown', press, true)
    return () => window.removeEventListener('pointerdown', press, true)
  }, [])
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
            : current.selected
      if (row.name === target) return
      // Picking the closed record again reopens it. Picking the record a pending step
      // was going to show leaves the rail closed until that record loads.
      if (row.name === current.selected) setClosing(null)
      else if (current.closing?.includes(row.name))
        setClosing(current.closing.filter((name) => name !== row.name))
      setKeyStep(null)
      keepFocus.current = keyboard ? row.name : null
      // Going back to the shown record cancels the rail's own pending navigation,
      // which has no history entry yet, so the address is replaced. Back or Forward
      // already moved through history, so a pick after either is a new entry.
      // A step from one mark to the next adds no entry either, as a step between rows.
      const replace =
        (row.name === current.selected && tokens.current(current.pendingToken)) ||
        (keyboard && current.open && row.kind === 'output')
      void current.navigate(current.nav.recordHref(row.name, current.rootName), {
        preventScrollReset: true,
        replace,
        state: tokens.next(),
      })
    },
    toggle: (name, keyboard = false) => {
      const current = latest.current
      // The root heads its waterfall and never closes.
      if (name === current.rootName) return
      const collapsing = !current.collapsed.has(name)
      const names = new Set(current.collapsed)
      if (collapsing) names.add(name)
      else names.delete(name)
      setFolded((value) => ({ ...value, names }))
      setKeyStep(null)
      if (!collapsing) return
      const enclosing = current.around
      // A collapse that hides the record the rail shows opens the collapsed record
      // instead. From the keyboard it replaces the address, as a step does.
      if (current.open && enclosing.includes(name))
        return void current.navigate(current.nav.recordHref(name, current.rootName), {
          preventScrollReset: true,
          replace: keyboard,
          state: tokens.next(),
        })
      // Collapsing the Run live mode follows, or its Task, stops following it.
      if (current.liveOn && liveChain(current.selectedRow).includes(name))
        void current.navigate(current.nav.liveHref(null), { preventScrollReset: true })
    },
  }))
  useEffect(() => {
    if (glimpse) return
    /** Moves the rail's record, or the cursor, one record up or down. */
    const move = (step: 1 | -1) => {
      const current = latest.current
      const from = current.open ? current.position : current.cursor
      const next = stepStop(current.stops, from, step)
      if (!next) {
        // The step stops at children the trace does not hold yet, and asks for the ones
        // it stopped at first, so the next press goes on.
        const beside = from ? current.layout?.rows[resolveStop(current.stops, from) + step] : null
        if (beside?.kind === 'gap' && beside.reason === 'unloaded')
          current.trace?.prefer([
            { parent: beside.parent, at: step > 0 ? beside.from : beside.to - 1 },
          ])
        return
      }
      const target = stopPosition(next)
      // With an unsent comment the review asks first; the table and rail stay put.
      if (!current.unsent) setReveal((value) => ({ ...value, record: next.row.name }))
      if (!current.open) return setCursor(target)
      if (!current.unsent) setKeyStep({ at: current.locationKey, to: target })
      void current.navigate(current.nav.recordHref(next.row.name, current.rootName), {
        preventScrollReset: true,
        replace: true,
        state: tokens.next(),
      })
    }
    /** Space: whether it toggled an expandable record. The dense timeline has none. */
    const toggle = (_owner: 'table' | 'rail') => false
    /**
     * ← and →: the mark before or after the record the rail shows, or else the cursor's,
     * in its Run's order. From the Run itself, → starts at its first mark and ← at its last.
     */
    const mark = (step: 1 | -1) => {
      const current = latest.current
      const from = (current.open ? current.selectedRow : null) ?? current.activeStop?.row
      const next = adjacentMark(current.store, current.layout?.rows ?? NO_ROWS, from, step)
      if (next === null) return false
      if (next) actions.select(next, true)
      return true
    }
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.defaultPrevented || event.isComposing) return
      if (event.altKey || event.ctrlKey || event.metaKey || event.shiftKey) return
      if (
        !['ArrowUp', 'ArrowDown', 'ArrowLeft', 'ArrowRight', ' ', 'Enter', 'Escape'].includes(
          event.key,
        )
      )
        return
      const table = tableRef.current
      const rail = popupRef.current
      const target = event.target
      if (!(target instanceof Element)) return
      if (!table) return
      // The list of top-level records moves its own cursor. With no waterfall open, the
      // arrows from the page reach the list too.
      if (rootsRef.current?.contains(target)) {
        const current = latest.current
        const name = target.closest('[data-record-key]')?.getAttribute('data-record-key')
        if (event.key === 'Enter' && name && !(current.open && name === current.selected))
          keepFocus.current = name
        return
      }
      if (!latest.current.stops.length && !latest.current.open) return
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
      if (event.key === 'ArrowLeft' || event.key === 'ArrowRight') {
        if (mark(event.key === 'ArrowRight' ? 1 : -1)) event.preventDefault()
      } else if (event.key === ' ') {
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
  }, [glimpse, actions, tokens, rootsRef])
  // Where focus was when live mode last changed the record. The new record's panel
  // replaces the old one, so focus that was in it returns to the same place.
  const refocus = useRef<{
    record: string | null
    on: 'toggle' | 'rail'
  } | null>(null)
  useLayoutEffect(() => {
    const noted = refocus.current
    const rail = popupRef.current
    if (!noted || noted.record === selected) return
    refocus.current = null
    const active = document.activeElement
    if (!rail || (active && active !== document.body && document.contains(active))) return
    const target =
      noted.on === 'toggle' ? rail.querySelector<HTMLElement>(`[${liveToggleAttribute}]`) : null
    ;(target ?? rail).focus({ preventScroll: true })
  }, [selected])
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
    if (presented.current?.record !== selected)
      presented.current = { record: selected, at: Date.now() }
  }, [selected])
  useLayoutEffect(() => {
    if (presentedScope.current !== liveScope && liveScope === 'latest')
      presented.current = { record: selected, at: -Infinity }
    presentedScope.current = liveScope
  }, [liveScope, selected])
  const connected = live?.connected ?? true
  // How many payload or source viewers you have open expanded. Live mode holds the record
  // meanwhile.
  const [inspecting, setInspecting] = useState(0)
  const inspect = useCallback(
    (expanded: boolean) => setInspecting((count) => count + (expanded ? 1 : -1)),
    [],
  )
  const guarded = useCommentGuard()
  // Whether the review guarded an unsent comment when live mode last looked.
  const wasGuarded = useRef(guarded)
  // The request the rail reviews, and the decision its approval review submits.
  const reviewed =
    page.detail?.kind === 'invocation' || page.detail?.kind === 'run' ? page.detail.request : null
  const reviewedApproval = reviewed?.kind === 'approval' ? reviewed.name : undefined
  // The location where the advance started, which also marks the reads it takes first.
  const [advance, setAdvance] = useState<string | null>(null)
  // A decision you make in the review advances it once, when your decision wins and the
  // read after it lands.
  const decision = useDecisionOutcome(reviewedApproval ?? 'activity:no-review', ({ recorded }) => {
    if (recorded) setAdvance(locationKey)
  })
  // It holds through the reads it takes first, and ends with its selection. Any other
  // navigation ends it, and Back or Forward to one of its reads never resumes it.
  const advancing =
    advance !== null && (advance === locationKey || advanceMark(locationState) === advance)
  if (advance !== null && !advancing) setAdvance(null)
  const following = liveOn || advancing
  // The step live mode, or an advance, takes next: the Run's oldest action, else its
  // newest record, in the Run its scope names. Null holds the record: an unsent
  // comment, a payload you inspect expanded, a pending navigation or decision, or a lost
  // connection. 'done' rests on the record shown. Only the newest activity follows while
  // the rail is closed.
  const follow = ((): FollowStep | 'done' | 'blocked' | null => {
    if (!following || glimpse || closing !== null || guarded || inspecting || !connected)
      return null
    // A Task shows in its header, as a record shows in the rail: live mode follows from it.
    const inView = open || (selected !== null && railSelected === null)
    if (page.unavailable || (!inView && liveScope !== 'latest')) return null
    if (transition.state !== 'idle' || decision.state !== 'idle') return null
    // In an archived workspace you cannot act, so only the newest record counts.
    return liveFollow(page, nav, {
      scope: liveScope,
      actionable: !archived,
      shownRecord: selected,
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
    step.record !== selected
  // A read an advance takes first carries its mark, so the advance holds through it.
  const followMark = advancing && step && !step.last ? advance : undefined
  // A selection that opens the rail leaves focus where it is.
  const followOpens = Boolean(step?.last) && !open
  const followRecord = step?.record ?? null
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
            record: selected,
            on: active.closest(`[${liveToggleAttribute}]`) ? 'toggle' : 'rail',
          }
        : null
    if (followOpens) keepFocus.current = followRecord
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
    followRecord,
    followReplaces,
    rested,
    selected,
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
  // A Task heads its own waterfall: the header above it states what a rail would of the
  // Task, and follows it live.
  const rootTask = !glimpse && store?.rootRow?.kind === 'task' ? store.rootRow : null
  const taskDetail =
    rootTask && page.detail?.kind === 'task' && page.detail.task.name === rootTask.name
      ? page.detail
      : null
  const taskHeader = rootTask && (
    <TaskHeader
      task={taskDetail?.task ?? rootTask}
      // The trace reads the Task's own detail once, also when a link opened a record under it.
      feedback={store?.taskDetail?.feedback}
      actions={
        <LiveToggle
          scope="task"
          on={liveScope === 'task'}
          blocked={
            liveScope === 'task'
              ? stalled
              : // Following a Task also expands one of its Runs.
                !nav.liveRoom([rootTask.name, `${rootTask.name}/run`], selectedChain)
          }
          onToggle={() => toggleLive('task', [rootTask.name], windowAt(rootTask.name, 0))}
        />
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
  const loadable = more ? undefined : list
  // A root's address keeps the reading position, so a reload opens the list there. It is
  // the same for every root but for the root's name.
  const rootTemplate = nav.rootHref(ROOT_PLACEHOLDER)
  // A Task's row also selects the Task, so the rail shows it beside its waterfall.
  const taskTemplate = nav.rootHref(ROOT_PLACEHOLDER, true)
  const scope = useMemo<RootScope>(
    () => ({
      open: rootName,
      href: glimpse
        ? revealHref
        : (row) =>
            (row.kind === 'task' ? taskTemplate : rootTemplate).replaceAll(
              ROOT_PLACEHOLDER,
              encodeURIComponent(row.name),
            ),
      glimpse,
    }),
    [rootName, rootTemplate, taskTemplate, glimpse],
  )
  // Top-level records sit under the UTC day they started.
  const tableRows = activityDays(roots).map(({ day, key, rows: dayRows }) => {
    const label = dayLabel(day)
    return (
      <Table.Group
        key={key}
        label={label}
        // The day orients; the records stay the strongest text.
        heading={
          <Table.Cell fullWidth>
            <Typography.BodySmall variant="secondary">{label}</Typography.BodySmall>
          </Table.Cell>
        }
      >
        {dayRows.map((row) => (
          <RootRow key={row.name} row={row} scope={scope} />
        ))}
      </Table.Group>
    )
  })
  const providers = useContext(ProviderMarksContext)
  // The open root, when the loaded list does not hold it.
  const outside =
    !outsideScope && store?.rootRow && !roots.some((row) => row.name === store.rootRow?.name)
      ? store.rootRow
      : null
  // A record's address differs from this one only in the selection. Without it, the
  // address is the same whichever record is selected, so a step renders no link again.
  const unselected = useMemo(() => {
    const query = new URLSearchParams(search)
    for (const key of selectionKeys) query.delete(key)
    return query.toString()
  }, [search])
  const opener = useMemo(
    () => ({
      href: (row: ActivityRow) => selectionHref(page.listPath, unselected, row.name),
      select: (row: ActivityRow) => actions.select(row),
    }),
    [page.listPath, unselected, actions],
  )
  // The waterfall brings the record a key or live mode moved to into view.
  // Only a key's own move: the route's record is the selection, which shows already.
  const revealed = reveal.record === reveal.route ? undefined : reveal.record
  const shownKey =
    selected && page.selectedRow ? recordPosition(page.selectedRow, around).name : selected
  // The table reveals each once, when its row is there: a record that stays selected
  // never pulls the table back, and one that loads later still comes into view.
  const [brought, setBrought] = useState({ shown: shownKey, moved: revealed, record: shownKey })
  if (brought.shown !== shownKey || brought.moved !== revealed)
    setBrought({
      shown: shownKey,
      moved: revealed,
      // Whichever changed last. A key's move that ends leaves the selection revealed.
      record: (brought.moved !== revealed ? revealed : undefined) ?? shownKey,
    })
  return (
    <ActivityConnectedContext value={live?.connected ?? true}>
      <ActivityInspectContext value={inspect}>
        <div
          className={styles.root}
          // A narrow screen shows one pane: the list, or the waterfall the address names.
          data-activity-view={glimpse ? 'glimpse' : namedRoot ? 'trace' : 'list'}
          // Beside a docked rail, the list gives its room to the waterfall.
          data-activity-rail={open ? 'open' : 'closed'}
        >
          <div className={styles.main}>
            <div className={styles.toolbar}>
              {live && !live.connected && (
                <Typography.BodySmall role="status">
                  Activity is unavailable. Reconnecting…
                </Typography.BodySmall>
              )}
            </div>
            <div className={styles.list}>
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
              <div className={glimpse ? styles.list : styles.panes}>
                <div className={glimpse ? styles.list : styles.rootPane}>
                  <Table.Container
                    readingKey={windowKey}
                    readingPaused={live && !live.connected}
                    readingFollowStart={live?.followRoot}
                    onReadingStartChange={live?.atStart}
                    revealRecord={glimpse ? undefined : (namedRoot ?? undefined)}
                    onReadingPositionLost={() => setLostPosition(windowKey)}
                    onVisibleRecordsChange={live?.visible}
                    ariaLabel={glimpse ? 'Activity records' : 'Tasks and runs'}
                    // A glimpse shares its page with other tables, which keep the arrows.
                    ref={glimpse ? undefined : rootsRef}
                    density="compact"
                    layout="scroll"
                    columns={[
                      { width: glimpse ? 104 : 92, label: glimpse ? 'TIME · UTC' : 'UTC' },
                      {
                        width: glimpse ? 'minmax(320px, 1fr)' : 'minmax(140px, 1fr)',
                        label: 'RECORD',
                      },
                      { width: glimpse ? 88 : 64, label: 'DUR' },
                      {
                        width: glimpse ? 150 : 88,
                        label: 'CONTEXT',
                        ariaLabel: 'Context impact',
                        align: 'right',
                      },
                    ]}
                  >
                    <Table.Head />
                    <Table.Body>
                      {loadable?.newer && (
                        <LoadRecords control={loadable.newer} direction="newer" />
                      )}
                      {tableRows}
                      {loadable?.older && (
                        <LoadRecords control={loadable.older} direction="older" />
                      )}
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
                        <Table.Row
                          recordKey={outside.name}
                          selected
                          status={rowStatus(outside)}
                          to={scope.href(outside)}
                          preventScrollReset
                        >
                          <Table.Cell mono>
                            <UtcTime value={outside.startedAt} />
                          </Table.Cell>
                          <Table.Cell>
                            <Table.Record
                              icon={recordIcon(outside, providers)}
                              metadata={
                                <>
                                  <span>Outside this workspace or page:</span>
                                  {/* This row sits outside every day group, so it names its own day. */}
                                  <span>{dayLabel(utcDay(outside.startedAt))}</span>
                                </>
                              }
                            >
                              <Table.RowLink to={scope.href(outside)}>
                                <RecordLabel row={outside} />
                              </Table.RowLink>
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
                {!glimpse && (
                  <div
                    className={styles.tracePane}
                    onFocus={(event) => {
                      const table = tableRef.current
                      // A record's link is not a Tab stop of its own. Focus that lands on one,
                      // as a click on it leaves it, rests on the table, whose cursor row shows it.
                      if (
                        table?.contains(event.target) &&
                        event.target.matches('[data-record-link], [data-timeline-mark]')
                      )
                        return table.focus({ preventScroll: true })
                      setTableFocused(Boolean(table?.contains(event.target)))
                    }}
                    onBlur={(event) =>
                      setTableFocused(Boolean(tableRef.current?.contains(event.relatedTarget)))
                    }
                  >
                    <Waterfall
                      store={store}
                      // A record Coral returns no history for has nothing left to read.
                      reader={outsideScope && !looking ? undefined : trace}
                      rows={layout?.rows ?? NO_ROWS}
                      selected={selected}
                      holder={page.selectedRow?.kind === 'output' ? page.selectedRow.run : null}
                      active={activeRecord}
                      open={opener}
                      toggle={actions.toggle}
                      reveal={brought.record ?? undefined}
                      paused={
                        page.listUnavailable ||
                        (live ? !live.connected : false) ||
                        Boolean(trace?.restoring)
                      }
                      tableRef={tableRef}
                      listHref={nav.listHref}
                      header={taskHeader}
                    />
                  </div>
                )}
              </div>
            </div>
          </div>
          {/* One rail for every record: another selection replaces its content in place.
              On a wide screen it docks beside the list, which shrinks to make room. */}
          <Dialog.Rail docked open={open} onOpenChange={(next) => !next && close()}>
            <Dialog.Portal>
              <Dialog.Backdrop />
              <Dialog.Popup
                size="xl"
                placement="right"
                fullWidthOnMobile
                ref={popupRef}
                // Enter in the table or the list, or live mode, opens the rail and leaves focus
                // where it was. Otherwise focus goes to the close control, as before the live toggle
                // came first, so no toggle's tooltip opens and Escape closes the rail.
                initialFocus={() => {
                  const keep = keepFocus.current !== null && keepFocus.current === selected
                  keepFocus.current = null
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
                    <ActivityDetail key={detail.record} page={detailPage} />
                  </ExecutionSourceSplit>
                ) : (
                  <PanelClose value={ownHeader ? railControls : null}>
                    <ActivityDetail key={detail.record} page={detailPage} />
                  </PanelClose>
                )}
              </Dialog.Popup>
            </Dialog.Portal>
          </Dialog.Rail>
        </div>
      </ActivityInspectContext>
    </ActivityConnectedContext>
  )
}

export { duration } from './duration'
