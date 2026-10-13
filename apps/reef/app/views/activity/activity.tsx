import {
  createContext,
  memo,
  useCallback,
  useContext,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  useSyncExternalStore,
} from 'react'
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
import {
  activityStops,
  expandable,
  type ActivityStop,
  revealHref,
  selectionKeys,
  shownRecord,
  stepStop,
  useActivityNavigation,
} from './navigation'
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
/**
 * The keyboard cursor's record identity. A move re-renders only the two rows it
 * leaves and reaches, not every memoized row.
 */
class CursorStore {
  private value: string | null = null
  private listeners = new Set<() => void>()
  subscribe = (listener: () => void) => {
    this.listeners.add(listener)
    return () => void this.listeners.delete(listener)
  }
  get = () => this.value
  set(value: string | null) {
    if (value === this.value) return
    this.value = value
    for (const listener of this.listeners) listener()
  }
}
const CursorContext = createContext(new CursorStore())
function useCursorOn(identity: string) {
  const store = useContext(CursorContext)
  return useSyncExternalStore(
    store.subscribe,
    () => store.get() === identity,
    () => false,
  )
}
/**
 * A plain key nothing took yet: not composing a character, and not modified, so
 * modified keys keep their own meaning.
 */
const plainKey = (event: KeyboardEvent) =>
  !event.defaultPrevented &&
  !event.isComposing &&
  !(event.altKey || event.ctrlKey || event.metaKey || event.shiftKey)
/** A plain key pressed on the element that handles it, not on a control inside it. */
const ownKey = (event: React.KeyboardEvent) =>
  event.target === event.currentTarget && plainKey(event.nativeEvent)
const arrowStep = (event: KeyboardEvent) =>
  event.key === 'ArrowDown' ? 1 : event.key === 'ArrowUp' ? -1 : 0
/** A plain left click; other clicks keep the link's own behaviour, such as a new tab. */
const plainClick = (event: React.MouseEvent) =>
  event.button === 0 && !event.metaKey && !event.ctrlKey && !event.shiftKey && !event.altKey

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
  const toggles = !scope.glimpse && expandable(row)
  const branch = scope.glimpse ? undefined : branches?.[row.name]
  const expanded = scope.expanded.has(row.name)
  const cursorOn = useCursorOn(activityRowIdentity(row))
  const recordRowProps = {
    recordKey: row.name,
    selected: row.name === scope.selected,
    // An open detail marks its record as selected instead.
    active: cursorOn && scope.selected === null,
    status: rowStatus(row),
    onClick: (event: React.MouseEvent<HTMLElement>) => {
      if (!(event.target as HTMLElement).closest('a,button,input,select')) actions.select(row)
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
          onToggle={toggles ? () => actions.toggle(row.name) : undefined}
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
            onClick={(event) => {
              if (!plainClick(event)) return
              event.preventDefault()
              actions.select(row)
            }}
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
  if (!branch || !expandable(row)) return <Table.Row {...recordRowProps}>{cells}</Table.Row>
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
/** The stop that shows the record `name`, by its detail or its child-window identity. */
function stopOf(stops: ActivityStop[], name: string) {
  return stops.find((stop) => stop.row.name === name || stop.identity === name)
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
  const { search } = useLocation()
  const pending = useNavigation()
  const shown = shownRecord(page.selected, search)
  // The record the URL names, or the one a navigation now loading will name.
  const headingTo = new URLSearchParams(
    pending.state === 'idle' ? search : (pending.location?.search ?? search),
  ).get('record')
  // One drawer shows each record in turn, so a close belongs to the drawer: the
  // record it opened on, which arrow steps keep. A step that lands while it
  // closes cannot reopen it. Any other record that arrives opens a new drawer;
  // until it arrives, the closing drawer stays closed.
  const [steppedTo, setSteppedTo] = useState<string | null>(null)
  const [drawerRecord, setDrawerRecord] = useState(shown)
  const [arrived, setArrived] = useState(shown)
  if (arrived !== shown) {
    setArrived(shown)
    setSteppedTo(null)
    if (!shown || shown !== steppedTo) setDrawerRecord(shown)
  }
  const where = useRef({ shown, headingTo, steppedTo })
  useLayoutEffect(() => {
    where.current = { shown, headingTo, steppedTo }
  })
  const drawer = Dialog.useCloseAfterExit(page.selected ? (drawerRecord ?? shown) : null, () => {
    // A record opened while the drawer closed takes its place: the finished
    // close does not navigate it away, however slowly it loads. A step the
    // close interrupted is no such record, so the close still leaves it.
    const { shown: closing, headingTo: next, steppedTo: stepping } = where.current
    if (next && next !== closing && next !== stepping) return
    void navigate(nav.backHref, { preventScrollReset: true })
  })
  // With an unsent comment, a close navigates at once, so the comment guard asks
  // over the open drawer, as it does for a step: keeping the comment keeps the
  // detail. Without one, the drawer plays its exit before it navigates.
  const unsent = useUnsentComment()
  const closeDrawer = {
    ...drawer,
    onOpenChange: (next: boolean) => {
      if (!next && unsent) void navigate(nav.backHref, { preventScrollReset: true })
      else drawer.onOpenChange(next)
    },
  }
  const observed = live !== undefined
  const glimpse = more !== undefined
  // A glimpse never loads more, and lists only the latest roots a live update merged in.
  const roots = more ? page.rows.slice(0, more.roots) : page.rows
  const loadable = more ? undefined : list
  // A glimpse has no keyboard cursor; the full list's table owns the keys.
  const stops = useMemo(() => (glimpse ? [] : activityStops(page, roots)), [glimpse, page, roots])
  const [cursor, setCursor] = useState<string | null>(null)
  // A collapsed Run shows no final-value row, so its final value rests on the
  // Run's own row. An expanded Run never stands in for it: that would wrap.
  const shownStop = !shown
    ? undefined
    : (stopOf(stops, shown) ??
      (page.selected && shown !== page.selected && !page.expanded.includes(page.selected)
        ? stopOf(stops, page.selected)
        : undefined))
  // The cursor follows the open detail, so it rests on the last record the detail showed.
  // A step that lands while the drawer closes was never shown, so it is not followed.
  const [followedSelection, setFollowedSelection] = useState<string | null>(null)
  if (followedSelection !== shown && (!shown || drawer.open)) {
    setFollowedSelection(shown)
    if (shown) setCursor(shownStop?.identity ?? null)
  }
  // A record that leaves the table takes the cursor with it: no neighbour inherits it.
  // A cursor that just followed the detail comes from these stops, so it stays.
  else if (cursor !== null && !stops.some((stop) => stop.identity === cursor)) setCursor(null)
  const [cursorStore] = useState(() => new CursorStore())
  // The cursor shows only while focus is in the table. Its keys act when the
  // table itself has focus; a control in a row keeps its own.
  const [tableFocused, setTableFocused] = useState(false)
  useLayoutEffect(
    () => cursorStore.set(tableFocused ? cursor : null),
    [cursorStore, cursor, tableFocused],
  )
  // Rows navigate with the reader's latest position, read when the click happens.
  const latest = useRef({
    nav,
    navigate,
    live,
    more,
    expanded: page.expanded,
    stops,
    cursor,
  })
  useLayoutEffect(() => {
    latest.current = { nav, navigate, live, more, expanded: page.expanded, stops, cursor }
  })
  const [actions] = useState<RowActions>(() => ({
    select: (row) => {
      // A record opened from the table is no step, even one a guard held back.
      setSteppedTo(null)
      const current = latest.current
      // A glimpse opens the record on its Activity page, at the top of that page.
      if (current.more) void current.navigate(revealHref(row))
      else void current.navigate(current.nav.recordHref(row.name), { preventScrollReset: true })
    },
    toggle: (name) => {
      const current = latest.current
      // Collapsing the branch that holds the cursor moves the cursor to its parent.
      const held = current.stops.find((stop) => stop.identity === current.cursor)
      const parent = current.stops.find((stop) => stop.row.name === name)
      if (current.expanded.includes(name) && parent && held?.ancestors.includes(name))
        setCursor(parent.identity)
      void current.navigate(current.nav.toggleHref(name), { preventScrollReset: true })
    },
    viewport: (parent, value) => latest.current.live?.viewport(parent, value),
  }))
  const expandedKey = page.expanded.join('\n')
  const finalValueRun =
    page.selectedRow?.kind === 'run' && page.selectedRow.hasFinalValue
      ? page.selectedRow.name
      : null
  const table = useRef<HTMLDivElement>(null)
  // The cursor's row shows in full below the pinned headings.
  const revealStop = (stop: ActivityStop | undefined) => {
    const key = stop && CSS.escape(stop.row.name)
    const row = key && table.current?.querySelector<HTMLElement>(`[data-record-key="${key}"]`)
    if (row) Table.revealRow(row)
  }
  const cursorStop = stops.find((stop) => stop.identity === cursor)
  // Only the table itself takes these keys: a control in a row, or anything
  // elsewhere on the page, keeps its own. ↑ and ↓ move the cursor. Space
  // toggles the cursor's own Task or Run, expanded or not; a non-expandable
  // record has none, so Space collapses its closest enclosing Run or Task
  // and the cursor follows there. Enter opens the cursor record's detail.
  // ↑ or ↓ moves the cursor. Returns whether the table took the key, even at an
  // end or a gap where the cursor stays.
  const moveCursor = (step: 1 | -1) => {
    if (!stops.length) return false
    const next = stepStop(stops, cursor, step)
    if (next) {
      setCursor(next.identity)
      revealStop(next)
    }
    return true
  }
  const tableKey = (event: React.KeyboardEvent<HTMLDivElement>) => {
    if (glimpse || !ownKey(event)) return
    const step = arrowStep(event.nativeEvent)
    if (step) {
      if (moveCursor(step)) event.preventDefault()
      return
    }
    // A held Space or Enter acts once.
    if (event.repeat || !cursorStop || page.selected) return
    if (event.key === 'Enter') {
      event.preventDefault()
      actions.select(cursorStop.row)
    } else if (event.key === ' ') {
      const { row, ancestors } = cursorStop
      const target = expandable(row) ? row.name : ancestors.at(-1)
      if (!target) return
      const expanding = expandable(row) && !page.expanded.includes(row.name)
      if (expanding && page.expanded.length >= MAX_EXPANDED_RECORDS) return
      event.preventDefault()
      actions.toggle(target)
    }
  }
  // Focus anywhere in the table shows the cursor; arriving at the table reveals
  // it. Native listeners also see the focus a closing drawer returns during a
  // React commit, which React's own focus events miss.
  useEffect(() => {
    const node = table.current
    if (glimpse || !node) return
    const enter = (event: FocusEvent) => {
      setTableFocused(true)
      const { stops: current, cursor: at } = latest.current
      const stop = event.target === node && current.find((item) => item.identity === at)
      const row =
        stop && node.querySelector<HTMLElement>(`[data-record-key="${CSS.escape(stop.row.name)}"]`)
      if (row) Table.revealRow(row)
    }
    const leave = (event: FocusEvent) => {
      if (!(event.relatedTarget instanceof Node && node.contains(event.relatedTarget)))
        setTableFocused(false)
    }
    node.addEventListener('focusin', enter)
    node.addEventListener('focusout', leave)
    return () => {
      node.removeEventListener('focusin', enter)
      node.removeEventListener('focusout', leave)
    }
  }, [glimpse])
  // In the open detail, ↑ and ↓ show the previous and next record in place, but
  // only from the drawer itself: its controls and dialogs keep their keys.
  // ↑ or ↓ shows the previous or next record in the open drawer. A closing
  // drawer steps nowhere. Returns whether the drawer took the key, even at an end
  // or a gap where the detail stays.
  const stepShown = (step: 1 | -1) => {
    if (!page.selected || !drawer.open || !shownStop) return false
    const next = stepStop(stops, shownStop.identity, step)
    if (next) {
      setSteppedTo(next.row.name)
      void navigate(nav.recordHref(next.row.name), { replace: true, preventScrollReset: true })
    }
    return true
  }
  // The drawer itself takes ↑ and ↓ to step the detail, and Space to expand or
  // collapse the Task or Run it shows, as the table would; the detail stays open
  // on that record. Space has no meaning for a record without children of its own.
  const stepDetail = (event: React.KeyboardEvent<HTMLDivElement>) => {
    if (!ownKey(event)) return
    const step = arrowStep(event.nativeEvent)
    if (step) {
      if (stepShown(step)) event.preventDefault()
      return
    }
    if (event.key !== ' ' || event.repeat || !drawer.open || !shownStop) return
    const { row } = shownStop
    // A final value shows its Run's detail, but it is not the Run: Space leaves it.
    if (shown !== page.selected || row.name !== page.selected || !expandable(row)) return
    const expanding = !page.expanded.includes(row.name)
    if (expanding && page.expanded.length >= MAX_EXPANDED_RECORDS) return
    event.preventDefault()
    actions.toggle(row.name)
  }
  // Each record's detail mounts at its top; the drawer itself stays open.
  const popup = useRef<HTMLDivElement>(null)
  // An arrow pressed on the page background, not on any control, enters
  // Activity on the same press: the open drawer steps, or else the table's
  // cursor moves. The open drawer comes first, so the hidden table never moves.
  const onBackground = (event: KeyboardEvent) => {
    const step = arrowStep(event)
    if (!step || !plainKey(event)) return
    if (event.target !== document.body && event.target !== document.documentElement) return
    // Another dialog, such as a confirmation over the drawer, keeps its keys.
    const dialogs = document.querySelectorAll('[role="dialog"], [role="alertdialog"]')
    if ([...dialogs].some((dialog) => dialog !== popup.current)) return
    // Focus moves only where the key will be taken.
    if (page.selected) {
      if (!drawer.open || !shownStop || !popup.current) return
      popup.current.focus({ preventScroll: true })
      if (stepShown(step)) event.preventDefault()
      return
    }
    if (!stops.length || !table.current) return
    table.current.focus({ preventScroll: true })
    if (moveCursor(step)) event.preventDefault()
  }
  const background = useRef(onBackground)
  useLayoutEffect(() => {
    background.current = onBackground
  })
  useEffect(() => {
    if (glimpse) return
    const listener = (event: KeyboardEvent) => background.current(event)
    window.addEventListener('keydown', listener)
    return () => window.removeEventListener('keydown', listener)
  }, [glimpse])
  const scrollToTop = useCallback((content: HTMLDivElement | null) => {
    if (content) popup.current?.scrollTo?.({ top: 0 })
  }, [])
  // The keyboard cursor rests in the table, so closing returns focus there. Base
  // UI would pass it on to the first link in a table that does not scroll.
  const returnFocus = () => {
    if (!table.current) return true
    table.current.focus({ preventScroll: true })
    revealStop(cursorStop)
    return false
  }
  // A detail the comment guard let go leaves with its navigation, while the
  // guard's question still holds focus; that question then closes onto nothing.
  // Once no other dialog remains, focus left on the page returns to the table.
  const hadDetail = useRef(Boolean(page.selected))
  useEffect(() => {
    const had = hadDetail.current
    hadDetail.current = Boolean(page.selected)
    const node = table.current
    if (!had || page.selected || glimpse || !node) return
    const settle = () => {
      if (document.querySelector('[role="dialog"], [role="alertdialog"]')) return false
      if (document.activeElement === document.body || !document.activeElement)
        node.focus({ preventScroll: true })
      return true
    }
    if (settle()) return
    const watch = new MutationObserver(() => {
      if (settle()) watch.disconnect()
    })
    watch.observe(document.body, { childList: true, subtree: true })
    return () => watch.disconnect()
  }, [page.selected, glimpse])
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
          <div className={styles.list}>
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
              ref={table}
              className={styles.table}
              tabIndex={glimpse ? undefined : 0}
              onKeyDown={tableKey}
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
                <CursorContext value={cursorStore}>{tableRows}</CursorContext>
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
                        <Link to={nav.recordHref(outside.name)} preventScrollReset>
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
          {/* One drawer shows each record in turn, so stepping does not reopen it. */}
          <Dialog.Root key={page.selected ? 'open' : 'closed'} {...closeDrawer}>
            <Dialog.Portal>
              <Dialog.Backdrop />
              <Dialog.Popup
                ref={popup}
                size="xl"
                placement="right"
                fullWidthOnMobile
                onKeyDown={stepDetail}
                // The drawer takes focus itself, so its arrows step at once and a held
                // Enter does not close it. Shift+Tab from its first control returns here.
                tabIndex={0}
                initialFocus={popup}
                finalFocus={returnFocus}
              >
                <Dialog.Header>
                  <Dialog.Title>
                    {(page.detail?.kind === 'invocation' &&
                      page.detail.request?.kind === 'approval') ||
                    (page.detail?.kind === 'run' &&
                      page.detail.request?.kind === 'account_connection')
                      ? 'Review action'
                      : 'Activity details'}
                  </Dialog.Title>
                  <Dialog.Close ariaLabel="Close details" />
                </Dialog.Header>
                {page.unavailable ? (
                  <Typography.Body>This history is unavailable.</Typography.Body>
                ) : (
                  <div key={shown} ref={scrollToTop} className={styles.detail}>
                    <ActivityDetail page={page} />
                  </div>
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
          </Dialog.Root>
        </div>
      </ActivityConnectedContext>
    </RowActionsContext>
  )
}
