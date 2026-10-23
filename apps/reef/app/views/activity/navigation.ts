import { createContext, useContext } from 'react'
import { useLocation, useSearchParams } from 'react-router'
import {
  ACTIVITY_CHILD_WINDOW,
  activityRowIdentity,
  readingWindow,
  type ActivityViewport,
} from '@/lib/activity-live-types'
import {
  liveScopeOf,
  liveValue,
  MAX_EXPANDED_RECORDS,
  type ActivityBranch,
  type ActivityPage,
  type ActivityRow,
  type LiveAction,
  type LiveScope,
} from './types'

export const ActivityNavigationContext = createContext<ReturnType<
  typeof activityNavigation
> | null>(null)

/** The expanded record a query key positions (`offset.<record>` and the like), if any. */
export function branchCoordinate(key: string): string | undefined {
  return /^(?:cursor|anchor|offset|preceding)\.(.+)$/.exec(key)?.[1]
}

/**
 * Query keys that only select a record and its review, never the listed window.
 * `live` names what live mode follows (`liveScopeOf`); any other selection turns it
 * off. `liveExpanded` names the records live mode expanded itself, which it may
 * collapse again. Both live in the address, so a reload or Back keeps them, and each
 * step's link and the expansion limit come from the address alone.
 */
export const selectionKeys = ['record', 'request', 'actionsAfter', 'live', 'liveExpanded'] as const
const clearSelection = Object.fromEntries(selectionKeys.map((key) => [key, null]))
const listKeys = new Set<string>([
  ...selectionKeys,
  'tree',
  'expand',
  'anchor',
  'before',
  'after',
  'pendingAfter',
  'root',
  'rootMode',
])

/** Whether the query selects a record or positions the list, which only a complete list can show. */
export function readsActivity(query: URLSearchParams) {
  return [...query.keys()].some((key) => listKeys.has(key) || branchCoordinate(key) !== undefined)
}

/** Address changes that read the children of `parent` from `offset`, with no anchor. */
export const windowAt = (parent: string, offset: number) => ({
  [`offset.${parent}`]: String(offset),
  [`preceding.${parent}`]: '0',
  [`anchor.${parent}`]: null,
  [`cursor.${parent}`]: null,
})

/** Reveal the record's authorized history; leaving review clears only its selection. */
export function revealHref(row: ActivityRow, options: { select?: boolean; request?: string } = {}) {
  const task = row.kind === 'task' ? row.name : 'task' in row ? (row.task ?? '') : ''
  const run = row.kind === 'run' ? row.name : 'run' in row ? row.run : ''
  const next = new URLSearchParams({
    anchor: task || run || row.name,
    record: row.name,
    tree: '1',
  })
  for (const name of [task, run]) if (name) next.append('expand', name)
  if (task && task !== row.name) next.set(`anchor.${task}`, run || row.name)
  if (run && run !== row.name) next.set(`anchor.${run}`, row.name)
  const path = row.agent ? `/${row.agent}` : `/${row.workspace}/activity`
  if (options.select === false) next.delete('record')
  else if (options.request) next.set('request', options.request)
  return `${path}?${next}`
}

/** Pin server-resolved review identity and its exact child window without another read. */
export function activitySelection(page: ActivityPage, search: string): URLSearchParams {
  const query = new URLSearchParams(search)
  if (!query.has('request') && page.initialRequest) query.set('request', page.initialRequest)
  if (page.initialRecord) {
    query.set('record', page.initialRecord)
    if (page.detail?.kind === 'invocation') {
      const run = page.detail.invocation.run
      query.set(`anchor.${run}`, page.initialRecord)
      for (const prefix of ['cursor', 'offset', 'preceding']) query.delete(`${prefix}.${run}`)
    }
  }
  return query
}

/** Where the reader is in the list when it selects or opens a record. */
export interface ActivityReading {
  /** The first top-level record on screen, or null at the newest records. */
  anchor: string | null
  branches: Record<string, ActivityViewport>
}

/** Identity-only navigation coordinates are reauthorized by the destination loader. */
export function activityNavigation(
  page: ActivityPage,
  pathname: string,
  search: string,
  reading: ActivityReading = {
    anchor: new URLSearchParams(search).get('anchor'),
    branches: readingWindow(page).branches,
  },
) {
  const query = new URLSearchParams(search)
  const context = activitySelection(page, search)
  if (!context.has('tree')) {
    context.set('tree', '1')
    context.delete('expand')
    for (const name of page.expanded) context.append('expand', name)
    for (const [parent, record] of Object.entries(page.anchors ?? {})) {
      if (!context.has(`anchor.${parent}`)) context.set(`anchor.${parent}`, record)
    }
  }
  // Pages are no longer links. The reading anchor below names the position instead.
  for (const key of ['before', 'after', 'rootMode', 'root']) context.delete(key)
  // Carry the requested viewport, not just the first row of a possibly shifted page.
  // No rows, totals, selectors, or exact evidence cross this navigation seam.
  for (const parent of page.expanded) {
    const viewport = reading.branches[parent]
    if (!viewport) continue
    const branch = page.branches[parent]
    const first = branch?.rows.find((row) => row.position === viewport.offset)
    // Buffering is not navigation away from an explicitly revealed Event. Keep its
    // identity while it remains in this bounded window (including terminal Events).
    const explicit = context.get(`anchor.${parent}`) ?? page.anchors?.[parent]
    const retained = branch?.rows.find(
      (row) => row.name === explicit || activityRowIdentity(row) === explicit,
    )
    const distance = retained?.position === undefined ? -1 : retained.position - viewport.offset
    const keepAnchor = explicit && distance >= 0 && distance < ACTIVITY_CHILD_WINDOW
    const anchor = keepAnchor
      ? activityRowIdentity(retained!)
      : viewport.anchor || (first ? activityRowIdentity(first) : '')
    context.set(`offset.${parent}`, String(viewport.offset))
    // A replacement first-row anchor already denotes the requested offset.
    context.set(
      `preceding.${parent}`,
      String(keepAnchor ? distance : viewport.anchor ? viewport.preceding : 0),
    )
    if (anchor) context.set(`anchor.${parent}`, anchor)
    else context.delete(`anchor.${parent}`)
    context.delete(`cursor.${parent}`)
  }
  const href = (changes: Record<string, string | string[] | null>, path = page.listPath) => {
    const next = new URLSearchParams(context)
    // Selection and expansion keep the reading position, so a reload opens there.
    // Explicit root navigation names its own anchor; pending-action pages keep the URL's.
    if (
      !['before', 'after', 'anchor', 'pendingAfter'].some((key) => key in changes) &&
      path === page.listPath
    ) {
      if (reading.anchor) next.set('anchor', reading.anchor)
      else next.delete('anchor')
    }
    // Stable Run routes carry their selection in the path, not a competing query value.
    if (pathname !== page.listPath && page.selected) next.set('record', page.selected)
    for (const [key, value] of Object.entries(changes)) {
      next.delete(key)
      for (const item of Array.isArray(value) ? value : value ? [value] : []) next.append(key, item)
    }
    const expanded = new Set(next.getAll('expand'))
    const keys = [...next.keys()]
    for (const key of keys) {
      const parent = branchCoordinate(key)
      if (parent && !expanded.has(parent)) next.delete(key)
    }
    return `${path}?${next}`
  }
  const recordHref = (name: string) => href({ ...clearSelection, record: name })
  /** `stopLive` also turns live mode off, for a collapse that hides the Run it follows. */
  const toggleHref = (name: string, stopLive = false) =>
    href({
      ...(stopLive && { live: null, liveExpanded: null }),
      tree: '1',
      expand: page.expanded.includes(name)
        ? page.expanded.filter((item) => item !== name)
        : [...page.expanded, name],
    })
  /** Collapses `name` and selects it, for a collapse that hides the selected record. */
  const collapseHref = (name: string) =>
    href({
      ...clearSelection,
      record: name,
      tree: '1',
      expand: page.expanded.filter((item) => item !== name),
    })
  const backHref = href(clearSelection)
  /** What live mode follows, or null while it is off. */
  const liveScope = liveScopeOf(query.get('live'))
  const owned = query.getAll('liveExpanded')
  /**
   * Live mode in `scope`, with `chain` expanded and `keep` still open. It collapses the
   * records it expanded itself that neither names, and never one you expanded. Off, it
   * collapses nothing, and a decision's advance still expands `chain`, as yours.
   */
  const liveChanges = (scope: LiveScope | null, chain: string[], keep: string[]) => {
    const open = context.getAll('expand')
    const kept = new Set([...chain, ...keep])
    const stays = (name: string) => !scope || kept.has(name) || !owned.includes(name)
    const expand = [...new Set([...open.filter(stays), ...chain])]
    const nextOwned = scope
      ? expand.filter((name) => kept.has(name) && (owned.includes(name) || !open.includes(name)))
      : null
    const changed = expand.length !== open.length || expand.some((name) => !open.includes(name))
    return {
      live: scope && liveValue(scope),
      liveExpanded: nextOwned,
      ...(changed && { tree: '1', expand }),
    }
  }
  /** Whether `chain` can expand while `keep` stays open, within the expansion limit. */
  const liveRoom = (chain: string[], keep: string[] = chain) =>
    (liveChanges('run', chain, keep).expand ?? context.getAll('expand')).length <=
    MAX_EXPANDED_RECORDS
  /**
   * Turns live mode on in `scope`, or off in place with null. On, it expands `chain`
   * and keeps `keep` open. `changes` also changes the address, such as a window.
   */
  const liveHref = (
    scope: LiveScope | null,
    chain: string[] = [],
    keep: string[] = chain,
    changes: Record<string, string | null> = {},
  ) => href({ ...changes, ...liveChanges(scope, chain, keep) })
  /**
   * Live mode's selection of `name`, which `chain` leads to: its newest record, or the
   * record that reviews `action`. An action the Run's window does not hold opens at
   * that action. After a decision the review advances the same way, and `scope`
   * keeps live mode as it was. Without `chain`, it keeps what live mode expanded.
   */
  const followHref = (
    name: string,
    action?: LiveAction,
    scope: LiveScope | null = liveScope,
    chain?: string[],
  ) => {
    const held = page.branches[action?.run ?? '']?.rows.some((row) => row.name === name)
    const reveal = action && name !== action.run && !held
    return href({
      ...clearSelection,
      record: name,
      request: action?.request ?? null,
      ...liveChanges(scope, chain ?? [], chain ?? owned),
      ...(reveal && {
        [`anchor.${action.run}`]: name,
        [`offset.${action.run}`]: null,
        [`preceding.${action.run}`]: null,
        [`cursor.${action.run}`]: null,
      }),
    })
  }
  /**
   * Live mode's read of the children of `parent` from `offset`. An offset at the end
   * reads the last window. The selection stays until it loads. An advance after a
   * decision reads it with live mode as it was, which `scope` keeps. Without `chain`,
   * it keeps what live mode expanded.
   */
  const followWindowHref = (
    parent: string,
    offset: number,
    scope: LiveScope | null = liveScope,
    chain?: string[],
  ) => href({ ...liveChanges(scope, chain ?? [], chain ?? owned), ...windowAt(parent, offset) })
  return {
    query,
    href,
    recordHref,
    toggleHref,
    collapseHref,
    revealHref,
    backHref,
    liveScope,
    liveHref,
    liveRoom,
    followHref,
    followWindowHref,
  }
}

export function useActivityNavigation(page: ActivityPage) {
  const reader = useContext(ActivityNavigationContext)
  const [query] = useSearchParams()
  const { pathname } = useLocation()
  return reader ?? activityNavigation(page, pathname, query.toString())
}

/** A branch's loaded children at their ordinals, and how many ordinals it has. */
export function childWindow(branch: ActivityBranch) {
  const items = branch.rows.map((row, index) => ({
    row,
    index: row.position ?? (branch.offset ?? 0) + index,
  }))
  return {
    items,
    total: Math.max(
      Number(branch.total ?? branch.rows.length),
      ...items.map(({ index }) => index + 1),
    ),
  }
}

/**
 * The Run live mode follows for a record: the Run itself, or the Run that made it.
 * `chain` names the records that must be expanded for its children to load: its
 * Task, if any, then the Run.
 */
export function liveRun(row: ActivityRow | null): { run: string; chain: string[] } | null {
  const run =
    row?.kind === 'run'
      ? row.name
      : row?.kind === 'invocation' || row?.kind === 'output'
        ? row.run
        : null
  if (!row || !run) return null
  const task = 'task' in row ? (row.task ?? '') : ''
  return { run, chain: task ? [task, run] : [run] }
}

/** What live mode follows next in its scope, before the followed Run's own step. */
export type LiveTarget =
  /** Follow `run`; `liveStep` picks its record. `chain` must be expanded. */
  | { kind: 'run'; run: string; chain: string[] }
  /** Show `record`, a Task with no Run yet. */
  | { kind: 'select'; record: string; chain: string[] }
  /** Expand `chain`, so its children load. */
  | { kind: 'expand'; chain: string[] }
  /** Read the children of `parent` from `offset`. An offset at the end reads the last window. */
  | { kind: 'load'; parent: string; offset: number; chain: string[] }
  /** Read the newest top-level records, which the list does not hold. */
  | { kind: 'newest' }
  | { kind: 'wait' }

/** How long after it starts page live mode still opens a Task, in milliseconds. */
export const TAKS_MAX_AGE = 60 * 60 * 1000

/**
 * The target of live mode in `scope`. It never moves off the Task or Run you have open:
 * a Run stays followed through its records and its end, whatever arrives.
 * - `run`: the open record's Run.
 * - `latest`: the open record's Run, else the open Task's latest Run that has not
 *   ended. With nothing open, the newest top-level record in Coral's order that has not
 *   ended: a standalone Run itself, a Task through its latest Run that has not ended,
 *   or a Task that has not ended while it has none.
 *   Of those, it passes a Task that started more than `TAKS_MAX_AGE` before `now`.
 * - `task`: the open record's Run, else the open Task's first Run by submission that
 *   has not ended, else its last Run.
 */
export function liveTarget(page: ActivityPage, scope: LiveScope, now = Date.now()): LiveTarget {
  const row = page.selectedRow
  const current = liveRun(row)
  if (current) return { kind: 'run', ...current }
  const task = row?.kind === 'task' ? row.name : row && 'task' in row ? (row.task ?? '') : ''
  if (scope === 'task' && task) return taskTarget(page, task)
  if (scope === 'latest' && task) {
    // The open Task can open its own Run, and nothing else.
    const root = page.rows.find((item) => item.name === task)
    const target = root && rootTarget(page, root)
    return target && target.kind !== 'select' ? target : { kind: 'wait' }
  }
  if (scope !== 'latest' || row) return { kind: 'wait' }
  if (page.hasBefore) return { kind: 'newest' }
  for (const root of page.rows) {
    if (root.kind === 'task' && now - Date.parse(root.startedAt) > TAKS_MAX_AGE) continue
    const target = rootTarget(page, root)
    if (target) return target
  }
  return { kind: 'wait' }
}

type RunRow = Extract<ActivityRow, { kind: 'run' }>

/** What `latest` follows in `root`, or null when its work has ended. */
function rootTarget(page: ActivityPage, root: ActivityRow): LiveTarget | null {
  if (root.kind === 'run')
    return root.state === 'running' ? { kind: 'run', run: root.name, chain: [root.name] } : null
  if (root.kind !== 'task') return null
  // An ended Task can still hold a Run that has not ended; `contextImpactPartial` says so.
  if (root.endedAt && !root.contextImpactPartial) return null
  const task = root.name
  const loaded = taskRuns(page, task)
  if (loaded.kind !== 'runs') return loaded
  // The latest Run may be past the window.
  if (loaded.end < loaded.total)
    return { kind: 'load', parent: task, offset: loaded.total, chain: [task] }
  const latest = loaded.runs.findLast((row) => row.state === 'running')
  if (latest) return { kind: 'run', run: latest.name, chain: [task, latest.name] }
  // An ended Task's Run that runs is past the loaded Runs. Passing it would collapse
  // the Task, then expand it again on the next step.
  return root.endedAt ? { kind: 'wait' } : { kind: 'select', record: task, chain: [task] }
}

/**
 * The loaded Runs of `task` by submission, where its window ends, and how many children
 * it has. Without them, the step that reads them.
 */
function taskRuns(
  page: ActivityPage,
  task: string,
): LiveTarget | { kind: 'runs'; runs: RunRow[]; end: number; total: number } {
  if (!page.expanded.includes(task)) return { kind: 'expand', chain: [task] }
  const branch = page.branches[task]
  if (!branch || branch.unavailable) return { kind: 'wait' }
  const { items, total } = childWindow(branch)
  const runs = items
    .toSorted((a, b) => a.index - b.index)
    .flatMap(({ row }) => (row.kind === 'run' ? [row] : []))
  const end = Math.max(branch.offset ?? 0, ...items.map(({ index }) => index + 1))
  return { kind: 'runs', runs, end, total }
}

/**
 * The next Run of `task` by submission that has not ended, else its last Run, else the
 * Task. Runs never start again once they end, so the next one never lies before the
 * loaded window.
 */
function taskTarget(page: ActivityPage, task: string): LiveTarget {
  const chain = [task]
  const loaded = taskRuns(page, task)
  if (loaded.kind !== 'runs') return loaded
  const next = loaded.runs.find((row) => row.state === 'running')
  if (next) return { kind: 'run', run: next.name, chain: [task, next.name] }
  // Past the window, the next one may wait.
  if (loaded.end < loaded.total) return { kind: 'load', parent: task, offset: loaded.end, chain }
  const last = loaded.runs.at(-1)?.name
  return last
    ? { kind: 'run', run: last, chain: [task, last] }
    : { kind: 'select', record: task, chain }
}

/** What live mode does next for a Run. */
export type LiveStep =
  /**
   * Select the record that reviews the Run's oldest action, with its `request`.
   * Without one, the Run's newest record, or the Run itself while it has none.
   */
  | { kind: 'select'; record: string; request?: string }
  /** The newest child is past the loaded window; read the Run's last `total` children. */
  | { kind: 'load'; total: number }
  /** Expand the Run, so its children load. */
  | { kind: 'expand' }
  /** The Run's children are not readable now. */
  | { kind: 'wait' }

/**
 * The oldest action you can take in `run`, as the loader names it, wins over newer
 * records. `actionable` is false where you cannot act, such as in an archived workspace.
 * Otherwise, the newest record of `run` on `page`. Children keep the order Coral gives
 * them: the highest position is the newest. Only this Run's own branch counts.
 */
export function liveStep(page: ActivityPage, run: string, actionable = true): LiveStep {
  const action = actionable && page.liveAction?.run === run ? page.liveAction : undefined
  if (action) {
    const row = page.branches[run]?.rows.find((item) => item.name === action.record)
    // A live update ended the wait first. The next read names the next action, if any.
    if (row?.kind === 'invocation' && !row.requiresAction) return { kind: 'wait' }
    return { kind: 'select', record: action.record, request: action.request }
  }
  if (!page.expanded.includes(run)) return { kind: 'expand' }
  const branch = page.branches[run]
  if (!branch || branch.unavailable) return { kind: 'wait' }
  const { items, total } = childWindow(branch)
  if (!total) return { kind: 'select', record: run }
  // Positions are unique. Were two equal, the later in Coral's order wins.
  const newest = items.reduce<(typeof items)[number] | undefined>(
    (last, item) => (!last || item.index >= last.index ? item : last),
    undefined,
  )
  if (!newest || newest.index + 1 < total) return { kind: 'load', total }
  return { kind: 'select', record: newest.row.name }
}

/** Where ↑ and ↓ rest: a record the table shows, with its enclosing Tasks and Runs. */
export interface RecordStop {
  kind: 'record'
  row: ActivityRow
  /** Enclosing records, nearest first. */
  parents: string[]
  /** It shows its children below it. */
  expanded: boolean
}
/** In table order, the records ↑ and ↓ reach and the boundaries they stop at. */
export type ActivityStop = RecordStop | { kind: 'boundary' }
/** A record the keys start from, which the table may not show. */
export interface StopPosition {
  name: string
  parents: string[]
}

/** A Run's final value is its own record, although its Run's detail shows it. */
export const finalValue = (run: string) => `${run}/final`

/** The record a selection shows: its Run's final value when the URL's `record` names it. */
export const shownRecordOf = (selected: string | null, record: string | null) =>
  selected && record === finalValue(selected) ? record : selected

/** Where the keys start from when they rest on `stop`. */
export const stopPosition = (stop: RecordStop): StopPosition => ({
  name: stop.row.name,
  parents: stop.parents,
})

/** The Tasks and Runs that enclose a record, nearest first. `shown` names a final value. */
export function recordParents(row: ActivityRow, shown = row.name): string[] {
  const own = shown === finalValue(row.name) ? [row.name] : []
  const run = row.kind === 'invocation' || row.kind === 'output' ? row.run : ''
  const task = row.kind === 'task' ? '' : (row.task ?? '')
  return [...own, run, task].filter(Boolean)
}

/**
 * The displayed record tree in order. An unloaded run of child ordinals and an
 * unavailable history are boundaries, so the keys never skip records.
 */
export function activityStops(page: ActivityPage, roots: ActivityRow[]): ActivityStop[] {
  const stops: ActivityStop[] = []
  const visit = (rows: ActivityRow[], parents: string[]) => {
    for (const row of rows) {
      const expandable = row.kind === 'task' || row.kind === 'run'
      const expanded = expandable && page.expanded.includes(row.name)
      stops.push({ kind: 'record', row, parents, expanded })
      if (!expanded) continue
      const branch = page.branches[row.name]
      if (!branch || branch.unavailable) {
        stops.push({ kind: 'boundary' })
        continue
      }
      const { items, total } = childWindow(branch)
      let next = 0
      for (const item of items.toSorted((x, y) => x.index - y.index)) {
        if (item.index > next) stops.push({ kind: 'boundary' })
        visit([item.row], [row.name, ...parents])
        next = item.index + 1
      }
      if (total > next) stops.push({ kind: 'boundary' })
    }
  }
  visit(roots, [])
  return stops
}

/**
 * Where a position rests: its own record, or the nearest collapsed record that
 * hides it. An expanded record whose window lacks it never stands in. -1 for none.
 */
export function resolveStop(stops: ActivityStop[], position: StopPosition): number {
  const at = (name: string) =>
    stops.findIndex(
      (stop) =>
        stop.kind === 'record' &&
        (stop.row.name === name || activityRowIdentity(stop.row) === name),
    )
  const own = at(position.name)
  if (own >= 0) return own
  for (const parent of position.parents) {
    const index = at(parent)
    if (index < 0) continue
    return (stops[index] as RecordStop).expanded ? -1 : index
  }
  return -1
}

/**
 * The record one step from a position. Without a position, ↓ starts at the first
 * record and ↑ at the last. It stops at either end and at every boundary.
 */
export function stepStop(
  stops: ActivityStop[],
  position: StopPosition | null,
  step: 1 | -1,
): RecordStop | null {
  if (!position) {
    const ends = step > 0 ? stops : stops.toReversed()
    return ends.find((stop): stop is RecordStop => stop.kind === 'record') ?? null
  }
  const from = resolveStop(stops, position)
  const next = from < 0 ? undefined : stops[from + step]
  return next?.kind === 'record' ? next : null
}

/**
 * The position among its Run's records, as Coral ordered them, of a record inside a
 * Run: an Operation Invocation or any Program Run Event, its lifecycle included.
 * Undefined for any other record, such as the Run itself or its final value, or when its Run's
 * loaded window does not hold it.
 */
export function orderedRecordPosition(page: ActivityPage, row: ActivityRow | null) {
  if (row?.kind !== 'invocation' && row?.kind !== 'output') return undefined
  const branch = page.branches[row.run]
  if (!branch || branch.unavailable) return undefined
  return branch.rows.find((item) => item.name === row.name)?.position
}
