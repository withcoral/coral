import { createContext, useContext } from 'react'
import { useLocation, useSearchParams } from 'react-router'
import {
  ACTIVITY_CHILD_WINDOW,
  activityRowIdentity,
  readingWindow,
  type ActivityViewport,
} from '@/lib/activity-live-types'
import type { ActivityBranch, ActivityPage, ActivityRow, LiveAction } from './types'

export const ActivityNavigationContext = createContext<ReturnType<
  typeof activityNavigation
> | null>(null)

/** The expanded record a query key positions (`offset.<record>` and the like), if any. */
export function branchCoordinate(key: string): string | undefined {
  return /^(?:cursor|anchor|offset|preceding)\.(.+)$/.exec(key)?.[1]
}

/**
 * Query keys that only select a record and its review, never the listed window.
 * `live=1` follows the newest record of the selected record's Run; any other
 * selection turns it off.
 */
export const selectionKeys = ['record', 'request', 'actionsAfter', 'live'] as const
const clearSelection = Object.fromEntries(selectionKeys.map((key) => [key, null]))

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
      ...(stopLive && { live: null }),
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
  /** Live mode follows the newest record of the selected record's Run. */
  const live = query.get('live') === '1'
  /** Turns live mode on or off for the shown record. On, it expands `expand` too. */
  const liveHref = (on: boolean, expand: string[] = []) => {
    const open = context.getAll('expand')
    return href({
      live: on ? '1' : null,
      ...(expand.some((name) => !open.includes(name)) && {
        tree: '1',
        expand: [...new Set([...open, ...expand])],
      }),
    })
  }
  /**
   * Live mode's selection of `name` in the Run it follows: its newest record, or the
   * record that reviews `action`. An action the Run's window does not hold opens at
   * that action. After a decision the review advances the same way, and `on` keeps
   * live mode as it was.
   */
  const followHref = (name: string, action?: LiveAction, on = true) => {
    const held = page.branches[action?.run ?? '']?.rows.some((row) => row.name === name)
    const reveal = action && name !== action.run && !held
    return href({
      ...clearSelection,
      record: name,
      request: action?.request ?? null,
      live: on ? '1' : null,
      ...(reveal && {
        [`anchor.${action.run}`]: name,
        [`offset.${action.run}`]: null,
        [`preceding.${action.run}`]: null,
        [`cursor.${action.run}`]: null,
      }),
    })
  }
  /**
   * Live mode's read of the last children of `run`, which has `total` of them. An
   * offset at the end reads the last window. The selection stays until it loads.
   * An advance after a decision reads it with live mode as it was, which `on` keeps.
   */
  const followEndHref = (run: string, total: number, on = true) =>
    href({
      live: on ? '1' : null,
      [`offset.${run}`]: String(total),
      [`preceding.${run}`]: '0',
      [`anchor.${run}`]: null,
      [`cursor.${run}`]: null,
    })
  return {
    query,
    href,
    recordHref,
    toggleHref,
    collapseHref,
    revealHref,
    backHref,
    live,
    liveHref,
    followHref,
    followEndHref,
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
      : row?.kind === 'invocation' || row?.kind === 'output' || row?.kind === 'middleware'
        ? row.run
        : null
  if (!row || !run) return null
  const task = 'task' in row ? (row.task ?? '') : ''
  return { run, chain: task ? [task, run] : [run] }
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

/** Where the keys start from when they rest on `stop`. */
export const stopPosition = (stop: RecordStop): StopPosition => ({
  name: stop.row.name,
  parents: stop.parents,
})

/** The Tasks and Runs that enclose a record, nearest first. `shown` names a final value. */
export function recordParents(row: ActivityRow, shown = row.name): string[] {
  const own = shown === finalValue(row.name) ? [row.name] : []
  const run =
    row.kind === 'invocation' || row.kind === 'output' || row.kind === 'middleware' ? row.run : ''
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
