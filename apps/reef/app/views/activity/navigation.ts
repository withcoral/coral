import { createContext, useContext } from 'react'
import { useLocation, useSearchParams } from 'react-router'
import {
  ACTIVITY_CHILD_WINDOW,
  activityRowIdentity,
  readingWindow,
  type ActivityViewport,
} from '@/lib/activity-live-types'
import type { ActivityPage, ActivityRow } from './types'

export const ActivityNavigationContext = createContext<ReturnType<
  typeof activityNavigation
> | null>(null)

/** The expanded record a query key positions (`offset.<record>` and the like), if any. */
export function branchCoordinate(key: string): string | undefined {
  return /^(?:cursor|anchor|offset|preceding)\.(.+)$/.exec(key)?.[1]
}

/** Query keys that only select a record and its review, never the listed window. */
export const selectionKeys = ['record', 'request', 'actionsAfter'] as const
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
  const toggleHref = (name: string) =>
    href({
      tree: '1',
      expand: page.expanded.includes(name)
        ? page.expanded.filter((item) => item !== name)
        : [...page.expanded, name],
    })
  const backHref = href(clearSelection)
  return {
    query,
    href,
    recordHref,
    toggleHref,
    revealHref,
    backHref,
  }
}

export function useActivityNavigation(page: ActivityPage) {
  const reader = useContext(ActivityNavigationContext)
  const [query] = useSearchParams()
  const { pathname } = useLocation()
  return reader ?? activityNavigation(page, pathname, query.toString())
}

/** Only a Task or Run has children to expand. */
export const expandable = (row: ActivityRow) => row.kind === 'task' || row.kind === 'run'

/** A record the keyboard cursor reaches, in the order the table shows it. */
export interface ActivityStop {
  row: ActivityRow
  /** Child-window identity: it survives a submission becoming its accepted Run. */
  identity: string
  /** The records whose expanded branches hold it, outermost first. */
  ancestors: string[]
  /** Unloaded records sit between it and the stop before it. */
  gapBefore: boolean
}

/**
 * The records the keyboard cursor reaches, in the order the table shows them:
 * each root, then the loaded children of a Task or Run whose branch the table
 * shows, by ordinal. Headings, status rows, and unloaded ordinals are not stops.
 */
export function activityStops(page: ActivityPage, roots: ActivityRow[]): ActivityStop[] {
  const stops: ActivityStop[] = []
  let gap = false
  const add = (row: ActivityRow, ancestors: string[]) => {
    stops.push({ row, identity: activityRowIdentity(row), ancestors, gapBefore: gap })
    gap = false
    const branch = page.branches[row.name]
    if (!expandable(row)) return
    if (!branch) {
      // An expanded record whose children have not arrived yet hides them.
      if (page.expanded.includes(row.name)) gap = true
      return
    }
    if (branch.unavailable) return
    const children = branch.rows
      .map((child, index) => ({ child, index: child.position ?? (branch.offset ?? 0) + index }))
      .toSorted((left, right) => left.index - right.index)
    let next = 0
    for (const { child, index } of children) {
      if (index > next) gap = true
      add(child, [...ancestors, row.name])
      next = index + 1
    }
    if (Number(branch.total ?? branch.rows.length) > next) gap = true
  }
  for (const row of roots) add(row, [])
  return stops
}

/**
 * The stop a step from `cursor` reaches, or undefined at either end or an
 * unloaded gap. Without a cursor, a step forward starts at the first stop and a
 * step back at the last.
 */
export function stepStop(stops: ActivityStop[], cursor: string | null, step: 1 | -1) {
  const at = cursor === null ? -1 : stops.findIndex((stop) => stop.identity === cursor)
  if (at < 0) return step > 0 ? stops[0] : stops.at(-1)
  const next = stops[at + step]
  return next && !(step > 0 ? next : stops[at]!).gapBefore ? next : undefined
}
