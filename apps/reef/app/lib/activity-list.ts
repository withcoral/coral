import type { ActivityBranch, ActivityPage, ActivityRow } from '@/views/activity/types'

/** Records in each page that Reef asks Coral for. */
export const ACTIVITY_PAGE_SIZE = 25
/** Past this many top-level records, the reader asks for each further page. */
export const MAX_LOADED_RECORDS = 500
/** Coral observes at most this many top-level records in one reading window. */
const MAX_LIVE_RECORDS = 25
/** Records on each side of the viewport that still count as near it. */
const NEAR_VIEWPORT = 5

export type ActivityListDirection = 'older' | 'newer'

/**
 * The top-level records (Tasks and standalone Runs) the reader has loaded, newest
 * first, with the branches of the expanded ones. This module owns how pages and
 * live snapshots join the list. The table renders `rows` without knowing how they
 * arrived, so a later backend can replace only this data source.
 */
export interface ActivityList {
  rows: ActivityRow[]
  branches: Record<string, ActivityBranch>
  /** Coral has newer records above the first loaded one. */
  hasNewer: boolean
  /** Coral has older records below the last loaded one. */
  hasOlder: boolean
  /** The anchor the list opened at is gone. */
  anchorUnavailable: boolean
  /** The page in flight, and the record it continues from. */
  loading: { direction: ActivityListDirection; from: string } | null
  failed: ActivityListDirection | null
}

export function openActivityList(page: ActivityPage): ActivityList {
  return {
    rows: page.rows,
    branches: page.branches,
    hasNewer: page.hasBefore,
    hasOlder: page.hasAfter,
    anchorUnavailable: page.anchorUnavailable,
    loading: null,
    failed: null,
  }
}

/** The loaded record at `direction`'s end, when Coral has more beyond it. */
const edge = (list: ActivityList, direction: ActivityListDirection) =>
  direction === 'older'
    ? list.hasOlder
      ? list.rows.at(-1)?.name
      : undefined
    : list.hasNewer
      ? list.rows[0]?.name
      : undefined
/** The record the next page continues from, unless a page is already loading. */
const nextFrom = (list: ActivityList, direction: ActivityListDirection) =>
  list.loading ? undefined : edge(list, direction)

/** The Activity loader's query for the next page, or null when there is none to load. */
export function pageQuery(
  list: ActivityList,
  direction: ActivityListDirection,
): { before: string } | { after: string } | null {
  const from = nextFrom(list, direction)
  if (!from) return null
  return direction === 'older' ? { after: from } : { before: from }
}

export function startPage(list: ActivityList, direction: ActivityListDirection): ActivityList {
  const from = nextFrom(list, direction)
  return from ? { ...list, loading: { direction, from }, failed: null } : list
}

/** Loaded rows, each replaced by its fresh read in `page` when the page has one. */
const refreshed = (rows: ActivityRow[], page: ActivityPage) => {
  const fresh = new Map(page.rows.map((row) => [row.name, row]))
  return rows.map((row) => fresh.get(row.name) ?? row)
}

export function failPage(list: ActivityList, direction: ActivityListDirection): ActivityList {
  if (list.loading?.direction !== direction) return list
  return { ...list, loading: null, failed: direction }
}

/** Joins a loaded page to the end it continues. A page for an earlier list is ignored. */
export function receivePage(
  list: ActivityList,
  direction: ActivityListDirection,
  page: ActivityPage,
): ActivityList {
  if (!list.loading || list.loading.direction !== direction) return list
  if (list.loading.from !== edge(list, direction)) return list
  if (page.listUnavailable) return failPage(list, direction)
  const loaded = new Set(list.rows.map((row) => row.name))
  const kept = refreshed(list.rows, page)
  const added = page.rows.filter((row) => !loaded.has(row.name))
  return {
    ...list,
    rows: direction === 'older' ? [...kept, ...added] : [...added, ...kept],
    branches: { ...list.branches, ...page.branches },
    hasOlder: direction === 'older' ? page.hasAfter : list.hasOlder,
    hasNewer: direction === 'newer' ? page.hasBefore : list.hasNewer,
    loading: null,
    failed: null,
  }
}

/**
 * Joins a fresh read of loaded records: a live snapshot or a loader page.
 *
 * A `retained` read names only the records it watched, so it replaces them in place
 * and adds nothing. Any other read is a contiguous page: records it has between,
 * above or below loaded ones join next to them. A page that shares no record with
 * the list joins only as arrivals at the newest edge; otherwise this returns null,
 * because the page belongs to another part of the history.
 *
 * A merge never removes or reorders loaded records.
 */
export function mergeActivityPage(
  list: ActivityList,
  page: ActivityPage,
  { retained }: { retained: boolean },
): ActivityList | null {
  let rows = refreshed(list.rows, page)
  let { hasNewer, hasOlder } = list
  if (!retained && page.rows.length) {
    const loaded = new Set(list.rows.map((row) => row.name))
    const shared = page.rows.filter((row) => loaded.has(row.name))
    if (!shared.length) {
      if (page.hasBefore || list.hasNewer) return null
      rows = [...page.rows, ...rows]
    } else {
      // Each new record goes next to its nearest loaded neighbour in the page.
      const before = new Map<string, ActivityRow[]>()
      const after: ActivityRow[] = []
      let pending: ActivityRow[] = []
      for (const row of page.rows) {
        if (!loaded.has(row.name)) pending.push(row)
        else {
          if (pending.length) before.set(row.name, pending)
          pending = []
        }
      }
      after.push(...pending)
      const last = shared.at(-1)!.name
      rows = rows.flatMap((row) => [
        ...(before.get(row.name) ?? []),
        row,
        ...(row.name === last ? after : []),
      ])
    }
    if (rows[0]?.name === page.rows[0]!.name) hasNewer = page.hasBefore
    if (rows.at(-1)?.name === page.rows.at(-1)!.name) hasOlder = page.hasAfter
  }
  const expanded = new Set(page.expanded)
  const branches = Object.fromEntries(
    Object.entries({ ...list.branches, ...page.branches }).filter(([name]) => expanded.has(name)),
  )
  return { ...list, rows, branches, hasNewer, hasOlder }
}

/** Whether reaching an end of the list loads the next page without a click. */
export function loadsAutomatically(list: ActivityList) {
  return list.rows.length < MAX_LOADED_RECORDS && !list.failed
}

// Activity reads time in UTC, as its column heading says. An ISO instant splits
// into the UTC day without a time zone lookup.
export const utcDay = (instant: string) => instant.slice(0, 10)

export interface ActivityDay {
  day: string
  key: string
  rows: ActivityRow[]
}
/** Top-level records under the UTC day they started. One day is one group across pages. */
export function activityDays(rows: ActivityRow[]): ActivityDay[] {
  const days: ActivityDay[] = []
  for (const row of rows) {
    const day = utcDay(row.startedAt)
    const last = days.at(-1)
    if (last?.day === day) last.rows.push(row)
    // A day keeps its key as records join; only an out-of-order repeat needs another.
    else
      days.push({
        day,
        key: days.some((group) => group.day === day) ? `${day}:${row.name}` : day,
        rows: [row],
      })
  }
  return days
}

/** Work that can still change: a Run that has not ended, or a Task with work underway. */
function running(row: ActivityRow) {
  // Reef cannot tell whether a Run in an unknown state has ended, so it keeps watching it.
  if (row.kind === 'run') return row.state === 'running' || row.state === 'unknown'
  // A terminal Task outcome does not cancel a Run that is still running. Coral reports
  // a running child Run only through `contextImpactPartial`, which is false when the
  // Task has no context impact total. Such a Task stays live only while it is expanded.
  if (row.kind === 'task') return !row.endedAt || Boolean(row.contextImpactPartial)
  return false
}

/**
 * The loaded records near the viewport that Coral must keep current: those still
 * running, and expanded ones whose child windows follow the scroll. Ended records
 * keep their last loaded state. Nearest to the viewport first wins the bounded window.
 *
 * This window applies only away from the newest records. While the reader follows
 * them, Coral reads the newest page instead, so arrivals appear at the top.
 */
export function liveRoots(list: ActivityList, visible: string[], expanded: string[]): string[] {
  const index = new Map(list.rows.map((row, at) => [row.name, at]))
  const shown = visible.flatMap((name) => (index.has(name) ? [index.get(name)!] : []))
  if (!shown.length) return []
  const first = Math.min(...shown)
  const last = Math.max(...shown)
  const open = new Set(expanded)
  const distance = (at: number) => (at < first ? first - at : at > last ? at - last : 0)
  return list.rows
    .flatMap((row, at) =>
      distance(at) <= NEAR_VIEWPORT && (running(row) || open.has(row.name)) ? [at] : [],
    )
    .toSorted((a, b) => distance(a) - distance(b) || a - b)
    .slice(0, MAX_LIVE_RECORDS)
    .toSorted((a, b) => a - b)
    .map((at) => list.rows[at]!.name)
}
