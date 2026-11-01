import type { ActivityPage, ActivityRow } from '@/views/activity/types'

export const ACTIVITY_CHILD_WINDOW = 100
/** Ordinals only select a bounded window. Anchors are reauthorized, never retained content. */
export interface ActivityViewport {
  offset: number
  anchor: string
  preceding: number
}
export interface ActivityReadingWindow {
  roots: string[]
  followRoot: boolean
  branches: Record<string, ActivityViewport>
}
export interface ActivityObservation {
  path: string
  search: string
  window: ActivityReadingWindow
}
export type ActivityPageValue = { kind: 'page'; page: ActivityPage }
/** A kind of pending request that has its own count and revision. */
export type ActionCountKind = 'approvals' | 'connections'
/** Pending requests in one Workspace. A connection request several Runs share counts once.
 * A revision is opaque and changes whenever your pending requests of that kind change, so a
 * page refreshes on it even when the count stays the same. */
export interface ActionCount {
  total: string
  approvals: string
  connections: string
  revisions: Record<ActionCountKind, string>
}
/** Pending-action counts keyed by Workspace path. Null marks a count that is unavailable. */
export type ActionCounts = Record<string, ActionCount | null>
export type ActivityCountValue = { kind: 'count'; counts: ActionCounts }
/** One stream's server-sent events. A stream carries only its own value kind. */
export type ActivityStreamEvent<Value extends ActivityPageValue | ActivityCountValue> =
  | Value
  | { kind: 'heartbeat' }
  | { kind: 'unavailable' }
  // The stream reached its lifetime bound; the browser replaces it at once.
  | { kind: 'reconnect' }
  // The browser session no longer authorizes observation; the reader must sign in again.
  | { kind: 'sessionExpired' }

export function activityRowIdentity(row: ActivityRow): string {
  return row.kind === 'run' ? (row.submission?.name ?? row.name) : row.name
}
export function readingWindow(page: ActivityPage): ActivityReadingWindow {
  return {
    roots: page.rows.map((row) => row.name),
    followRoot: !page.hasBefore && !page.anchorUnavailable,
    branches: Object.fromEntries(
      Object.entries(page.branches).map(([name, branch]) => [
        name,
        {
          offset: branch.offset ?? 0,
          anchor: '',
          preceding: 0,
        },
      ]),
    ),
  }
}
export function retainRows(rows: ActivityRow[], names: string[]): ActivityRow[] {
  const current = new Map(rows.map((row) => [activityRowIdentity(row), row]))
  return names.flatMap((name) => (current.has(name) ? [current.get(name)!] : []))
}
