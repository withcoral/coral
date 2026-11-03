import type { ActivityPage, ActivityRow } from '@/views/activity/types'

/** Fewest characters an activity search reads, as Coral's `INTENT_QUERY_MIN_CHARS`. */
export const ACTIVITY_SEARCH_MIN_CHARS = 3
/** Most characters a Task or Run intent has, so the most the field takes. */
export const ACTIVITY_SEARCH_FIELD_CHARS = 80
/** Most characters Coral reads, as its `INTENT_QUERY_MAX_CHARS`. */
export const ACTIVITY_SEARCH_MAX_CHARS = 1024

/** The address key that holds the typed search. */
export const activitySearchKey = 'q'

/** The query Coral searches for the typed text: trimmed, or empty while it is too short. */
export function activitySearch(typed: string | null): string {
  const text = (typed ?? '').trim()
  return [...text].length >= ACTIVITY_SEARCH_MIN_CHARS ? text : ''
}

/** A Task a search lists only for its Runs. */
export const keptForRuns = (row: ActivityRow) => row.kind === 'task' && row.intentMatches === false

/**
 * The Tasks a search opens: those it lists only for their Runs, unless you collapsed
 * them. The list decides them, so they take no place in the address or in the limit on
 * the records you expand, and each read holds at most one page of them.
 */
export function revealedTasks(rows: ActivityRow[], collapsed: readonly string[] = []): string[] {
  return rows.flatMap((row) =>
    keptForRuns(row) && !collapsed.includes(row.name) ? [row.name] : [],
  )
}

/** The records open over `rows`: those `page` names, and those its search opens. */
export function openRecords(page: ActivityPage, rows: ActivityRow[]): string[] {
  return [...new Set([...page.expanded, ...revealedTasks(rows, page.collapsed)])]
}
