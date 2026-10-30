import type { FilterQuery } from '@/utils/filter-query'

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

const fold = (text: string) => text.replace(/[A-Z]/g, (letter) => letter.toLowerCase())

/**
 * Marks matches as Coral matches intent: every character stands for itself, and only
 * ASCII letters match in either case. `filterQuery` also folds other letters, so it
 * would mark text that Coral did not match.
 */
export function intentQuery(search: string): FilterQuery {
  const text = search.trim()
  const needle = fold(text)
  const ranges = (marked: string): [number, number][] => {
    if (!needle) return []
    const found: [number, number][] = []
    const haystack = fold(marked)
    for (
      let at = haystack.indexOf(needle);
      at !== -1;
      at = haystack.indexOf(needle, at + needle.length)
    )
      found.push([at, at + needle.length])
    return found
  }
  return {
    text,
    matches: (...texts) =>
      !needle || texts.some((candidate) => candidate != null && ranges(candidate).length > 0),
    ranges,
  }
}
