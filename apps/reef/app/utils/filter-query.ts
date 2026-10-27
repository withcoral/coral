/**
 * What someone types in a filter field. A query matches a text that contains
 * it, ignoring case. Every character stands for itself, so a name with `.` or
 * `(` in it matches as typed and no query is invalid.
 */
export interface FilterQuery {
  /** The query as typed, trimmed, for a message such as "No agents match “…”". Empty for no query. */
  readonly text: string
  /** Whether any of `texts` matches. An empty query matches everything. */
  matches(...texts: readonly (string | null | undefined)[]): boolean
  /** The `[start, end)` ranges of `marked` that the query matched, to mark them. */
  ranges(marked: string): [start: number, end: number][]
}

const EVERYTHING: FilterQuery = {
  text: '',
  matches: () => true,
  ranges: () => [],
}

export function filterQuery(input: string): FilterQuery {
  const text = input.trim()
  if (!text) return EVERYTHING
  // A regular expression, not `toLowerCase`, so ranges keep the text's indices
  // where lowercasing would change a character's length.
  const literal = text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  const finds = new RegExp(literal, 'i')
  const marks = new RegExp(literal, 'gi')
  return {
    text,
    matches: (...texts) => texts.some((candidate) => candidate != null && finds.test(candidate)),
    ranges: (marked) =>
      [...marked.matchAll(marks)].map((match) => [match.index, match.index + match[0].length]),
  }
}
