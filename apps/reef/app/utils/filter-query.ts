/**
 * What someone types in a filter field. A query matches a text that contains
 * it, ignoring case. Three characters change that:
 *
 * - `^` at the start: the text starts with the query (`^aws`).
 * - `$` at the end: the text ends with the query (`prod$`).
 * - `*` anywhere: any run of characters (`aws*prod`).
 *
 * Every other character, `^` and `$` elsewhere too, stands for itself, so a
 * name with `.` or `(` in it matches as typed and no query is invalid.
 */
export interface FilterQuery {
  /** The query as typed, trimmed, for a message such as "No agents match “…”". Empty for no query. */
  readonly text: string
  /** Whether any of `texts` matches. An empty query matches everything. */
  matches(...texts: readonly (string | null | undefined)[]): boolean
  /** The `[start, end)` ranges of `marked` that the query's characters matched, to mark them. */
  ranges(marked: string): [start: number, end: number][]
}

const EVERYTHING: FilterQuery = { text: '', matches: () => true, ranges: () => [] }

export function filterQuery(input: string): FilterQuery {
  const text = input.trim()
  if (!text) return EVERYTHING
  let pattern = text
  const start = pattern.startsWith('^')
  if (start) pattern = pattern.slice(1)
  const end = pattern.endsWith('$')
  if (end) pattern = pattern.slice(0, -1)
  // One group for each literal piece, so `ranges` can mark it and leave out
  // what `*` matched.
  const body = pattern
    .split('*')
    .map((piece) => `(${piece.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})`)
    .join('.*')
  const source = `${start ? '^' : ''}${body}${end ? '$' : ''}`
  const test = new RegExp(source, 'i')
  // `matchAll` copies its pattern, so every call can share this one.
  const marks = new RegExp(source, 'gid')
  return {
    text,
    matches: (...texts) => texts.some((candidate) => candidate != null && test.test(candidate)),
    ranges: (marked) =>
      [...marked.matchAll(marks)].flatMap((match) =>
        match
          .indices!.slice(1)
          .filter((range): range is [number, number] => !!range && range[1] > range[0]),
      ),
  }
}
