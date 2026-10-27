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
  /** Each match in `marked`, as the ranges its characters matched, to count and step through. */
  hits(marked: string): [start: number, end: number][][]
}

/** The literal pieces between unescaped `*`s, with `\*` and `\\` read. */
function pieces(pattern: string) {
  const found = ['']
  for (let at = 0; at < pattern.length; at++) {
    const char = pattern[at]
    if (char === '\\' && (pattern[at + 1] === '*' || pattern[at + 1] === '\\')) {
      found[found.length - 1] += pattern[++at]
    } else if (char === '*') {
      found.push('')
    } else {
      found[found.length - 1] += char
    }
  }
  return found
}

type Range = [start: number, end: number]

const EVERYTHING: FilterQuery = { text: '', matches: () => true, ranges: () => [], hits: () => [] }

export function filterQuery(input: string): FilterQuery {
  const text = input.trim()
  if (!text) return EVERYTHING
  let pattern = text
  const start = pattern.startsWith('^')
  if (start) pattern = pattern.slice(1)
  const end = pattern.endsWith('$')
  if (end) pattern = pattern.slice(0, -1)
  const literals = pieces(pattern)
  // Each piece is literal text, so finding it cannot backtrack. Finding each
  // at its leftmost place after the one before is enough: a `*` between two
  // pieces takes whatever lies between them.
  const finders = literals.map((piece, index) => {
    const literal = piece.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
    const anchored = index === literals.length - 1 && end ? `${literal}$` : literal
    return new RegExp(anchored, index === 0 && start ? 'iy' : 'ig')
  })
  /** The ranges of one match that starts at `from` or later, or null. */
  const matchFrom = (candidate: string, from: number) => {
    const found: Range[] = []
    let at = from
    for (const finder of finders) {
      finder.lastIndex = at
      const match = finder.exec(candidate)
      if (!match) return null
      at = match.index + match[0].length
      found.push([match.index, at])
    }
    return found
  }
  /** Each match, without the empty ranges a `*` leaves. */
  const hits = (marked: string) => {
    const found: Range[][] = []
    let from = 0
    for (let match = matchFrom(marked, from); match; match = matchFrom(marked, from)) {
      const marks = match.filter(([a, b]) => b > a)
      if (marks.length) found.push(marks)
      const next = match[match.length - 1][1]
      // An anchored query matches once; an empty match would never move on.
      if (start || end || next === from) break
      from = next
    }
    return found
  }
  return {
    text,
    matches: (...texts) =>
      texts.some((candidate) => candidate != null && matchFrom(candidate, 0) !== null),
    ranges: (marked) => hits(marked).flat(),
    hits,
  }
}
