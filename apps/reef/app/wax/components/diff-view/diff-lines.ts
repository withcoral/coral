import { sourceLines } from '@/wax/components/code-block'

/** One line of a diff, numbered from 1 on each side that holds it. */
export interface DiffLine {
  kind: 'same' | 'removed' | 'added'
  text: string
  /** Its number in the text before; null for an added line. */
  before: number | null
  /** Its number in the text after; null for a removed line. */
  after: number | null
}

/** A run of unchanged lines that a folded diff leaves out. */
export interface DiffFold {
  hidden: number
}

type Edit = DiffLine['kind']

// The longest edit script the search follows. Past it, a diff is too large to
// read line by line, and the search would cost too much memory.
const MAX_EDITS = 1000

/**
 * The shortest line diff from `before` to `after` (Myers, 1986), with each
 * changed block as its removed lines, then its added lines. Lines are split as
 * JavaScript counts them. Two texts further apart than `MAX_EDITS` lines come
 * back as their common start and end around one block that removes and adds
 * the rest.
 */
export function diffLines(before: string, after: string): DiffLine[] {
  const a = sourceLines(before)
  const b = sourceLines(after)
  // The common start and end need no search, and most edits leave them long.
  let start = 0
  while (start < a.length && start < b.length && a[start] === b[start]) start += 1
  let endA = a.length
  let endB = b.length
  while (endA > start && endB > start && a[endA - 1] === b[endB - 1]) {
    endA -= 1
    endB -= 1
  }
  const edits: Edit[] = [
    ...Array<Edit>(start).fill('same'),
    ...removedFirst(shortestEdits(a.slice(start, endA), b.slice(start, endB))),
    ...Array<Edit>(a.length - endA).fill('same'),
  ]
  const lines: DiffLine[] = []
  let x = 0
  let y = 0
  for (const kind of edits) {
    if (kind === 'same') lines.push({ kind, text: a[x]!, before: (x += 1), after: (y += 1) })
    else if (kind === 'removed') lines.push({ kind, text: a[x]!, before: (x += 1), after: null })
    else lines.push({ kind, text: b[y]!, before: null, after: (y += 1) })
  }
  return lines
}

/**
 * Whether the path that reaches diagonal `k` in round `d` comes down from
 * `k + 1`, by adding a line, and not across from `k - 1`, by removing one.
 * `reach` is the furthest line each diagonal reached before the round.
 */
function descends(reach: Int32Array, max: number, d: number, k: number) {
  return k === -d || (k !== d && reach[k - 1 + max]! < reach[k + 1 + max]!)
}

/** The edits that turn `a` into `b`, fewest first; all of both when they are too far apart. */
function shortestEdits(a: string[], b: string[]): Edit[] {
  const n = a.length
  const m = b.length
  const everything = (): Edit[] => [
    ...Array<Edit>(n).fill('removed'),
    ...Array<Edit>(m).fill('added'),
  ]
  if (n === 0 || m === 0) return everything()
  const max = Math.min(n + m, MAX_EDITS)
  // `reach[k + max]` is the furthest line of `a` a path reaches on diagonal
  // `k`, where a path's diagonal is its line in `a` less its line in `b`.
  const reach = new Int32Array(2 * max + 2)
  // The reach before each round, which the walk back reads.
  const rounds: Int32Array[] = []
  for (let d = 0; d <= max; d += 1) {
    rounds.push(reach.slice())
    for (let k = -d; k <= d; k += 2) {
      let x = descends(reach, max, d, k) ? reach[k + 1 + max]! : reach[k - 1 + max]! + 1
      let y = x - k
      while (x < n && y < m && a[x] === b[y]) {
        x += 1
        y += 1
      }
      reach[k + max] = x
      if (x >= n && y >= m) return walkBack(rounds, max, n, m)
    }
  }
  return everything()
}

/** Reads the edits of the path that reached the end, from the end back to the start. */
function walkBack(rounds: Int32Array[], max: number, n: number, m: number): Edit[] {
  const edits: Edit[] = []
  let x = n
  let y = m
  for (let d = rounds.length - 1; d > 0; d -= 1) {
    const from = rounds[d]!
    const k = x - y
    const down = descends(from, max, d, k)
    const previousX = from[(down ? k + 1 : k - 1) + max]!
    const previousY = previousX - (down ? k + 1 : k - 1)
    while (x > previousX && y > previousY) {
      edits.push('same')
      x -= 1
      y -= 1
    }
    edits.push(down ? 'added' : 'removed')
    if (down) y -= 1
    else x -= 1
  }
  for (; x > 0; x -= 1) edits.push('same')
  return edits.toReversed()
}

/** Orders each run of changes as its removals, then its additions, as a reader expects. */
function removedFirst(edits: Edit[]): Edit[] {
  const ordered: Edit[] = []
  let removed = 0
  let added = 0
  const flush = () => {
    ordered.push(...Array<Edit>(removed).fill('removed'), ...Array<Edit>(added).fill('added'))
    removed = 0
    added = 0
  }
  for (const edit of edits) {
    if (edit === 'removed') removed += 1
    else if (edit === 'added') added += 1
    else {
      flush()
      ordered.push(edit)
    }
  }
  flush()
  return ordered
}

/**
 * The diff with each long run of unchanged lines folded down to `context`
 * lines beside a change. A fold hides two lines or more: its own row takes
 * the place of one.
 */
export function foldUnchanged(lines: DiffLine[], context = 3): (DiffLine | DiffFold)[] {
  const rows: (DiffLine | DiffFold)[] = []
  let index = 0
  while (index < lines.length) {
    if (lines[index]!.kind !== 'same') {
      rows.push(lines[index]!)
      index += 1
      continue
    }
    let end = index
    while (end < lines.length && lines[end]!.kind === 'same') end += 1
    // A run keeps context only on a side where a change follows or precedes it.
    const head = index === 0 ? 0 : context
    const tail = end === lines.length ? 0 : context
    const hidden = end - index - head - tail
    if (hidden < 2) rows.push(...lines.slice(index, end))
    else rows.push(...lines.slice(index, index + head), { hidden }, ...lines.slice(end - tail, end))
    index = end
  }
  return rows
}

/** How many lines a diff from `before` to `after` adds and removes. */
export function diffCounts(before: string, after: string) {
  let added = 0
  let removed = 0
  for (const line of diffLines(before, after)) {
    if (line.kind === 'added') added += 1
    else if (line.kind === 'removed') removed += 1
  }
  return { added, removed }
}
