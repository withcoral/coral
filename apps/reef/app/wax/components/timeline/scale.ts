/** A span of time, or of the axis, with `from` at or before `to`. */
export interface Interval {
  from: number
  to: number
}

/** A stretch of time the axis compresses: `from`..`to` in time, drawn `width` wide at `at`. */
export interface Fold extends Interval {
  at: number
  width: number
}

/**
 * Maps time to a position on the axis and back. Both are numbers in one unit, such as
 * milliseconds. The axis starts at 0 and is `extent` long. Every mapping is monotonic, so
 * order on the axis is order in time.
 */
export interface TimeScale extends Interval {
  extent: number
  folds: readonly Fold[]
  toAxis: (time: number) => number
  toTime: (position: number) => number
}

/** An axis of no length would divide by zero wherever it is drawn. */
const MIN_EXTENT = 1

/** Overlapping or touching intervals joined, in order. An interval of no length joins nothing. */
export function unionIntervals(intervals: readonly Interval[]): Interval[] {
  const joined: Interval[] = []
  for (const next of intervals.toSorted((a, b) => a.from - b.from)) {
    if (!(next.to > next.from)) continue
    const last = joined.at(-1)
    if (last && next.from <= last.to) last.to = Math.max(last.to, next.to)
    else joined.push({ from: next.from, to: next.to })
  }
  return joined
}

/** The total length of intervals that do not overlap. */
export const intervalsLength = (intervals: readonly Interval[]) =>
  intervals.reduce((sum, interval) => sum + interval.to - interval.from, 0)

/** The parts of `within` that no interval of `covered` covers, in order. */
export function intervalGaps(within: Interval, covered: readonly Interval[]): Interval[] {
  const gaps: Interval[] = []
  let at = within.from
  for (const interval of unionIntervals(covered)) {
    const from = Math.max(interval.from, within.from)
    const to = Math.min(interval.to, within.to)
    if (to <= from) continue
    if (from > at) gaps.push({ from: at, to: from })
    at = Math.max(at, to)
  }
  if (within.to > at) gaps.push({ from: at, to: within.to })
  return gaps
}

/** Linear interpolation of `value` from the ascending `from` points to the `to` points. */
function interpolate(value: number, from: readonly number[], to: readonly number[]) {
  if (value <= from[0]!) return to[0]!
  for (let index = 1; index < from.length; index++) {
    const start = from[index - 1]!
    const end = from[index]!
    if (value > end) continue
    // Two points at one place: the later one wins, so the mapping never steps back.
    if (end === start) return to[index]!
    return to[index - 1]! + ((value - start) / (end - start)) * (to[index]! - to[index - 1]!)
  }
  return to.at(-1)!
}

function scaleOf(range: Interval, folds: Fold[], time: number[], axis: number[]): TimeScale {
  return {
    from: range.from,
    to: range.to,
    extent: axis.at(-1)!,
    folds,
    toAxis: (value) => interpolate(value, time, axis),
    toTime: (position) => interpolate(position, axis, time),
  }
}

/** Wall time: the axis is proportional to time. */
export function wallScale(range: Interval): TimeScale {
  const to = Math.max(range.to, range.from + MIN_EXTENT)
  return scaleOf({ from: range.from, to }, [], [range.from, to], [0, to - range.from])
}

/**
 * Folded time: the axis is proportional inside `busy` intervals and compresses each long
 * gap between them to one narrow fold. A gap folds when it is longer than `minGap`, and a
 * fold is `foldWidth` wide. By default a fold takes 4% of the busy time, and a gap folds
 * when it is over three folds long, so a fold always makes the axis shorter. With no busy
 * time, nothing folds.
 */
export function foldedScale(
  range: Interval,
  busy: readonly Interval[],
  options: { minGap?: number; foldWidth?: number } = {},
): TimeScale {
  const wall = wallScale(range)
  const within = { from: wall.from, to: wall.to }
  const active = intervalsLength(
    unionIntervals(busy).map((interval) => ({
      from: Math.max(interval.from, within.from),
      to: Math.min(interval.to, within.to),
    })),
  )
  if (!(active > 0)) return wall
  const foldWidth = options.foldWidth ?? active * 0.04
  const minGap = options.minGap ?? foldWidth * 3
  const time = [within.from]
  const axis = [0]
  const folds: Fold[] = []
  const advance = (to: number, length: number) => {
    time.push(to)
    axis.push(axis.at(-1)! + length)
  }
  for (const gap of intervalGaps(within, busy)) {
    if (gap.to - gap.from <= minGap || gap.to - gap.from <= foldWidth) continue
    if (gap.from > time.at(-1)!) advance(gap.from, gap.from - time.at(-1)!)
    folds.push({ ...gap, at: axis.at(-1)!, width: foldWidth })
    advance(gap.to, foldWidth)
  }
  if (within.to > time.at(-1)!) advance(within.to, within.to - time.at(-1)!)
  return scaleOf(within, folds, time, axis)
}

/** The folds that lie at least partly inside `view`, a span of the axis. */
export const foldsIn = (scale: TimeScale, view: Interval) =>
  scale.folds.filter((fold) => fold.at < view.to && fold.at + fold.width > view.from)
