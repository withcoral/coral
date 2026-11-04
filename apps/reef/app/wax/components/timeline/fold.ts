import { TICK_SPACING, timeTicks, type TimeRange } from './range'

/** A stretch of real time drawn as a short break on a folded axis. */
export interface TimeBreak {
  real: TimeRange
  folded: TimeRange
}

/**
 * A time axis with its long gaps folded: each gap is a break of one fixed width, and the time
 * around the gaps keeps its scale. `fold` turns a real instant into its place on the folded
 * axis, and `unfold` turns it back. Instants outside `bounds` keep the scale beyond its ends.
 */
export interface TimeFold {
  bounds: TimeRange
  breaks: TimeBreak[]
  fold: (instant: number) => number
  unfold: (folded: number) => number
}

/** Half a break's zigzag, and the room a tick label takes to its right, in pixels. */
const ZIGZAG_HALF = 6
const LABEL_ROOM = 40
/** The most of the folded axis its breaks may take together. */
const MOST_BREAKS = 0.25

/**
 * Folds each of `gaps` inside `bounds` to a break that takes `share` of the folded axis. A gap
 * shorter than a break keeps its real width, since folding would widen it. With no time outside
 * the gaps there is nothing to keep to scale, and the axis stays unfolded.
 */
export function foldTime(bounds: TimeRange, gaps: TimeRange[], share = 0.03): TimeFold {
  let folding = gaps
    .filter((gap) => gap.from >= bounds.from && gap.to <= bounds.to && gap.to > gap.from)
    .toSorted((a, b) => a.from - b.from)
  let width = 0
  // Each gap left unfolded joins the kept time and widens the breaks, so settle in turns.
  for (;;) {
    const each = Math.min(share, MOST_BREAKS / Math.max(1, folding.length))
    const kept = bounds.to - bounds.from - folding.reduce((sum, gap) => sum + gap.to - gap.from, 0)
    if (kept <= 0 || folding.length === 0) return unfolded(bounds)
    width = (kept * each) / (1 - folding.length * each)
    const longer = folding.filter((gap) => gap.to - gap.from > width)
    if (longer.length === folding.length) break
    folding = longer
  }
  const breaks: TimeBreak[] = []
  let removed = 0
  for (const gap of folding) {
    const from = gap.from - removed
    breaks.push({ real: gap, folded: { from, to: from + width } })
    removed += gap.to - gap.from - width
  }
  const fold = (instant: number) => {
    let shift = 0
    for (const { real, folded } of breaks) {
      if (instant <= real.from) break
      if (instant < real.to)
        return folded.from + ((instant - real.from) / (real.to - real.from)) * width
      shift += real.to - real.from - width
    }
    return instant - shift
  }
  const unfold = (value: number) => {
    let shift = 0
    for (const { real, folded } of breaks) {
      if (value <= folded.from) break
      if (value < folded.to)
        return real.from + ((value - folded.from) / width) * (real.to - real.from)
      shift += real.to - real.from - width
    }
    return value + shift
  }
  return { bounds: { from: bounds.from, to: fold(bounds.to) }, breaks, fold, unfold }
}

const same = (instant: number) => instant

/** `bounds` with nothing folded: every instant keeps its place. */
export function unfolded(bounds: TimeRange): TimeFold {
  return { bounds, breaks: [], fold: same, unfold: same }
}

/**
 * Ticks for the part of `fold` in `range`, an axis `width` pixels wide, as folded instants: whole
 * real steps after `origin` in each stretch between breaks, at least one a stretch, and none
 * closer than half a label's room to the one before or over a break's zigzag. `origin` itself
 * is one when it shows.
 */
export function foldedTicks(fold: TimeFold, range: TimeRange, origin: number, width: number) {
  const pixels = width / (range.to - range.from)
  const real = { from: fold.unfold(range.from), to: fold.unfold(range.to) }
  const stretches: TimeRange[] = []
  let from = real.from
  for (const { real: gap } of fold.breaks) {
    if (gap.to <= real.from || gap.from >= real.to) continue
    if (gap.from > from) stretches.push({ from, to: gap.from })
    from = gap.to
  }
  if (real.to > from) stretches.push({ from, to: real.to })
  // The origin is labelled whenever it shows, even when a break opens the axis.
  const ticks: number[] = origin >= real.from && origin <= real.to ? [fold.fold(origin)] : []
  for (const stretch of stretches) {
    const room = (fold.fold(stretch.to) - fold.fold(stretch.from)) * pixels
    for (const instant of timeTicks(
      stretch,
      origin,
      Math.max(1, Math.floor(room / TICK_SPACING)),
    )) {
      const at = fold.fold(instant)
      const last = ticks.at(-1)
      // A label runs right from its tick: it must end before a zigzag, or start past it.
      const nearBreak = fold.breaks.some(({ folded }) => {
        const ahead = ((folded.from + folded.to) / 2 - at) * pixels
        return ahead > -ZIGZAG_HALF && ahead < LABEL_ROOM
      })
      if (!nearBreak && (last === undefined || (at - last) * pixels >= TICK_SPACING / 2))
        ticks.push(at)
    }
  }
  return ticks
}
