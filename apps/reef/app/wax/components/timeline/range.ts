/** A stretch of time, in milliseconds. `to` is after `from`. */
export interface TimeRange {
  from: number
  to: number
}

/**
 * `range` scaled by `factor` around the instant at `at` (0 is its start, 1 its end), so
 * that instant stays under the pointer. A factor under 1 zooms in.
 */
export function zoomAt(range: TimeRange, bounds: TimeRange, at: number, factor: number): TimeRange {
  const width = range.to - range.from
  const pivot = range.from + width * at
  const next = clamp(width * factor, Math.min(LEAST_WIDTH, span(bounds)), span(bounds))
  return within(pivot - (pivot - range.from) * (next / width), next, bounds)
}

/** `range` moved by `by` times its width, inside `bounds`. A negative `by` moves it earlier. */
export function pan(range: TimeRange, bounds: TimeRange, by: number): TimeRange {
  return within(range.from + span(range) * by, span(range), bounds)
}

/** A range that frames `interval` with a tenth of its width either side, cut to `bounds`. */
export function focus(interval: TimeRange, bounds: TimeRange): TimeRange {
  const margin = span(interval) / 10
  const from = Math.max(bounds.from, interval.from - margin)
  const to = Math.min(bounds.to, interval.to + margin)
  if (to - from >= LEAST_WIDTH) return { from, to }
  return within((interval.from + interval.to - LEAST_WIDTH) / 2, LEAST_WIDTH, bounds)
}

const SECOND = 1000
const MINUTE = 60 * SECOND
const HOUR = 60 * MINUTE
/** Steps a reader counts in: milliseconds in 1, 2, 5; then clock units. */
const STEPS = [
  1,
  2,
  5,
  10,
  20,
  50,
  100,
  200,
  500,
  SECOND,
  2 * SECOND,
  5 * SECOND,
  10 * SECOND,
  15 * SECOND,
  30 * SECOND,
  MINUTE,
  2 * MINUTE,
  5 * MINUTE,
  10 * MINUTE,
  15 * MINUTE,
  30 * MINUTE,
  HOUR,
  2 * HOUR,
  3 * HOUR,
  6 * HOUR,
  12 * HOUR,
  24 * HOUR,
]

/**
 * The instants in `range` a time axis labels: whole steps after `origin`, about `count` of
 * them, on the smallest step a reader counts in that is no shorter than `range / count`.
 */
export function timeTicks(range: TimeRange, origin: number, count: number): number[] {
  const wanted = span(range) / Math.max(1, count)
  const step = STEPS.find((value) => value >= wanted) ?? Math.ceil(wanted / (24 * HOUR)) * 24 * HOUR
  const ticks: number[] = []
  for (let n = Math.ceil((range.from - origin) / step); origin + n * step <= range.to; n++)
    ticks.push(origin + n * step)
  return ticks
}

/** The pixels one axis label takes, with room around it. */
export const TICK_SPACING = 110

/** Ticks for an axis `width` pixels wide: as many as fit, and at least two. */
export function fittedTicks(range: TimeRange, origin: number, width: number): number[] {
  return timeTicks(range, origin, Math.max(2, Math.floor(width / TICK_SPACING)))
}

/** Timestamps reach the browser in whole milliseconds, so a narrower range shows nothing more. */
const LEAST_WIDTH = 1
const span = (range: TimeRange) => range.to - range.from
const clamp = (value: number, least: number, most: number) => Math.min(most, Math.max(least, value))
/** A range `width` wide that starts at `from`, moved inside `bounds`. */
function within(from: number, width: number, bounds: TimeRange): TimeRange {
  const start = clamp(from, bounds.from, bounds.to - width)
  return { from: start, to: start + width }
}
