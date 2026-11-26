import type { Interval } from './scale'

/** The whole axis. */
export const fullView = (extent: number): Interval => ({ from: 0, to: extent })

/** The shortest span a view shows: a millionth of the axis, so zooming always ends. */
export const minSpan = (extent: number) => extent / 1_000_000

/** `view` moved and shortened to lie inside the axis, at least `minSpan` long. */
export function clampView(view: Interval, extent: number): Interval {
  const span = Math.min(Math.max(view.to - view.from, minSpan(extent)), extent)
  const from = Math.min(Math.max(view.from, 0), extent - span)
  return { from, to: from + span }
}

/**
 * `view` scaled by `factor` around `anchor`, a position on the axis that stays where it
 * is on screen. A factor under 1 zooms in. Without an anchor, the centre stays.
 */
export function zoomView(
  view: Interval,
  extent: number,
  factor: number,
  anchor = (view.from + view.to) / 2,
): Interval {
  const span = view.to - view.from
  const next = Math.min(Math.max(span * factor, minSpan(extent)), extent)
  const share = span > 0 ? (Math.min(Math.max(anchor, view.from), view.to) - view.from) / span : 0.5
  const pivot = view.from + share * span
  return clampView({ from: pivot - share * next, to: pivot + (1 - share) * next }, extent)
}

/** `view` moved by `delta` along the axis, stopping at either end. */
export function panView(view: Interval, extent: number, delta: number): Interval {
  return clampView({ from: view.from + delta, to: view.to + delta }, extent)
}

/**
 * The view that shows `target` with `padding`, a share of its length, on each side. A
 * target of no length gets the shortest span around it.
 */
export function focusView(target: Interval, extent: number, padding = 0.08): Interval {
  const length = Math.max(target.to - target.from, 0)
  const margin = length * padding
  const centre = (target.from + target.to) / 2
  const span = Math.max(length + margin * 2, minSpan(extent))
  return clampView({ from: centre - span / 2, to: centre + span / 2 }, extent)
}

/** Whether `view` shows the whole axis. */
export const isFullView = (view: Interval, extent: number) =>
  view.from <= 0 && view.to >= extent - minSpan(extent)

/** `count` evenly spaced positions across `view`, both ends included. */
export function viewTicks(view: Interval, count: number): number[] {
  if (count < 2) return [view.from]
  const step = (view.to - view.from) / (count - 1)
  return Array.from({ length: count }, (_, index) => view.from + index * step)
}
