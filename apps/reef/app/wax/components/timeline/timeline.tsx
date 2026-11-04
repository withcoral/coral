import classnames from 'classnames'
import { useEffect, useEffectEvent, useRef, useState, type ReactNode, type RefObject } from 'react'

import { pan, zoomAt, type TimeRange } from './range'
import * as styles from './timeline.css'

export type TimelineTone = keyof typeof styles.toneFill

/** Where `instant` falls in `range`: 0 at its start, 1 at its end, outside 0–1 beyond it. */
export function position(range: TimeRange, instant: number) {
  return (instant - range.from) / (range.to - range.from)
}
const percent = (value: number) => `${value * 100}%`

/** Tick labels for `ticks`, placed where they fall in `range`. */
export function TimelineAxis({
  range,
  ticks,
  label,
  breaks = [],
}: {
  range: TimeRange
  ticks: number[]
  label: (instant: number) => string
  /** Folded stretches, in `range`'s units: each gets a zigzag, so the axis reads as broken. */
  breaks?: TimeRange[]
}) {
  return (
    <div className={styles.axis} aria-hidden>
      {breaks.map((band) => (
        <svg
          key={band.from}
          className={styles.axisBreak}
          style={{ left: percent(position(range, (band.from + band.to) / 2)) }}
          width="10"
          height="12"
          viewBox="0 0 10 12"
        >
          <path d="M2 0 L8 3 L2 6 L8 9 L2 12" fill="none" stroke="currentColor" />
        </svg>
      ))}
      {ticks.map((instant) => (
        <span
          key={instant}
          className={styles.tick}
          style={{ left: percent(position(range, instant)) }}
        >
          {label(instant)}
        </span>
      ))}
    </div>
  )
}

/** A vertical line at each tick, and a band over each folded break, behind whatever shares its
 * box. */
export function TimelineGrid({
  range,
  ticks,
  breaks = [],
}: {
  range: TimeRange
  ticks: number[]
  /** Folded stretches, in `range`'s units. */
  breaks?: TimeRange[]
}) {
  return (
    <div className={styles.grid} aria-hidden>
      {breaks.map((band) => (
        <span
          key={band.from}
          className={styles.breakBand}
          style={{
            left: percent(position(range, band.from)),
            width: percent(position(range, band.to) - position(range, band.from)),
          }}
        />
      ))}
      {ticks.map((instant) => (
        <span
          key={instant}
          className={styles.gridLine}
          style={{ left: percent(position(range, instant)) }}
        />
      ))}
    </div>
  )
}

/** How an interval ends, as far as its records say: at a known instant, still going on, or unknown. */
export type TimelineEnd = { kind: 'ended'; at: number } | { kind: 'ongoing' } | { kind: 'unknown' }

/** The instant `end` draws to: its own, `now` while it goes on, or `start` when unknown. */
export function endInstant(start: number, end: TimelineEnd, now: number) {
  return end.kind === 'ended' ? end.at : end.kind === 'ongoing' ? now : start
}

/**
 * One interval on a row of the waterfall, drawn in `range`. An ongoing one runs to `now`. The
 * part before `waitUntil` is hatched in amber whatever the bar's tone, so a wait never reads
 * as the bar's outcome. `children` is its text, such as a duration, placed beside
 * the bar where it fits. The bar itself is decorative: the row states what it means.
 */
export function TimelineBar({
  range,
  start,
  end,
  now,
  waitUntil,
  tone,
  shape = 'solid',
  children,
}: {
  range: TimeRange
  start: number
  end: TimelineEnd
  now: number
  waitUntil?: number
  tone: TimelineTone
  /** A solid bar, or a bracket that spans the bars of the rows under it. */
  shape?: 'solid' | 'bracket'
  children?: ReactNode
}) {
  const left = position(range, start)
  const right = position(range, endInstant(start, end, now))
  const waitRight = waitUntil === undefined ? left : position(range, waitUntil)
  // Text goes after the bar, before it when the bar reaches the right edge, and inside its
  // end when the bar spans the width and leaves room on neither side.
  const side = right <= 0.8 ? 'after' : left >= 0.2 ? 'before' : 'inside'
  return (
    <>
      {waitRight > left && (
        <span
          aria-hidden
          className={classnames(styles.bar, styles.wait)}
          style={{ left: percent(left), width: percent(waitRight - left) }}
        />
      )}
      <span
        aria-hidden
        className={classnames(
          styles.bar,
          shape === 'bracket' ? styles.bracketTone[tone] : styles.barTone[tone],
          end.kind === 'ongoing' && styles.ongoing,
          end.kind === 'unknown' && styles.unknown,
        )}
        style={{
          left: percent(Math.max(left, waitRight)),
          ...(end.kind !== 'unknown' && { width: percent(right - Math.max(left, waitRight)) }),
        }}
      />
      {children && (
        <span
          className={styles.label({ side })}
          style={
            side === 'after'
              ? {
                  left: `calc(${percent(Math.min(Math.max(right, left), 1))} + ${end.kind === 'unknown' ? styles.UNKNOWN_WIDTH : 0}px)`,
                }
              : side === 'before'
                ? { right: percent(1 - Math.max(left, 0)) }
                : {
                    right: percent(1 - Math.min(right, 1)),
                    // Text longer than the bar ends in an ellipsis; its full text is the row's title.
                    maxWidth: `calc(${percent(Math.min(right, 1) - Math.max(left, 0))} - 4px)`,
                  }
          }
        >
          {children}
        </span>
      )}
    </>
  )
}

/**
 * A stretch on a row when nothing of the row's ran, such as an agent working between calls:
 * a dotted line, with `label` above it when given. It is decorative; the row names it.
 */
export function TimelineGap({
  range,
  from,
  to,
  label,
  labelAt = 'middle',
  emphasis = false,
}: {
  range: TimeRange
  from: number
  to: number
  label?: ReactNode
  /** Centre the label on the stretch, or end it where the stretch ends, as at the axis's end. */
  labelAt?: 'middle' | 'end'
  /** Draw it stronger, for the stretch a reader must notice. */
  emphasis?: boolean
}) {
  const left = position(range, from)
  const width = position(range, to) - left
  return (
    <>
      <span
        aria-hidden
        className={classnames(styles.gap, emphasis && styles.gapEmphasis)}
        style={{ left: percent(left), width: percent(width) }}
      />
      {label && (
        <span
          aria-hidden
          className={classnames(styles.gapLabel, emphasis && styles.gapLabelEmphasis)}
          style={
            labelAt === 'end'
              ? { right: percent(1 - left - width), transform: 'translateY(-50%)' }
              : { left: percent(left + width / 2), transform: 'translate(-50%, -50%)' }
          }
        >
          {label}
        </span>
      )}
    </>
  )
}

/** A point in time on a row: a button that names what happened. */
export function TimelineMark({
  range,
  at,
  shape,
  label,
  onSelect,
}: {
  range: TimeRange
  at: number
  shape: 'diamond' | 'circle' | 'square'
  /** What happened, for a screen reader and the tooltip. */
  label: string
  onSelect: () => void
}) {
  return (
    <button
      type="button"
      aria-label={label}
      title={label}
      className={styles.mark({ shape })}
      style={{ left: percent(position(range, at)) }}
      onClick={(event) => {
        event.stopPropagation()
        onSelect()
      }}
    />
  )
}

/** A mark's shape on its own, to name it in a key. */
export function TimelineMarkKey({ shape }: { shape: 'diamond' | 'circle' | 'square' }) {
  return <span aria-hidden className={styles.markKey({ shape })} />
}

/** A thin line per interval across the minimap. */
export interface MinimapLane {
  start: number
  end: number
  tone: TimelineTone
}

/**
 * The whole of `bounds`, with `range` framed. Drag to frame a new range, click to centre the
 * frame there, double-click to frame everything. ← and → move the frame; + and − zoom it.
 */
export function TimelineMinimap({
  bounds,
  range,
  onRange,
  lanes,
  ticks,
  tickLabel,
  label,
}: {
  bounds: TimeRange
  range: TimeRange
  onRange: (range: TimeRange) => void
  lanes: MinimapLane[]
  ticks: number[]
  tickLabel: (instant: number) => string
  /** The framed range in words, such as "0:48 to 4:12". */
  label: string
}) {
  const box = useRef<HTMLDivElement>(null)
  const [drag, setDrag] = useState<[number, number] | null>(null)
  /** Where `clientX` falls across the minimap, 0 to 1. */
  const shareAt = (clientX: number) => {
    const rect = box.current!.getBoundingClientRect()
    return Math.min(1, Math.max(0, (clientX - rect.left) / rect.width))
  }
  const instant = (value: number) => bounds.from + value * (bounds.to - bounds.from)
  // The frame's edges, 0 to 1: the drag under way, else the range.
  const [frameStart, frameEnd] = drag
    ? [Math.min(...drag), Math.max(...drag)]
    : [position(bounds, range.from), position(bounds, range.to)]
  const height = Math.max(1, Math.min(4, 48 / Math.max(1, lanes.length)))
  return (
    <div
      ref={box}
      role="slider"
      tabIndex={0}
      aria-label="Visible time"
      aria-valuemin={bounds.from}
      aria-valuemax={bounds.to}
      aria-valuenow={range.from}
      aria-valuetext={label}
      className={styles.minimap}
      onPointerDown={(event) => {
        event.currentTarget.setPointerCapture(event.pointerId)
        const at = shareAt(event.clientX)
        setDrag([at, at])
      }}
      onPointerMove={(event) => drag && setDrag([drag[0], shareAt(event.clientX)])}
      onPointerUp={() => {
        if (!drag) return
        const [lo, hi] = [Math.min(...drag), Math.max(...drag)]
        setDrag(null)
        if (hi - lo > 0.003) return onRange({ from: instant(lo), to: instant(hi) })
        // A click centres the frame there.
        const width = range.to - range.from
        onRange(pan(range, bounds, (instant(lo) - (range.from + width / 2)) / width))
      }}
      onDoubleClick={() => onRange(bounds)}
      onKeyDown={(event) => {
        const step = { ArrowLeft: -0.1, ArrowRight: 0.1 }[event.key]
        if (step !== undefined) onRange(pan(range, bounds, step))
        else if (event.key === '+' || event.key === '=') onRange(zoomAt(range, bounds, 0.5, 0.5))
        else if (event.key === '-' || event.key === '_') onRange(zoomAt(range, bounds, 0.5, 2))
        else return
        event.preventDefault()
      }}
    >
      <TimelineAxis range={bounds} ticks={ticks} label={tickLabel} />
      {lanes.map((lane, index) => (
        <span
          key={index}
          aria-hidden
          className={classnames(styles.lane, styles.barTone[lane.tone])}
          style={{
            top: 18 + index * height,
            height: Math.max(1, height - 1),
            left: percent(position(bounds, lane.start)),
            width: percent(position(bounds, lane.end) - position(bounds, lane.start)),
          }}
        />
      ))}
      <span className={styles.shade} style={{ left: 0, width: percent(frameStart) }} />
      <span className={styles.shade} style={{ left: percent(frameEnd), right: 0 }} />
      <span
        className={styles.window}
        style={{ left: percent(frameStart), width: percent(frameEnd - frameStart) }}
      />
    </div>
  )
}

/**
 * Cmd/Ctrl + wheel over `area` zooms `range` around the pointer; Shift + wheel, or a
 * sideways wheel, pans it. `track` is the box the range spans, which sets the pointer's place.
 */
export function useTimelineWheel(
  area: RefObject<HTMLElement | null>,
  track: RefObject<HTMLElement | null>,
  bounds: TimeRange,
  onRange: (update: (range: TimeRange) => TimeRange) => void,
) {
  // The listener reads the latest bounds and callback without re-subscribing.
  const onWheel = useEffectEvent((event: WheelEvent) => {
    const rect = track.current?.getBoundingClientRect()
    if (!rect?.width) return
    if (event.ctrlKey || event.metaKey) {
      event.preventDefault()
      const at = Math.min(1, Math.max(0, (event.clientX - rect.left) / rect.width))
      onRange((range) => zoomAt(range, bounds, at, Math.exp(event.deltaY * 0.003)))
    } else if (event.shiftKey || Math.abs(event.deltaX) > Math.abs(event.deltaY)) {
      event.preventDefault()
      const delta = event.shiftKey ? event.deltaY || event.deltaX : event.deltaX
      onRange((range) => pan(range, bounds, delta / rect.width))
    }
  })
  useEffect(() => {
    const element = area.current
    if (!element) return
    // Preventing the page's own zoom needs a listener that is not passive.
    const listener = (event: WheelEvent) => onWheel(event)
    element.addEventListener('wheel', listener, { passive: false })
    return () => element.removeEventListener('wheel', listener)
  }, [area])
}

/** The width of `track` in pixels, kept current as it resizes. Axis ticks follow it. */
export function useTimelineWidth(track: RefObject<HTMLElement | null>, initial = 600) {
  const [width, setWidth] = useState(initial)
  useEffect(() => {
    const element = track.current
    if (!element) return
    const observer = new ResizeObserver(([entry]) => setWidth(entry!.contentRect.width))
    observer.observe(element)
    return () => observer.disconnect()
  }, [track])
  return width
}

/** The time, ticking each second while `running` so ongoing bars grow; never before `from`. */
export function useTimelineNow(running: boolean, from: number) {
  const [now, setNow] = useState(from)
  useEffect(() => {
    if (!running) return
    const timer = setInterval(() => setNow(Date.now()), 1000)
    return () => clearInterval(timer)
  }, [running])
  return Math.max(now, from)
}
