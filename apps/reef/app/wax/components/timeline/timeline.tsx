import classNames from 'classnames'
import {
  useEffect,
  useId,
  useLayoutEffect,
  useRef,
  useState,
  useSyncExternalStore,
  type CSSProperties,
  type HTMLAttributes,
  type KeyboardEvent,
  type MouseEventHandler,
  type PointerEvent,
  type ReactNode,
  type Ref,
} from 'react'
import { Icon } from '@/wax/components/icon'
import { Typography } from '@/wax/components/typography'
import { visuallyHidden } from '@/wax/components/visually-hidden.css'
import { clampView, focusView, fullView, isFullView, panView, viewTicks, zoomView } from './range'
import { foldsIn, type Fold, type Interval, type TimeScale } from './scale'
import * as styles from './timeline.css'

/** The scale a timeline draws on and the span of its axis that shows. */
export interface TimelineView {
  scale: TimeScale
  /** In axis units, inside 0..`scale.extent`. */
  view: Interval
  /** The view shows the whole axis, and keeps showing it as the axis grows. */
  full: boolean
}

/** Moves a timeline's view. Every timeline part that reads it follows. */
export interface TimelineController {
  get: () => TimelineView
  subscribe: (listener: () => void) => () => void
  /** `factor` under 1 zooms in. `anchor` is the axis position that stays in place. */
  zoom: (factor: number, anchor?: number) => void
  /** Moves the view by a share of its own span; negative moves back. */
  pan: (share: number) => void
  /** Shows the whole axis. */
  fit: () => void
  /** Shows a span of time, with a little room around it. */
  focus: (time: Interval) => void
  setView: (view: Interval) => void
}

function createController(initial: TimeScale) {
  let state: TimelineView = { scale: initial, view: fullView(initial.extent), full: true }
  const listeners = new Set<() => void>()
  const update = (scale: TimeScale, view: Interval) => {
    const next = clampView(view, scale.extent)
    state = { scale, view: next, full: isFullView(next, scale.extent) }
    for (const listener of listeners) listener()
  }
  const controller: TimelineController = {
    get: () => state,
    subscribe: (listener) => {
      listeners.add(listener)
      return () => listeners.delete(listener)
    },
    zoom: (factor, anchor) =>
      update(state.scale, zoomView(state.view, state.scale.extent, factor, anchor)),
    pan: (share) =>
      update(
        state.scale,
        panView(state.view, state.scale.extent, share * (state.view.to - state.view.from)),
      ),
    fit: () => update(state.scale, fullView(state.scale.extent)),
    focus: (time) =>
      update(
        state.scale,
        focusView(
          { from: state.scale.toAxis(time.from), to: state.scale.toAxis(time.to) },
          state.scale.extent,
        ),
      ),
    setView: (view) => update(state.scale, view),
  }
  /** A full view stays full. Any other keeps the span of time it showed. */
  const rescale = (scale: TimeScale) => {
    if (scale === state.scale) return
    const { view, full } = state
    update(
      scale,
      full
        ? fullView(scale.extent)
        : {
            from: scale.toAxis(state.scale.toTime(view.from)),
            to: scale.toAxis(state.scale.toTime(view.to)),
          },
    )
  }
  return { controller, rescale }
}

/**
 * One timeline's view. It follows `scale` as that changes, such as when work goes on.
 * Memoize the scale: every new one moves the view and renders its readers again.
 */
export function useTimeline(scale: TimeScale): TimelineController {
  const [{ controller, rescale }] = useState(() => createController(scale))
  // Before paint, so no frame draws the new scale under the old view.
  useLayoutEffect(() => rescale(scale), [rescale, scale])
  return controller
}

/** The current view, for a part that draws text from it. */
export function useTimelineView(controller: TimelineController): TimelineView {
  return useSyncExternalStore(controller.subscribe, controller.get, controller.get)
}

/** The custom property a vanilla-extract variable names, for an inline style. */
const property = (variable: string) => variable.slice(4, -1)
const VIEW_FROM = property(styles.viewFrom)
const VIEW_SPAN = property(styles.viewSpan)
const ITEM_FROM = property(styles.itemFrom)
const ITEM_LENGTH = property(styles.itemLength)
const FOLDS_IMAGE = property(styles.foldsImage)
const LEAD_WIDTH = property(styles.leadWidth)
const EDGE_INSET = property(styles.edgeInset)
const LANE = property(styles.lane)
const LANE_COUNT = property(styles.laneCount)

/** One gradient that shades each fold in `view`, in shares of the view's width. */
function foldsGradient(scale: TimeScale, view: Interval) {
  const span = view.to - view.from
  const folds = foldsIn(scale, view)
  if (!folds.length || !(span > 0)) return 'none'
  const share = (position: number) =>
    `${(Math.min(Math.max((position - view.from) / span, 0), 1) * 100).toFixed(3)}%`
  const stops = folds.flatMap((fold) => {
    const [from, to] = [share(fold.at), share(fold.at + fold.width)]
    return [
      `transparent ${from}`,
      `${styles.foldTint} ${from}`,
      `${styles.foldTint} ${to}`,
      `transparent ${to}`,
    ]
  })
  return `linear-gradient(to right, ${stops.join(', ')})`
}

const place = (from: number, length: number) =>
  ({ [ITEM_FROM]: String(from), [ITEM_LENGTH]: String(length) }) as CSSProperties

/** How far one wheel notch zooms. */
const WHEEL_ZOOM = 1.2

/**
 * Wraps everything one timeline draws. Inside it, a wheel with Ctrl or ⌘ zooms around the
 * pointer, and a horizontal wheel, or one with Shift, pans. Moving the view only changes
 * custom properties here, so no bar renders again. Every track shades the compressed time
 * in view.
 */
export function Root({
  controller,
  className,
  children,
  ref,
  ...props
}: {
  controller: TimelineController
  ref?: Ref<HTMLDivElement>
} & HTMLAttributes<HTMLDivElement>) {
  const element = useRef<HTMLDivElement | null>(null)
  useLayoutEffect(() => {
    const apply = () => {
      const { scale, view } = controller.get()
      element.current?.style.setProperty(VIEW_FROM, String(view.from))
      element.current?.style.setProperty(VIEW_SPAN, String(view.to - view.from))
      element.current?.style.setProperty(FOLDS_IMAGE, foldsGradient(scale, view))
    }
    apply()
    return controller.subscribe(apply)
  }, [controller])
  useEffect(() => {
    const node = element.current
    if (!node) return
    const onWheel = (event: WheelEvent) => {
      const zooms = event.ctrlKey || event.metaKey
      const horizontal = Math.abs(event.deltaX) > Math.abs(event.deltaY)
      if (!zooms && !horizontal && !event.shiftKey) return
      const target = event.target instanceof Element ? event.target : null
      const track = (target?.closest('[data-timeline-track]') ??
        node.querySelector('[data-timeline-track]')) as HTMLElement | null
      const box = track?.getBoundingClientRect()
      if (!box?.width) return
      event.preventDefault()
      const { view } = controller.get()
      if (zooms) {
        const share = Math.min(Math.max((event.clientX - box.left) / box.width, 0), 1)
        const anchor = view.from + share * (view.to - view.from)
        controller.zoom(event.deltaY < 0 ? 1 / WHEEL_ZOOM : WHEEL_ZOOM, anchor)
      } else {
        const delta = horizontal ? event.deltaX : event.deltaY
        controller.pan(delta / box.width)
      }
    }
    // React's wheel listeners are passive, and a passive one cannot stop the page's zoom.
    node.addEventListener('wheel', onWheel, { passive: false })
    return () => node.removeEventListener('wheel', onWheel)
  }, [controller])
  return (
    <div
      {...props}
      ref={(node) => {
        element.current = node
        if (typeof ref === 'function') ref(node)
        else if (ref) ref.current = node
      }}
      className={classNames(styles.root, className)}
    >
      {children}
    </div>
  )
}

/** One row's strip of the axis. Bars, marks and folds go in it. */
export function Track({
  children,
  className,
  ...props
}: { children?: ReactNode } & HTMLAttributes<HTMLDivElement>) {
  return (
    <div {...props} data-timeline-track="" className={classNames(styles.track, className)}>
      {children}
    </div>
  )
}

export type BarColor = keyof typeof styles.barColor
/** Whether the interval has a known end, goes on, or has no recorded end. */
export type BarEnd = keyof typeof styles.bar

export interface BarProps {
  /** Axis positions. `to` at or before `from` draws the shortest bar. */
  from: number
  to: number
  color?: BarColor
  /** The shape of the end, so an open or unknown end reads apart without colour. */
  end?: BarEnd
  /** A thin line, for a record that only frames others. */
  thin?: boolean
  /** Hatched: time spent waiting. */
  wait?: boolean
  selected?: boolean
  /** Shows after the bar's end, such as the sign of a failure. */
  after?: ReactNode
  /** The exact timing, shown on hover. */
  title?: string
  /**
   * Marks each end of the track past which the bar goes on, while the view shows only
   * part of the axis: the bar starts before the view, ends after it, or both.
   */
  edges?: boolean
  /** How far in the sign at the track's start sits, past a lead there, in pixels. */
  edgeInset?: number
}

/** An interval on the axis. */
export function Bar({
  from,
  to,
  color = 'blue',
  end = 'ended',
  thin = false,
  wait = false,
  selected = false,
  after,
  title,
  edges = false,
  edgeInset,
}: BarProps) {
  const position = place(from, Math.max(to - from, 0))
  return (
    <>
      <div
        className={classNames(
          styles.bar[end],
          wait ? styles.waitBar : styles.barColor[color],
          thin && styles.thinBar,
          selected && styles.selectedBar,
        )}
        style={position}
        title={title}
        data-timeline-bar={wait ? 'wait' : end}
      />
      {after && (
        <span className={end === 'ongoing' ? styles.afterOpen : styles.after} style={position}>
          {after}
        </span>
      )}
      {edges && (
        <>
          <span
            className={styles.edgeBefore}
            style={
              edgeInset === undefined
                ? position
                : ({ ...position, [EDGE_INSET]: `${edgeInset}px` } as CSSProperties)
            }
            data-timeline-edge="before"
            aria-hidden
          >
            <Icon name="ChevronLeft" size="14" />
          </span>
          <span
            className={styles.edgeAfter}
            style={position}
            data-timeline-edge="after"
            aria-hidden
          >
            <Icon name="ChevronRight" size="14" />
          </span>
        </>
      )}
    </>
  )
}

export type MarkShape = keyof typeof styles.mark

export interface MarkProps {
  /** Axis position. */
  at: number
  shape: MarkShape
  /** The mark's accessible name, such as "log #4". */
  label: string
  /** More about it, shown on hover: its exact time. */
  title?: string
  /** The mark's record is the one a detail shows. */
  current?: boolean
  href?: string
  onClick?: MouseEventHandler<HTMLElement>
}

/**
 * A point on the axis that opens something: a link with `href`, or else a button. It is
 * no Tab stop: the caller moves through marks with its own keys.
 */
export function Mark({ at, shape, label, title, current = false, href, onClick }: MarkProps) {
  const props = {
    className: styles.mark[shape],
    style: place(at, 0),
    'aria-label': label,
    'aria-current': current || undefined,
    title: title ?? label,
    tabIndex: -1,
    'data-timeline-mark': shape,
    onClick,
  }
  return href === undefined ? (
    <button type="button" {...props} />
  ) : (
    // `aria-label` names the link.
    <a href={href} {...props} />
  )
}

/**
 * The compressed time in view, as one band each. Every `Track` already shades it; put this
 * in one more track, such as the heading's, to say with `label` what each band hides.
 */
export function Folds({
  controller,
  label,
}: {
  controller: TimelineController
  /** What a band hides, such as how long it is. It shows where the band has room. */
  label: (fold: Fold) => string
}) {
  const { scale, view } = useTimelineView(controller)
  return foldsIn(scale, view).map((fold) => (
    <div
      key={fold.from}
      className={styles.fold}
      style={place(fold.at, fold.width)}
      title={label(fold)}
      data-timeline-fold=""
    >
      <Typography.BodySmall className={styles.foldPlate}>
        <span aria-hidden>⋯</span>
        <span className={styles.foldText}>{label(fold)}</span>
      </Typography.BodySmall>
    </div>
  ))
}

/** Tick labels across the view. Each reads the time at its own position. */
export function Axis({
  controller,
  ticks = 5,
  format,
}: {
  controller: TimelineController
  ticks?: number
  format: (time: number) => ReactNode
}) {
  const { scale, view } = useTimelineView(controller)
  const positions = viewTicks(view, ticks)
  return (
    <div className={styles.axis} aria-hidden>
      {positions.map((position, index) => (
        <Typography.CodeSmallInline
          key={index}
          variant="tertiary"
          className={styles.tick}
          style={{ left: `${(index / (positions.length - 1 || 1)) * 100}%` }}
        >
          {format(scale.toTime(position))}
        </Typography.CodeSmallInline>
      ))}
    </div>
  )
}

/**
 * One interval in a `Minimap`, as a thin line in lane `lane` of `lanes`, counted from the
 * top. Lanes let intervals that overlap in time show one above the other, as the rows of
 * the timeline do. It only draws: the window and the timeline itself state the values.
 */
export function MinimapSpan({
  from,
  to,
  lane = 0,
  lanes = 1,
  color = 'neutral',
  end = 'ended',
  flagged = false,
}: {
  /** Axis positions. */
  from: number
  to: number
  lane?: number
  lanes?: number
  color?: BarColor
  /** How it ends, by shape as a `Bar` says it: an open end fades out, an unknown one is dashed. */
  end?: 'ended' | 'ongoing' | 'unknown'
  /** It needs a look, such as a failure: a tick stands across its line where it starts. */
  flagged?: boolean
}) {
  return (
    <div
      className={classNames(
        styles.minimapSpan,
        styles.minimapSpanEnd[end],
        flagged && styles.minimapSpanFlagged,
        styles.barColor[color],
      )}
      data-timeline-span-end={end}
      data-timeline-span-flagged={flagged || undefined}
      style={
        {
          ...place(from, Math.max(to - from, 0)),
          [LANE]: String(lane),
          [LANE_COUNT]: String(Math.max(lanes, 1)),
        } as CSSProperties
      }
      data-timeline-span={color}
    />
  )
}

/**
 * The whole axis in a strip, with a window over the part the timeline shows. Drag the
 * window, or press on the strip, to move the view. With focus on the window, ← and → pan,
 * + and − zoom, and Home shows everything. Its children draw on the whole axis: a
 * `MinimapSpan` for each interval, or a `Bar`.
 */
export function Minimap({
  controller,
  label,
  valueText,
  description,
  children,
}: {
  controller: TimelineController
  /** The window's accessible name. */
  label: string
  /** The range the view shows, as assistive technology reads it. */
  valueText: string
  /** What the strip draws, in words, for a reader who cannot see its lines. */
  description?: string
  children?: ReactNode
}) {
  const describedBy = useId()
  const { scale, view, full } = useTimelineView(controller)
  const strip = useRef<HTMLDivElement>(null)
  const drag = useRef<{ x: number; from: number } | null>(null)
  const span = view.to - view.from
  const toAxis = (clientX: number) => {
    const box = strip.current?.getBoundingClientRect()
    return box?.width ? ((clientX - box.left) / box.width) * scale.extent : 0
  }
  const onPointerDown = (event: PointerEvent<HTMLDivElement>) => {
    if (event.button !== 0) return
    const onWindow = (event.target as Element).closest('[role=slider]')
    // A press outside the window centres the view there, then drags it.
    const from = onWindow ? view.from : toAxis(event.clientX) - span / 2
    if (!onWindow) controller.setView({ from, to: from + span })
    drag.current = { x: event.clientX, from: controller.get().view.from }
    event.currentTarget.setPointerCapture?.(event.pointerId)
  }
  const onPointerMove = (event: PointerEvent<HTMLDivElement>) => {
    const start = drag.current
    const box = strip.current?.getBoundingClientRect()
    if (!start || !box?.width) return
    const from = start.from + ((event.clientX - start.x) / box.width) * scale.extent
    controller.setView({ from, to: from + span })
  }
  const onKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    if (event.altKey || event.ctrlKey || event.metaKey) return
    const step = event.shiftKey ? 0.5 : 0.1
    if (event.key === 'ArrowLeft') controller.pan(-step)
    else if (event.key === 'ArrowRight') controller.pan(step)
    else if (event.key === '+' || event.key === '=') controller.zoom(1 / 1.5)
    else if (event.key === '-' || event.key === '_') controller.zoom(1.5)
    else if (event.key === 'Home' || event.key === '0') controller.fit()
    else return
    event.preventDefault()
    event.stopPropagation()
  }
  return (
    <div
      ref={strip}
      className={styles.minimap}
      style={{ [VIEW_SPAN]: String(scale.extent) } as CSSProperties}
      onPointerDown={onPointerDown}
      onPointerMove={onPointerMove}
      onPointerUp={() => (drag.current = null)}
      onPointerCancel={() => (drag.current = null)}
    >
      {children}
      {description && (
        <span id={describedBy} className={visuallyHidden}>
          {description}
        </span>
      )}
      <div
        role="slider"
        aria-describedby={description ? describedBy : undefined}
        tabIndex={0}
        aria-label={label}
        aria-orientation="horizontal"
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={Math.round(((view.from + view.to) / 2 / scale.extent) * 100)}
        aria-valuetext={valueText}
        data-full={full || undefined}
        className={styles.minimapWindow}
        style={{
          left: `${(view.from / scale.extent) * 100}%`,
          width: `${(span / scale.extent) * 100}%`,
        }}
        onKeyDown={onKeyDown}
      />
    </div>
  )
}

/**
 * What leads an interval on its track, such as its icon and controls: it ends where the
 * interval starts, `width` pixels wide, and never starts before the track.
 */
export function Lead({ at, width, children }: { at: number; width: number; children: ReactNode }) {
  return (
    <span
      className={styles.lead}
      style={{ ...place(at, 0), [LEAD_WIDTH]: `${width}px` } as CSSProperties}
      data-timeline-lead=""
    >
      {children}
    </span>
  )
}

/**
 * A note about the interval at `at`, under its track, from where a lead `width` pixels
 * wide starts. It never overflows the track's width. It repeats what the record states
 * elsewhere, for the eye, so assistive technology skips it.
 */
export function Note({
  at,
  width,
  className,
  children,
}: {
  at: number
  width: number
  className?: string
  children: ReactNode
}) {
  return (
    <span
      className={classNames(styles.note, className)}
      style={{ ...place(at, 0), [LEAD_WIDTH]: `${width}px` } as CSSProperties}
      aria-hidden
      data-timeline-note=""
    >
      {children}
    </span>
  )
}
