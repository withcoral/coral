import { Component, createContext, createElement, type ReactNode, type RefObject } from 'react'

/** The concrete sparse-section protocol shared with Table.Window. */
interface ReadingWindow {
  total: number
  size: number
  items: readonly { index: number }[]
  onViewportChange?: (viewport: { offset: number; anchor: string; preceding: number }) => void
}

export interface ReadingProps {
  /** Keep visible [data-record-key] anchors fixed and restore bounded navigation history. */
  readingKey?: string
  /** No visible identity survives; retain the nearest valid offset and notify the caller. */
  onReadingPositionLost?: () => void
  /** Concealed records must not replace the saved identity-only coordinates. */
  readingPaused?: boolean
  /** Follow insertions only at the caller-certified global edge, never a local page top. */
  readingFollowStart?: boolean
  onReadingStartChange?: (atStart: boolean) => void
  /** Reveal an explicitly selected record once, without moving it on each update. */
  revealRecord?: string
}

type Position = {
  top: number
  left: number
  anchors: { key: string; offset: number }[]
  gap?: { section: string; index: number; offset: number }
}
interface ReadingOptions {
  key?: string
  onLost?: () => void
  paused?: boolean
  /** Only the caller can certify the global edge. A local top never enables following. */
  followStart?: boolean
  onStartChange?: (atStart: boolean) => void
  reveal?: string
  /** Reports the record keys on screen, first to last, whenever they change. */
  onVisible?: (keys: string[]) => void
}
export interface ReadingPositionProps {
  element: RefObject<HTMLDivElement | null>
  options: ReadingOptions
  children: ReactNode
  /** Table supplies its sticky heading and sparse-row sizing variable. Ordinary areas omit both. */
  headingSelector?: string
  /**
   * Rows that pin at the top of their group while its rows scroll. Their own box
   * stays put, so they are measured at their group's top, which moves with the content.
   */
  pinnedSelector?: string
  windowRowHeight?: string
}

/** Internal registration seam: sections do not observe or restore the shared scroll port. */
export const ReadingContext = createContext<Pick<ReadingPosition, 'registerWindow'> | null>(null)

/** One owner captures pre-mutation geometry, sizes sparse sections, restores, then requests data. */
export class ReadingPosition extends Component<ReadingPositionProps> {
  private positions = new Map<string, Position>()
  private windows = new Map<HTMLElement, ReadingWindow>()
  private sent = new WeakMap<HTMLElement, string>()
  private revealed?: string
  private reported?: string
  private restoring = false
  private frame = 0
  private intentFrame = 0
  private resize?: ResizeObserver

  registerWindow = (element: HTMLElement, props: ReadingWindow) => {
    this.windows.set(element, props)
    this.schedule()
    return () => {
      this.windows.delete(element)
    }
  }
  private visible() {
    const port = this.props.element.current
    return port && port.clientHeight > 0 && port.clientWidth > 0 && port.getClientRects().length > 0
  }
  private rows() {
    return [
      ...(this.props.element.current?.querySelectorAll<HTMLElement>('[data-record-key]') ?? []),
    ]
  }
  /** Where a row sits in the content, for anchoring and revealing it. */
  private anchorBox(row: HTMLElement) {
    const box = row.getBoundingClientRect()
    const { pinnedSelector } = this.props
    // A pinned row heads its group, so the group's top is where the row really starts.
    if (!pinnedSelector || !row.matches(pinnedSelector) || !row.parentElement) return box
    const top = row.parentElement.getBoundingClientRect().top
    return { top, bottom: top + box.height, height: box.height }
  }
  private headingBottom() {
    const port = this.props.element.current!
    const heading = this.props.headingSelector
      ? port.querySelector(this.props.headingSelector)
      : null
    return heading?.getBoundingClientRect().bottom ?? port.getBoundingClientRect().top
  }
  /** Where a revealed row must start: below the heading and every heading pinned above it. */
  private revealTop(row: HTMLElement) {
    let top = this.headingBottom()
    const { pinnedSelector } = this.props
    const port = this.props.element.current!
    if (!pinnedSelector) return top
    // Each enclosing group's heading pins above the row while its rows scroll.
    for (let group = row.parentElement; group && group !== port; group = group.parentElement) {
      const heading = group.firstElementChild
      if (heading !== row && heading instanceof HTMLElement && heading.matches(pinnedSelector))
        top += heading.getBoundingClientRect().height
    }
    return top
  }
  private entries(section: HTMLElement) {
    return [...section.children].filter(
      (child): child is HTMLElement => child instanceof HTMLElement,
    )
  }
  private indexAt(entry: HTMLElement, y: number) {
    if (entry.dataset.windowFrom === undefined) return Number(entry.dataset.windowIndex)
    const box = entry.getBoundingClientRect()
    const count = Number(entry.dataset.windowCount)
    return (
      Number(entry.dataset.windowFrom) +
      Math.min(count - 1, Math.max(0, Math.floor((y - box.top) / (box.height / count))))
    )
  }
  private measure(options = this.props.options): Position | null {
    const port = this.props.element.current
    if (!port || !options.key || options.paused || !this.visible()) return null
    const top = port.getBoundingClientRect().top
    const visibleTop = Math.max(top, this.headingBottom())
    const anchors = this.rows().flatMap((row) => {
      const box = this.anchorBox(row)
      return box.bottom > visibleTop && box.top < top + port.clientHeight
        ? [{ key: row.dataset.recordKey!, offset: box.top - top }]
        : []
    })
    // A selected nested record takes precedence over its containing group while visible.
    // Once the reader moves away, ordinary visible anchors take over again.
    const selected = anchors.findIndex((anchor) => anchor.key === options.reveal)
    if (selected > 0) anchors.unshift(...anchors.splice(selected, 1))
    // When scrolling into an unloaded gap, retain a logical coordinate, not an old row above it.
    const gap = [...port.querySelectorAll<HTMLElement>('[data-window-from]')].find((entry) => {
      const box = entry.getBoundingClientRect()
      return box.top <= visibleTop && box.bottom > visibleTop
    })
    const index = gap ? this.indexAt(gap, visibleTop) : 0
    return {
      top: port.scrollTop,
      left: port.scrollLeft,
      anchors,
      gap: gap
        ? {
            section: gap.parentElement!.dataset.window!,
            index,
            offset:
              gap.getBoundingClientRect().top -
              top +
              ((index - Number(gap.dataset.windowFrom)) * gap.getBoundingClientRect().height) /
                Number(gap.dataset.windowCount),
          }
        : undefined,
    }
  }
  private remember(key: string | undefined, position: Position | null) {
    if (!key || !position) return
    this.positions.delete(key)
    this.positions.set(key, position)
    while (this.positions.size > 20) this.positions.delete(this.positions.keys().next().value!)
  }
  private capture = () => this.remember(this.props.options.key, this.measure())
  private scroll = () => {
    this.capture()
    if (this.props.options.key && !this.restoring && !this.props.options.paused && this.visible())
      this.props.options.onStartChange?.(this.props.element.current!.scrollTop <= 1)
    this.schedule()
  }
  private intent = (event: Event) => {
    if (!this.props.options.key || this.props.options.paused) return
    if (
      event instanceof KeyboardEvent &&
      !['ArrowUp', 'ArrowDown', 'PageUp', 'PageDown', 'Home', 'End', ' '].includes(event.key)
    )
      return
    if (event.type === 'pointerup' && event.target !== this.props.element.current) return
    if (
      (event instanceof WheelEvent && event.deltaY > 0) ||
      (event instanceof KeyboardEvent && ['ArrowDown', 'PageDown', 'End', ' '].includes(event.key))
    )
      this.props.options.onStartChange?.(false)
    cancelAnimationFrame(this.intentFrame)
    this.intentFrame = requestAnimationFrame(() => {
      if (this.visible())
        this.props.options.onStartChange?.(this.props.element.current!.scrollTop <= 1)
    })
  }
  private gapTop(saved: NonNullable<Position['gap']>) {
    const section = [...this.windows.keys()].find(
      (element) => element.dataset.window === saved.section,
    )
    if (!section) return undefined
    const entry = this.entries(section).find((item) =>
      item.dataset.windowFrom === undefined
        ? Number(item.dataset.windowIndex) === saved.index
        : Number(item.dataset.windowFrom) <= saved.index &&
          Number(item.dataset.windowFrom) + Number(item.dataset.windowCount) > saved.index,
    )
    if (!entry) return undefined
    const box = entry.getBoundingClientRect()
    return (
      box.top +
      (entry.dataset.windowFrom === undefined
        ? 0
        : ((saved.index - Number(entry.dataset.windowFrom)) * box.height) /
          Number(entry.dataset.windowCount))
    )
  }
  private restore(reset = false) {
    const port = this.props.element.current
    const { key, paused, followStart, reveal, onStartChange, onLost } = this.props.options
    if (!port || !key || paused || !this.visible()) return
    this.restoring = true
    const saved = this.positions.get(key)
    if (saved) {
      const available = new Map(this.rows().map((row) => [row.dataset.recordKey!, row]))
      const anchor = saved.anchors.find((item) => available.has(item.key))
      const gapTop = saved.gap && this.gapTop(saved.gap)
      if (followStart && saved.top <= 1) port.scrollTop = 0
      else if (anchor)
        port.scrollTop +=
          this.anchorBox(available.get(anchor.key)!).top -
          port.getBoundingClientRect().top -
          anchor.offset
      else if (gapTop !== undefined)
        port.scrollTop += gapTop - port.getBoundingClientRect().top - saved.gap!.offset
      else {
        port.scrollTop = saved.top
        if (saved.anchors.length) onLost?.()
      }
      port.scrollLeft = saved.left
    } else if (reset) port.scrollTop = 0
    if (reveal && this.revealed !== reveal) {
      const row = this.rows().find((item) => item.dataset.recordKey === reveal)
      if (row) {
        const box = this.anchorBox(row)
        const bottom = port.getBoundingClientRect().top + port.clientHeight
        const top = this.revealTop(row)
        if (box.top < top || box.height > bottom - top) port.scrollTop += box.top - top
        else if (box.bottom > bottom) port.scrollTop += box.bottom - bottom
        this.revealed = reveal
        onStartChange?.(port.scrollTop <= 1)
      }
    }
    if (followStart && port.scrollTop > 1) onStartChange?.(false)
    this.capture()
  }
  private sizeWindows() {
    const property = this.props.windowRowHeight
    if (!property) return
    for (const [section, props] of this.windows) {
      const heights = this.entries(section).flatMap((entry) => {
        const row = entry.querySelector<HTMLElement>(':scope > [data-record-key]')
        const height = row?.getBoundingClientRect().height ?? 0
        return height > 0 ? [height] : []
      })
      if (!heights.length) continue
      const height = Math.min(
        heights.reduce((sum, value) => sum + value, 0) / heights.length,
        500_000 / Math.max(1, props.total),
      )
      if (
        Math.abs(Number.parseFloat(section.style.getPropertyValue(property)) - height) > 0.5 ||
        !section.style.getPropertyValue(property)
      )
        section.style.setProperty(property, `${height}px`)
    }
  }
  private inspectWindows() {
    const port = this.props.element.current
    if (!port || !this.visible() || this.props.options.paused) return
    const box = port.getBoundingClientRect()
    const top = Math.max(box.top, this.headingBottom())
    const bottom = box.top + port.clientHeight
    for (const [section, props] of this.windows) {
      if (!props.onViewportChange || !props.total) continue
      const bounds = section.getBoundingClientRect()
      if (bounds.bottom <= top || bounds.top >= bottom) continue
      const entries = this.entries(section).filter((entry) => {
        const rect = entry.getBoundingClientRect()
        return rect.bottom > top && rect.top < bottom
      })
      if (!entries.length) continue
      const first = entries[0]!
      const last = entries.at(-1)!
      // An expanded child can fill the viewport by itself. Its own section requests its data.
      if (first === last && first.dataset.windowKey && first.querySelector('[data-window]'))
        continue
      const index = this.indexAt(first, Math.max(top, bounds.top))
      const end = this.indexAt(last, Math.min(bottom, bounds.bottom) - 0.01)
      const buffer = Math.min(10, Math.floor(props.size / 8))
      const start = Math.max(0, index - buffer)
      const stop = Math.min(props.total - 1, end + buffer)
      const loaded = new Set(props.items.map((item) => item.index))
      let covered = stop - start < loaded.size
      for (let at = start; covered && at <= stop; at++) covered = loaded.has(at)
      if (covered) {
        this.sent.delete(section)
        continue
      }
      const atStart = bounds.top >= top
      const atEnd = bounds.bottom <= bottom + 1
      const preceding = atStart || atEnd ? 0 : Math.min(Math.floor(props.size / 4), index)
      const viewport = {
        offset: atStart
          ? 0
          : atEnd
            ? Math.max(0, props.total - props.size)
            : Math.max(0, index - preceding),
        anchor: atStart || atEnd ? '' : (first.dataset.windowKey ?? ''),
        preceding,
      }
      const fingerprint = JSON.stringify(viewport)
      if (this.sent.get(section) !== fingerprint) {
        this.sent.set(section, fingerprint)
        props.onViewportChange(viewport)
      }
    }
  }
  private reportVisible() {
    const port = this.props.element.current
    const { key, paused, onVisible } = this.props.options
    if (!port || !onVisible || !key || paused || !this.visible()) return
    const box = port.getBoundingClientRect()
    const top = Math.max(box.top, this.headingBottom())
    const bottom = box.top + port.clientHeight
    const { pinnedSelector } = this.props
    const keys = this.rows().flatMap((row) => {
      // A pinned heading stays on screen while any row of its group is.
      const group = pinnedSelector && row.matches(pinnedSelector) ? row.parentElement : null
      const rect = (group ?? row).getBoundingClientRect()
      return rect.bottom > top && rect.top < bottom ? [row.dataset.recordKey!] : []
    })
    const fingerprint = keys.join('\n')
    if (fingerprint === this.reported) return
    this.reported = fingerprint
    onVisible(keys)
  }
  private observeRows() {
    this.resize?.disconnect()
    const port = this.props.element.current
    if (port) this.resize?.observe(port)
    for (const row of this.rows()) this.resize?.observe(row)
  }
  private schedule = () => {
    cancelAnimationFrame(this.frame)
    this.frame = requestAnimationFrame(() => {
      this.restoring = false
      this.inspectWindows()
      this.reportVisible()
    })
  }
  private resized = () => {
    if (!this.visible()) return
    const port = this.props.element.current!
    const saved = this.positions.get(this.props.options.key ?? '')
    if (saved && Math.abs(port.scrollTop - saved.top) > 1) this.capture()
    this.sizeWindows()
    this.restore()
    this.schedule()
  }
  componentDidMount() {
    const port = this.props.element.current
    if (!port) return
    port.addEventListener('scroll', this.scroll, { passive: true })
    port.addEventListener('wheel', this.intent, { passive: true })
    port.addEventListener('keydown', this.intent)
    port.addEventListener('pointerup', this.intent)
    this.resize = new ResizeObserver(this.resized)
    this.observeRows()
    this.sizeWindows()
    this.restore()
    this.schedule()
  }
  getSnapshotBeforeUpdate(previous: ReadingPositionProps) {
    return this.measure(previous.options)
  }
  componentDidUpdate(previous: ReadingPositionProps, _state: unknown, snapshot: Position | null) {
    this.remember(previous.options.key, snapshot)
    this.observeRows()
    this.sizeWindows()
    this.restore(previous.options.key !== this.props.options.key)
    this.schedule()
  }
  componentWillUnmount() {
    const port = this.props.element.current
    port?.removeEventListener('scroll', this.scroll)
    port?.removeEventListener('wheel', this.intent)
    port?.removeEventListener('keydown', this.intent)
    port?.removeEventListener('pointerup', this.intent)
    this.resize?.disconnect()
    cancelAnimationFrame(this.frame)
    cancelAnimationFrame(this.intentFrame)
  }
  render() {
    return createElement(ReadingContext.Provider, { value: this }, this.props.children)
  }
}
