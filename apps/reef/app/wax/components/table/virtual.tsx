import { useVirtualizer } from '@tanstack/react-virtual'
import type { ReactNode, Ref, RefObject } from 'react'
import { useEffect, useMemo, useRef } from 'react'

import { Cell } from './cell'
import {
  GROUP_HEADING_HEIGHT_PROPERTY,
  GROUP_HEADING_HEIGHT_PX,
  HEADING_HEIGHT_PX,
  type TableDensity,
} from './constants'
import { Row } from './row'
import * as styles from './table.css'

/**
 * The rows a scrolling table keeps mounted, as an inclusive index range into
 * the caller's flat row list, and the tools to place, measure, and reach rows.
 */
export interface VirtualRowsHandle {
  /** The first and last row indexes that mount. New object when either moves. */
  range: VirtualRange
  /**
   * Scrolls the table so the row at `index` starts just under the head and the
   * `depth` group headings pinned above it (0 for a top-level heading, 1 for a
   * row directly in a top-level group, and so on).
   */
  scrollToIndex: (index: number, options?: { depth?: number }) => void
  /**
   * Scrolls the least distance that shows the whole row at `index` below the
   * head and its `depth` pinned group headings. A row already in view stays put.
   */
  revealIndex: (index: number, options?: { depth?: number }) => void
  /** The height, in pixels, that the rows from `first` to `last` inclusive take. */
  extent: (first: number, last: number) => number
  /** Goes on every mounted row with `data-index`, so its real height replaces the estimate. */
  measureRow: Ref<HTMLDivElement>
  /** The height of every row together, measured where a row has mounted and estimated elsewhere. */
  totalSize: number
}

export interface VirtualRange {
  first: number
  last: number
}

/**
 * Mounts only the rows near the viewport of a `layout="scroll"` table. Rows
 * are addressed by their index in one flat list the caller keeps, whatever
 * groups they sit in; `Table.VirtualRows` renders a group's share of that list.
 *
 * Headings are the caller's to mount always, so the sticky groups behave as
 * they do without a virtualizer: only ordinary rows come and go.
 */
export function useVirtualRows({
  count,
  density = 'default',
  estimateSize,
  scrollRef,
  overscan = 16,
}: {
  /** Rows in the flat list, headings included. */
  count: number
  /** The table's density, which sets how tall its pinned headings are. */
  density?: TableDensity
  /** The height to assume for a row until it mounts and measures itself. */
  estimateSize: (index: number) => number
  /** The `Table.Container` that scrolls. */
  scrollRef: RefObject<HTMLDivElement | null>
  overscan?: number
}): VirtualRowsHandle {
  // oxlint-disable-next-line react/incompatible-library -- TanStack Virtual owns scroll measurements; the handle below is what crosses the compiler boundary.
  const virtualizer = useVirtualizer({
    count,
    estimateSize,
    getScrollElement: () => scrollRef.current,
    initialRect: { width: 1000, height: 720 },
    overscan,
  })
  const items = virtualizer.getVirtualItems()
  const first = items[0]?.index ?? 0
  const last = items[items.length - 1]?.index ?? -1
  // Row measurements move the spacers without moving the range, so the total
  // size is part of the handle: a memoized body re-renders on either.
  const totalSize = virtualizer.getTotalSize()
  const range = useMemo(() => ({ first, last }), [first, last])
  const settling = useRef(0)
  useEffect(() => () => cancelAnimationFrame(settling.current), [])
  return useMemo(() => {
    // `Table.Container` sets its group heading height on the table it scrolls.
    const groupHeadingHeight = () =>
      Number.parseFloat(
        scrollRef.current?.style.getPropertyValue(GROUP_HEADING_HEIGHT_PROPERTY) ?? '',
      ) || GROUP_HEADING_HEIGHT_PX[density]
    const pinnedHeight = (depth: number) =>
      HEADING_HEIGHT_PX[density] + depth * groupHeadingHeight()
    // The head sits above the first row in the scroll port, so a row's estimated
    // offset there is the head's height plus its start in the list.
    const inPort = (offset: number) => HEADING_HEIGHT_PX[density] + offset
    // Where the row at `index` lies in the port, from the DOM. Estimates for
    // rows that never mounted and the head above the rows put the virtualizer's
    // offsets off by many pixels, so a mounted row is the truth.
    const laidOut = (index: number) => {
      const port = scrollRef.current
      if (!port || port.getBoundingClientRect().height === 0) return undefined
      const row = port.querySelector<HTMLElement>(`[data-index="${index}"]`)
      // A pinned heading reports where it sticks; its group starts where it lies.
      const anchor = row?.hasAttribute('data-pinned-heading') ? row.parentElement : row
      if (!anchor) return undefined
      const top =
        anchor.getBoundingClientRect().top - port.getBoundingClientRect().top - port.clientTop
      return { port, top, bottom: top + row!.getBoundingClientRect().height }
    }
    return {
      range,
      scrollToIndex: (index, { depth = 0 } = {}) => {
        const pinned = pinnedHeight(depth)
        const start = virtualizer.measurementsCache[index]?.start
        if (start === undefined) return
        // The estimate mounts the rows around the target; the frames after it
        // correct against the target's real position as those rows measure.
        cancelAnimationFrame(settling.current)
        virtualizer.scrollToOffset(Math.max(0, inPort(start) - pinned), { align: 'start' })
        let frames = 12
        let steady = 0
        const settle = () => {
          const row = laidOut(index)
          if (row) {
            const delta = row.top - pinned
            if (Math.abs(delta) < 1) steady++
            else {
              steady = 0
              row.port.scrollTop += delta
            }
          }
          if (steady < 2 && --frames > 0) settling.current = requestAnimationFrame(settle)
        }
        settling.current = requestAnimationFrame(settle)
      },
      revealIndex: (index, { depth = 0 } = {}) => {
        const pinned = pinnedHeight(depth)
        const row = laidOut(index)
        if (row) {
          if (row.top < pinned) row.port.scrollTop += row.top - pinned
          else if (row.bottom > row.port.clientHeight)
            row.port.scrollTop += row.bottom - row.port.clientHeight
          return
        }
        const item = virtualizer.measurementsCache[index]
        const port = scrollRef.current
        if (!item || !port) return
        const top = port.scrollTop
        if (inPort(item.start) - pinned < top)
          virtualizer.scrollToOffset(Math.max(0, inPort(item.start) - pinned), { align: 'start' })
        else if (inPort(item.end) > top + port.clientHeight)
          virtualizer.scrollToOffset(inPort(item.end) - port.clientHeight, { align: 'start' })
      },
      extent: (from, to) => {
        const cache = virtualizer.measurementsCache
        const start = cache[from]
        const end = cache[to]
        return start && end ? end.end - start.start : 0
      },
      measureRow: virtualizer.measureElement,
      totalSize,
    } satisfies VirtualRowsHandle
  }, [density, range, scrollRef, totalSize, virtualizer])
}

/**
 * One group's share of the virtual rows: those inside the mounted range render
 * through `children`, the rest collapse to one spacer before and one after, so
 * the group keeps its full height and the scroll position is the same whichever
 * rows are mounted. `rows` must be in index order.
 */
export function VirtualRows<T extends { index: number }>({
  children,
  rows,
  virtual,
}: {
  children: (row: T) => ReactNode
  rows: readonly T[]
  virtual: VirtualRowsHandle
}) {
  const spans = rows.map((row) => ({ row, start: row.index, end: row.index }))
  return (
    <VirtualSpans spans={spans} virtual={virtual}>
      {({ row }) => children(row)}
    </VirtualSpans>
  )
}

/**
 * The nested groups of a group, for a table with too many to mount. A group
 * with a row in the mounted range renders through `children`, so its heading
 * pins while any of its rows scroll under it. The rest collapse to a spacer
 * before and one after, as `VirtualRows` does for rows, so a pinned heading
 * costs layout only where it can pin. `index` is the group's heading and
 * `end` its last row, in the caller's flat list. `groups` must be in index order.
 */
export function VirtualGroups<T extends { index: number; end: number }>({
  children,
  groups,
  virtual,
}: {
  children: (group: T) => ReactNode
  groups: readonly T[]
  virtual: VirtualRowsHandle
}) {
  const spans = groups.map((group) => ({ group, start: group.index, end: group.end }))
  return (
    <VirtualSpans spans={spans} virtual={virtual}>
      {({ group }) => children(group)}
    </VirtualSpans>
  )
}

function VirtualSpans<T extends { start: number; end: number }>({
  children,
  spans,
  virtual,
}: {
  children: (span: T) => ReactNode
  spans: readonly T[]
  virtual: VirtualRowsHandle
}) {
  const { first, last } = virtual.range
  const before = spans.filter(({ end }) => end < first).length
  const after = spans.filter(({ start }) => start > last).length
  const mounted = spans.slice(before, spans.length - after)
  return (
    <>
      {before > 0 && (
        <VirtualSpacer height={virtual.extent(spans[0]!.start, spans[before - 1]!.end)} />
      )}
      {mounted.map(children)}
      {after > 0 && (
        <VirtualSpacer
          height={virtual.extent(spans[spans.length - after]!.start, spans[spans.length - 1]!.end)}
        />
      )}
    </>
  )
}

// A run of rows the virtualizer left unmounted, holding their height.
function VirtualSpacer({ height }: { height: number }) {
  return (
    <Row className={styles.virtualSpacer} style={{ blockSize: height, minHeight: height }}>
      <Cell fullWidth />
    </Row>
  )
}
