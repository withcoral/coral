import { useEffect, useRef, type RefObject } from 'react'

import type { TableDensity } from './constants'
import { GROUP_HEADING_HEIGHT_PX, HEADING_HEIGHT_PX } from './constants'

/**
 * The top-level group whose heading holds the sticky slot under the table head:
 * the last group, in document order, whose top is at or above the bottom of
 * that slot. Only groups with an `id` count, since the id is what gets reported.
 */
export function pinnedGroupId(
  table: HTMLElement,
  density: TableDensity,
  groupHeadingHeight: number = GROUP_HEADING_HEIGHT_PX[density],
) {
  const slot = table.getBoundingClientRect().top + HEADING_HEIGHT_PX[density] + groupHeadingHeight
  let pinned: string | null = null
  for (const group of table.querySelectorAll<HTMLElement>('[role="rowgroup"][id]')) {
    if (group.getBoundingClientRect().top > slot) break
    pinned = group.id
  }
  return pinned
}

/**
 * Reports the pinned group after each render and on each scroll, only when it
 * changes. Returns the scroll handler for the table element.
 */
export function usePinnedGroup(
  table: RefObject<HTMLElement | null>,
  density: TableDensity,
  groupHeadingHeight: number,
  onChange: ((id: string | null) => void) | undefined,
) {
  const reported = useRef<string | null | undefined>(undefined)
  const callback = useRef(onChange)
  useEffect(() => {
    callback.current = onChange
  }, [onChange])
  const sync = () => {
    if (!callback.current || !table.current) return
    const id = pinnedGroupId(table.current, density, groupHeadingHeight)
    if (id === reported.current) return
    reported.current = id
    callback.current(id)
  }
  // Rows mount, measure, and filter between scrolls; each render may move the
  // groups without a scroll event.
  useEffect(sync)
  return onChange ? sync : undefined
}
