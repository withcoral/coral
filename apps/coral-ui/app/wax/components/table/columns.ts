import type { CSSProperties, ReactNode } from 'react'

import type { TableAlign, TableLayout } from './constants'
import { alignProperty, COLUMNS_PROPERTY, justifyProperty } from './constants'

/**
 * How wide a column is:
 * - `content` sizes it to its widest value. Under `layout="auto"` it keeps that
 *   width and the table scrolls sideways to reach it, so nothing is cut off;
 *   under `layout="fixed"` it gives way instead, because a fixed table has to fit
 *   the space it is given.
 * - `fill` shares the leftover width, `{ fill: 2 }` takes twice one share.
 * - a number is that many pixels.
 * - a string is a raw grid track, for the case a stylesheet has to own the width:
 *   pass `var(--my-width, 260px)` and set that property from a media query.
 */
export type ColumnWidth = 'content' | 'fill' | number | string | { fill: number }

export interface Column {
  /** Aligns the heading and every cell in the column. `Cell` can override it. */
  align?: TableAlign
  /** Names the column for a reader when the heading shows no text. */
  ariaLabel?: string
  /** Rendered by `Table.Head` unless it is given children of its own. */
  label?: ReactNode
  width?: ColumnWidth
}

// A zero floor is what lets a cell truncate rather than push its column wider, so
// every track carries one — except a `content` column of a table that scrolls,
// where the floor is the whole point: it holds the track at its full width and
// sends the overflow to the scroll port instead of into an ellipsis.
function resolveWidth(width: ColumnWidth = 'fill', layout: TableLayout): string {
  if (typeof width === 'number') return `${width}px`
  if (typeof width === 'object') return `minmax(0, ${width.fill}fr)`
  if (width === 'content') return layout === 'auto' ? 'max-content' : 'minmax(0, max-content)'
  if (width === 'fill') return 'minmax(0, 1fr)'
  return width
}

const JUSTIFY: Record<TableAlign, string> = {
  center: 'center',
  left: 'flex-start',
  right: 'flex-end',
}

/** The template, plus the alignment each column hands to the cells inside it. */
export function columnStyle(columns: readonly Column[], layout: TableLayout): CSSProperties {
  const properties: Record<string, string> = {
    [COLUMNS_PROPERTY]: columns.map((column) => resolveWidth(column.width, layout)).join(' '),
  }
  columns.forEach((column, index) => {
    if (column.align === undefined) return
    properties[alignProperty(index + 1)] = column.align
    properties[justifyProperty(index + 1)] = JUSTIFY[column.align]
  })
  return properties as CSSProperties
}
