// Fixed per-density row heights, one constant per row kind as the old grid had
// (CELL_HEIGHT_PX / HEADING_HEIGHT_PX). A row that a virtualizer places has to
// report its height before it renders, so height comes from here and not from
// the content of the cells.
export const CELL_HEIGHT_PX = {
  compact: 32,
  default: 44,
} as const

export const HEADING_HEIGHT_PX = {
  compact: 32,
  default: 44,
} as const

// The column template of the whole table. `Table.Container` builds it from its
// `columns` descriptors.
export const COLUMNS_PROPERTY = '--wax-table-columns'

// The height left for rows after the container subtracts the heading. When
// present, the body owns the vertical scroll port by itself.
export const ROWS_MAX_HEIGHT_PROPERTY = '--wax-table-rows-max-height'

// A column's alignment reaches its cells by position: the container names the
// value once, and the rules in table.css.ts hand it to whichever cell sits in
// that column. Cells past this many columns fall back to left alignment.
export const MAX_ALIGNED_COLUMNS = 12

export const alignProperty = (column: number) => `--wax-table-col-${column}-align`

export const justifyProperty = (column: number) => `--wax-table-col-${column}-justify`

// A single cell's override. It sits ahead of its column's value in the `var()`
// fallback chain, so it wins on the cascade of one element rather than on
// specificity against the position rules.
export const CELL_ALIGN_PROPERTY = '--wax-table-cell-align'

export const CELL_JUSTIFY_PROPERTY = '--wax-table-cell-justify'

export type TableAlign = 'center' | 'left' | 'right'

export type TableDensity = keyof typeof CELL_HEIGHT_PX

/**
 * `auto` sizes the columns to their content and scrolls the table sideways, which
 * makes it a scroll port and leaves a sticky heading nothing to pin to. `fixed`
 * shares the width between the columns and truncates inside the cells, so the
 * table can never outgrow its box and needs no scroll port: that is what lets its
 * heading pin to whatever scrolls outside it.
 */
export type TableLayout = 'auto' | 'fixed'

/** `card` draws bordered, rounded chrome around the table. */
export type TableVariant = 'card' | 'plain'
