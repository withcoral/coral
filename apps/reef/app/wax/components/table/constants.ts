// Fixed per-density row heights, one constant per row kind as the old grid had
// (CELL_HEIGHT_PX / HEADING_HEIGHT_PX / GROUP_HEADING_HEIGHT_PX). Sticky table
// heads and Group headings also have to know every pinned row's height before
// the content renders, so height comes from here and not from cell measurement.
export const CELL_HEIGHT_PX = {
  compact: 32,
  comfortable: 36,
  default: 44,
} as const

export const HEADING_HEIGHT_PX = {
  compact: 32,
  comfortable: 36,
  default: 44,
} as const

export const GROUP_HEADING_HEIGHT_PX = {
  compact: 32,
  comfortable: 36,
  default: 44,
} as const

// Controls in a row. A click on one keeps its own behavior, and a row that holds
// one shows hover.
export const ROW_CONTROLS = 'a, button, input, select, textarea, label, [role="button"], [tabindex]'

export const GROUP_DEPTH_PROPERTY = '--wax-table-group-depth'

// Room a scroll table leaves after its last row, inside its own scroll port, so
// an overlay at its foot, such as `ActionBar.Bar`, never hides the last rows.
export const SCROLL_END_ROOM_PROPERTY = '--wax-scroll-end-room'

// `Table.Container` sets its group heading height here. Sticky offsets read it,
// and so does the virtualizer, from the table it scrolls.
export const GROUP_HEADING_HEIGHT_PROPERTY = '--wax-table-group-heading-height'

/** Plain group headings, or prominent ones that stand taller and are tinted by level. */
export type TableGroupHeadings = 'plain' | 'prominent'

/** The height of a table's group headings, for a virtualizer's row estimates. */
export function groupHeadingHeightPx(density: TableDensity, headings: TableGroupHeadings) {
  return GROUP_HEADING_HEIGHT_PX[density] + (headings === 'prominent' ? 8 : 0)
}

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
/** `scroll` owns both scroll axes with a sticky heading, inside a bounded flex parent. */
export type TableLayout = 'auto' | 'fixed' | 'scroll'

/** `card` draws bordered, rounded chrome around the table. */
export type TableVariant = 'card' | 'plain'
