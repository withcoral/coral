import { useState } from 'react'

import type { TableAlign, TableSort } from './constants'
import { Heading } from './heading'
import { SortButton } from './sort-button'

export type SortDirection = Exclude<TableSort, 'none'>

/** How one column orders a table's rows. */
export interface SortColumn<T> {
  /** Orders two rows ascending, as `Array.prototype.sort` takes them. */
  compare: (a: T, b: T) => number
  /** The way the column runs when you pick it. */
  first: SortDirection
}

// One fixed locale, so the server and every browser put the rows in the same order when the
// page hydrates. Names that differ only by case or accents, such as "github" and "GitHub",
// tie.
export const compareText = (a: string, b: string) =>
  a.localeCompare(b, 'en', { sensitivity: 'base' })

/** A column of text, which starts at A. */
export function byText<T>(text: (row: T) => string): SortColumn<T> {
  return { compare: (a, b) => compareText(text(a), text(b)), first: 'ascending' }
}

/** A column of counts, which starts at the largest. */
export function byCount<T>(count: (row: T) => number): SortColumn<T> {
  return { compare: (a, b) => count(a) - count(b), first: 'descending' }
}

/** What a column's heading needs from a table's sort. */
export interface SortControl<C extends string> {
  /** The order `column` sets now, for its heading and its sort button. */
  of: (column: C) => TableSort
  /** Sorts by `column`. The column that sorts already turns around. */
  by: (column: C) => void
}

export interface Sort<T, C extends string> extends SortControl<C> {
  /** A sorted copy of `rows`. Rows that compare equal keep their order. */
  rows: (rows: readonly T[]) => T[]
}

/**
 * Which column orders a table's rows, and which way. It is page state: each visit starts
 * again at `initial`, run its `first` way.
 */
export function useSort<T, C extends string>({
  columns,
  initial,
  tiebreak,
}: {
  columns: Record<C, SortColumn<T>>
  initial: NoInfer<C>
  /** Orders the rows that the column finds equal, ascending whichever way the column runs. */
  tiebreak?: (a: T, b: T) => number
}): Sort<T, C> {
  const [sort, setSort] = useState(() => ({ column: initial, direction: columns[initial].first }))
  return {
    rows: (rows) => {
      const way = sort.direction === 'ascending' ? 1 : -1
      const { compare } = columns[sort.column]
      return rows.toSorted((a, b) => way * compare(a, b) || (tiebreak?.(a, b) ?? 0))
    },
    of: (column) => (sort.column === column ? sort.direction : 'none'),
    by: (column) =>
      setSort((now) => ({
        column,
        direction:
          now.column !== column
            ? columns[column].first
            : now.direction === 'ascending'
              ? 'descending'
              : 'ascending',
      })),
  }
}

/** A column heading whose label sorts the rows by the column. */
export function SortHeading<C extends string>({
  sort,
  column,
  align,
  label,
  children,
}: {
  sort: SortControl<C>
  column: C
  align?: TableAlign
  /**
   * The button's accessible name. By default it reads "Sort by" and the column's label.
   * Name the table too when another table beside it has a column of the same label.
   */
  label?: string
  /** The column's label. */
  children: string
}) {
  const order = sort.of(column)
  return (
    <Heading align={align} sort={order}>
      <SortButton
        label={label ?? `Sort by ${children.toLowerCase()}`}
        sort={order}
        onClick={() => sort.by(column)}
      >
        {children}
      </SortButton>
    </Heading>
  )
}
