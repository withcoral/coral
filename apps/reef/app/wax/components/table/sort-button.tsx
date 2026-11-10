import type { ReactNode } from 'react'

import { Icon } from '@/wax/components/icon'

import type { TableSort } from './constants'
import * as styles from './table.css'

export interface SortButtonProps {
  children: ReactNode
  /** The order this column sets now; pass the same value to its `Table.Heading`. */
  sort: TableSort
  /** The button's accessible name, such as "Sort by usage". */
  label: string
  onClick: () => void
}

/**
 * A column heading's label as a button that sorts the rows by the column. It
 * presses while the column sets the order, and its arrow shows which way.
 */
export function SortButton({ children, sort, label, onClick }: SortButtonProps) {
  return (
    <button
      type="button"
      aria-label={label}
      aria-pressed={sort !== 'none'}
      className={styles.sortButton}
      onClick={onClick}
    >
      {children}
      <Icon
        name={
          sort === 'descending' ? 'ArrowDown' : sort === 'ascending' ? 'ArrowUp' : 'ChevronsUpDown'
        }
        size="14"
        color={sort === 'none' ? 'tertiary' : 'secondary'}
      />
    </button>
  )
}
