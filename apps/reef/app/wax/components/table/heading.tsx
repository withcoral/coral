import classNames from 'classnames'
import type { CSSProperties, PropsWithChildren, Ref } from 'react'

import { Icon } from '@/wax/components/icon'

import { alignOverride } from './align'
import type { TableAlign } from './constants'
import * as styles from './table.css'

export type HeadingProps = PropsWithChildren<{
  /** Overrides the alignment its column states. */
  align?: TableAlign
  ariaLabel?: string
  className?: string
  ref?: Ref<HTMLDivElement>
  /**
   * Makes the heading a control that sorts the rows by its column. `active` marks
   * the column the rows are sorted by now; the rows sort one way, so it reads as
   * descending.
   */
  sort?: { active: boolean; onSort: () => void }
}>

export function Heading({ align, ariaLabel, children, className, ref, sort }: HeadingProps) {
  return (
    <div
      aria-label={ariaLabel}
      aria-sort={sort ? (sort.active ? 'descending' : 'none') : undefined}
      className={classNames(styles.heading, className)}
      ref={ref}
      role="columnheader"
      style={alignOverride(align) as CSSProperties}
    >
      <div className={styles.headingText}>
        {sort ? (
          <button
            type="button"
            className={styles.sortButton}
            aria-pressed={sort.active}
            onClick={sort.onSort}
          >
            {children}
            <Icon
              name="ChevronDown"
              size="14"
              color={sort.active ? 'primary' : 'tertiary'}
              className={sort.active ? undefined : styles.sortIconIdle}
            />
          </button>
        ) : (
          children
        )}
      </div>
    </div>
  )
}
