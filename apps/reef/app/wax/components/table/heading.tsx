import classNames from 'classnames'
import type { CSSProperties, PropsWithChildren, Ref } from 'react'

import { alignOverride } from './align'
import type { TableAlign, TableSort } from './constants'
import * as styles from './table.css'

export type HeadingProps = PropsWithChildren<{
  /** Overrides the alignment its column states. */
  align?: TableAlign
  ariaLabel?: string
  className?: string
  ref?: Ref<HTMLDivElement>
  /** The order the column's values set for the rows, when a reader can sort by it. */
  sort?: TableSort
}>

export function Heading({ align, ariaLabel, children, className, ref, sort }: HeadingProps) {
  return (
    <div
      aria-label={ariaLabel}
      aria-sort={sort}
      className={classNames(styles.heading, className)}
      ref={ref}
      role="columnheader"
      style={alignOverride(align) as CSSProperties}
    >
      <div className={styles.headingText}>{children}</div>
    </div>
  )
}
