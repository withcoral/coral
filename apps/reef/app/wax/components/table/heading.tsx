import classNames from 'classnames'
import type { CSSProperties, PropsWithChildren, Ref } from 'react'

import { alignOverride } from './align'
import type { TableAlign } from './constants'
import * as styles from './table.css'

export type HeadingProps = PropsWithChildren<{
  /** Overrides the alignment its column states. */
  align?: TableAlign
  ariaLabel?: string
  className?: string
  ref?: Ref<HTMLDivElement>
}>

export function Heading({ align, ariaLabel, children, className, ref }: HeadingProps) {
  return (
    <div
      aria-label={ariaLabel}
      className={classNames(styles.heading, className)}
      ref={ref}
      role="columnheader"
      style={alignOverride(align) as CSSProperties}
    >
      <div className={styles.headingText}>{children}</div>
    </div>
  )
}
