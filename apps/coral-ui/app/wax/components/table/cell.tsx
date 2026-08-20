import classNames from 'classnames'
import type { CSSProperties, PropsWithChildren, Ref } from 'react'

import { alignOverride } from './align'
import { useColumns } from './columns-context'
import type { TableAlign } from './constants'
import * as styles from './table.css'

export type CellProps = PropsWithChildren<{
  /** Overrides the alignment its column states. */
  align?: TableAlign
  className?: string
  /** Spans every column. Use it for a status or empty-state row. */
  fullWidth?: boolean
  mono?: boolean
  ref?: Ref<HTMLDivElement>
  title?: string
  /** Wraps onto as many lines as the value needs instead of truncating. */
  wrap?: boolean
}>

export function Cell({
  align,
  children,
  className,
  fullWidth = false,
  mono = false,
  ref,
  title,
  wrap = false,
}: CellProps) {
  const columns = useColumns()
  return (
    <div
      aria-colspan={fullWidth && columns.length > 0 ? columns.length : undefined}
      className={classNames(styles.cell({ fullWidth, wrap }), className)}
      ref={ref}
      role="cell"
      style={alignOverride(align) as CSSProperties}
      title={title}
    >
      <div className={styles.cellText({ mono, wrap })}>{children}</div>
    </div>
  )
}
