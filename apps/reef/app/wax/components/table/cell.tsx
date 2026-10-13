import classNames from 'classnames'
import type { CSSProperties, PropsWithChildren, Ref } from 'react'

import { alignOverride } from './align'
import { useColumns } from './columns-context'
import type { TableAlign } from './constants'
import * as styles from './table.css'

export type CellProps = PropsWithChildren<{
  /** Holds the row's actions: centred, and shown on the row the pointer or focus is in. */
  actions?: boolean
  /** Overrides the alignment its column states. */
  align?: TableAlign
  className?: string
  /** Holds an input or button, which brings its own padding and focus ring. */
  control?: boolean
  /** Spans every column. Use it for a status or empty-state row. */
  fullWidth?: boolean
  mono?: boolean
  ref?: Ref<HTMLDivElement>
  title?: string
  /** Wraps onto as many lines as the value needs instead of truncating. */
  wrap?: boolean
}>

export function Cell({
  actions = false,
  align,
  children,
  className,
  control = false,
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
      className={classNames(styles.cell({ actions, control, fullWidth, wrap }), className)}
      ref={ref}
      role="cell"
      style={alignOverride(align) as CSSProperties}
      title={title}
    >
      <div className={styles.cellText({ actions, control, mono, wrap })}>{children}</div>
    </div>
  )
}
