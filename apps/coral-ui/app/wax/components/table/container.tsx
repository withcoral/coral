import classNames from 'classnames'
import type { CSSProperties, PropsWithChildren, ReactNode, Ref } from 'react'

import type { Column } from './columns'
import { columnStyle } from './columns'
import { ColumnsProvider } from './columns-context'
import type { TableDensity, TableVariant } from './constants'
import * as styles from './table.css'

interface ContainerBaseProps {
  ariaLabel?: string
  children?: ReactNode
  className?: string
  /**
   * One descriptor per column, in order. They set the width of each column and
   * the alignment of everything in it, and `Table.Head` renders their labels.
   */
  columns: readonly Column[]
  density?: TableDensity
  ref?: Ref<HTMLDivElement>
  variant?: TableVariant
}

interface ContainerAutoLayoutProps extends ContainerBaseProps {
  /** Sizes the columns to their content and scrolls sideways. The default. */
  layout?: 'auto'
  /** The sideways scroll leaves no room for a vertical one. Needs `layout="fixed"`. */
  maxHeight?: never
}

interface ContainerFixedLayoutProps extends ContainerBaseProps {
  /** Shares the width between the columns, so the table owns no scroll port. */
  layout: 'fixed'
  /** Caps the height and scrolls the rows under the pinned heading. */
  maxHeight?: CSSProperties['maxHeight']
}

export type ContainerProps = PropsWithChildren<ContainerAutoLayoutProps | ContainerFixedLayoutProps>

export function Container({
  ariaLabel,
  children,
  className,
  columns,
  density = 'default',
  layout = 'auto',
  maxHeight,
  ref,
  variant = 'plain',
}: ContainerProps) {
  // Inline, so the scroll port beats the `layout` variant's own overflow.
  const bounds = maxHeight === undefined ? undefined : { maxHeight, overflowY: 'auto' as const }
  return (
    <div
      aria-label={ariaLabel}
      className={classNames(styles.container({ density, layout, variant }), className)}
      ref={ref}
      role="table"
      style={{ ...columnStyle(columns, layout), ...bounds }}
    >
      <ColumnsProvider columns={columns}>{children}</ColumnsProvider>
    </div>
  )
}
