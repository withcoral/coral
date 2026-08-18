import classNames from 'classnames'
import type { CSSProperties, PropsWithChildren, ReactNode, Ref } from 'react'

import type { Column } from './columns'
import { columnStyle } from './columns'
import { ColumnsProvider } from './columns-context'
import type { TableDensity, TableVariant } from './constants'
import { MAX_HEIGHT_PROPERTY } from './constants'
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
  /** Caps the table height and scrolls only the rows beneath the heading. */
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
  const bounds =
    maxHeight === undefined
      ? undefined
      : ({
          [MAX_HEIGHT_PROPERTY]: typeof maxHeight === 'number' ? `${maxHeight}px` : maxHeight,
        } as CSSProperties)
  return (
    <div
      aria-label={ariaLabel}
      className={classNames(
        styles.container({ density, layout, variant }),
        maxHeight !== undefined && styles.scrollRows,
        className,
      )}
      ref={ref}
      role="table"
      style={{ ...columnStyle(columns, layout), ...bounds }}
    >
      <ColumnsProvider columns={columns}>{children}</ColumnsProvider>
    </div>
  )
}
