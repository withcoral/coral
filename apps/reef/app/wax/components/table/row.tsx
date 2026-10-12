import classNames from 'classnames'
import type { CSSProperties, MouseEventHandler, PropsWithChildren, Ref } from 'react'

import * as styles from './table.css'

export type RowProps = PropsWithChildren<{
  className?: string
  'aria-busy'?: boolean
  'aria-level'?: number
  /** Indicates the row whose exact detail is open. */
  selected?: boolean
  /** Marks the row the keyboard cursor rests on, where no detail is open. */
  active?: boolean
  /** Stable identity for reading-position preservation, never an authority grant. */
  recordKey?: string
  /** A left-edge marker with an exact text description, independent of selection. */
  status?: { tone: 'success' | 'error' | 'warning' | 'neutral'; label: string }
  /** Opens the row's detail; the row shows a pointer. */
  onClick?: MouseEventHandler<HTMLDivElement>
  ref?: Ref<HTMLDivElement>
  /** A virtualizer places a row through this: it owns height and offset. */
  style?: CSSProperties
  /** The row's index in a virtualized list, for `useVirtualRows().measureRow`. */
  'data-index'?: number
  /** Marks a group heading that pins while its rows scroll. `Table.Group` sets it. */
  'data-pinned-heading'?: true
}>

export function Row({
  children,
  className,
  ref,
  style,
  selected = false,
  active = false,
  recordKey,
  status,
  onClick,
  'aria-busy': busy,
  'aria-level': level,
  'data-index': index,
  'data-pinned-heading': pinnedHeading,
}: RowProps) {
  return (
    <div
      className={classNames(
        styles.row,
        onClick && styles.interactiveRow,
        selected && styles.selectedRow,
        active && styles.activeRow,
        status && styles.markedRow,
        className,
      )}
      ref={ref}
      role="row"
      aria-busy={busy}
      aria-level={level}
      onClick={onClick}
      style={style}
      aria-selected={selected || undefined}
      data-status={status?.tone}
      data-active={active || undefined}
      data-record-key={recordKey}
      data-index={index}
      data-pinned-heading={pinnedHeading}
      aria-description={status?.label}
      title={status?.label}
    >
      {children}
    </div>
  )
}
