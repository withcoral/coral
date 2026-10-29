import classNames from 'classnames'
import type { CSSProperties, MouseEvent, MouseEventHandler, PropsWithChildren, Ref } from 'react'
import { useHref, useNavigate } from 'react-router'

import { ROW_CONTROLS } from './constants'
import * as styles from './table.css'

export type RowProps = PropsWithChildren<{
  className?: string
  'aria-busy'?: boolean
  'aria-level'?: number
  /** Indicates the row whose exact detail is open. */
  selected?: boolean
  /** Marks the row the keyboard cursor rests on, which then takes focus. It reads apart from `selected`. */
  active?: boolean
  /** Stable identity for reading-position preservation, never an authority grant. */
  recordKey?: string
  /** A left-edge marker with an exact text description, independent of selection. */
  status?: { tone: 'success' | 'error' | 'warning' | 'neutral'; label: string }
  /** Opens the row's detail; the row shows a pointer. */
  onClick?: MouseEventHandler<HTMLDivElement>
  /**
   * Makes the whole row a link to this path. A click anywhere in the row follows
   * it, except on a control or link inside the row, which keeps its own behavior.
   * Put a link to the same path in the row's first cell for keyboard users.
   */
  to?: string
  /** Keeps the scroll position when the row's link opens, as for a drawer over the table. */
  preventScrollReset?: boolean
  ref?: Ref<HTMLDivElement>
  /** A virtualizer places a row through this: it owns height and offset. */
  style?: CSSProperties
  /** The row's index in a virtualized list, for `useVirtualRows().measureRow`. */
  'data-index'?: number
  /** Marks a group heading that pins while its rows scroll. `Table.Group` sets it. */
  'data-pinned-heading'?: true
}>

export function Row({ to, preventScrollReset, ...props }: RowProps) {
  if (to === undefined) return <RowElement {...props} />
  return <LinkRow to={to} preventScrollReset={preventScrollReset} {...props} />
}

function LinkRow({ to, preventScrollReset, onClick, ...props }: RowProps & { to: string }) {
  const navigate = useNavigate()
  const href = useHref(to)
  const open = (event: MouseEvent<HTMLDivElement>) => {
    onClick?.(event)
    const target = event.target as Element
    // A portal, such as a dialog opened from the row, is not part of the row.
    if (event.defaultPrevented || !event.currentTarget.contains(target)) return
    const control = target.closest(ROW_CONTROLS)
    // An active row takes focus, so its own tabindex is not a control in it.
    if (control && control !== event.currentTarget && event.currentTarget.contains(control)) return
    // A drag to select text is not a click on the row.
    if (window.getSelection()?.toString()) return
    if (event.metaKey || event.ctrlKey) window.open(href, '_blank', 'noopener')
    else navigate(to, { preventScrollReset })
  }
  return (
    <RowElement {...props} className={classNames(styles.linkRow, props.className)} onClick={open} />
  )
}

function RowElement({
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
}: Omit<RowProps, 'to' | 'preventScrollReset'>) {
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
      // The cursor row takes focus, so the keys reach it wherever focus was.
      tabIndex={active ? -1 : undefined}
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
