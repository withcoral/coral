import classNames from 'classnames'
import type { KeyboardEvent, ReactNode } from 'react'

import * as styles from './table.css'

type TableVariant = 'compact' | 'default'

interface TableProps {
  children: ReactNode
  className?: string
}

interface WrapperProps {
  children: ReactNode
  className?: string
  variant?: TableVariant
}

interface TableHeadProps {
  children: ReactNode
  className?: string
}

interface TableBodyProps {
  children: ReactNode
  className?: string
}

interface TableRowProps {
  children: ReactNode
  className?: string
  onClick?: () => void
}

interface TableHeaderCellProps {
  align?: 'center' | 'left' | 'right'
  children?: ReactNode
  className?: string
}

interface TableCellProps {
  align?: 'center' | 'left' | 'right'
  children: ReactNode
  className?: string
  mono?: boolean
  title?: string
}

function Wrapper({ children, className, variant = 'default' }: WrapperProps) {
  const tableStyle = variant
  return <div className={classNames(styles.wrapper({ tableStyle }), className)}>{children}</div>
}

function Root({ children, className }: TableProps) {
  return <table className={classNames(styles.table, className)}>{children}</table>
}

function Head({ children, className }: TableHeadProps) {
  return <thead className={classNames(styles.thead, className)}>{children}</thead>
}

function Body({ children, className }: TableBodyProps) {
  return <tbody className={classNames(styles.tbody, className)}>{children}</tbody>
}

function Row({ children, className, onClick }: TableRowProps) {
  const handleKeyDown = (event: KeyboardEvent<HTMLTableRowElement>) => {
    if (!onClick) return
    if (event.key !== 'Enter' && event.key !== ' ') return

    event.preventDefault()
    onClick()
  }

  return (
    <tr
      className={classNames(styles.tr, className)}
      onClick={onClick}
      onKeyDown={onClick ? handleKeyDown : undefined}
      role={onClick ? 'button' : undefined}
      tabIndex={onClick ? 0 : undefined}
    >
      {children}
    </tr>
  )
}

function HeaderCell({ children, className, align = 'left' }: TableHeaderCellProps) {
  return (
    <th className={classNames(styles.th, className)} style={{ textAlign: align }}>
      {children}
    </th>
  )
}

function Cell({ children, className, title, mono = false, align = 'left' }: TableCellProps) {
  const cellClass = mono ? styles.td : styles.tdText
  return (
    <td className={classNames(cellClass, className)} style={{ textAlign: align }} title={title}>
      {children}
    </td>
  )
}

export const Table = {
  Body,
  Cell,
  Head,
  HeaderCell,
  Root,
  Row,
  Wrapper,
}
