import classNames from 'classnames'

import * as styles from './table.css'

type TableStyle = 'compact' | 'default'

interface TableProps {
  children: React.ReactNode
  className?: string
}

interface WrapperProps {
  children: React.ReactNode
  className?: string
  style?: TableStyle
}

interface TableHeadProps {
  children: React.ReactNode
  className?: string
}

interface TableBodyProps {
  children: React.ReactNode
  className?: string
}

interface TableRowProps {
  children: React.ReactNode
  className?: string
}

interface TableHeaderCellProps {
  align?: 'center' | 'left' | 'right'
  children?: React.ReactNode
  className?: string
}

interface TableCellProps {
  align?: 'center' | 'left' | 'right'
  children: React.ReactNode
  className?: string
  mono?: boolean
  title?: string
}

function Wrapper({ children, className, style: tableStyle = 'default' }: WrapperProps) {
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

function Row({ children, className }: TableRowProps) {
  return <tr className={classNames(styles.tr, className)}>{children}</tr>
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
