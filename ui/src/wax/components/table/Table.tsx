import { createContext, useContext } from 'react'
import classNames from 'classnames'
import * as styles from './table.css'

type TableStyle = 'compact' | 'default'

const TableStyleContext = createContext<TableStyle>('compact')

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
  onClick?: () => void
}

interface TableHeaderCellProps {
  children?: React.ReactNode
  className?: string
  align?: 'left' | 'center' | 'right'
}

interface TableCellProps {
  children: React.ReactNode
  className?: string
  title?: string
  mono?: boolean
  align?: 'left' | 'center' | 'right'
}

function Wrapper({ children, className, style: tableStyle = 'compact' }: WrapperProps) {
  const wrapperClass = tableStyle === 'default' ? styles.wrapperDefault : styles.wrapperCompact
  return (
    <TableStyleContext.Provider value={tableStyle}>
      <div className={classNames(wrapperClass, className)}>
        {children}
      </div>
    </TableStyleContext.Provider>
  )
}

function Root({ children, className }: TableProps) {
  return (
    <table className={classNames(styles.table, className)}>
      {children}
    </table>
  )
}

function Head({ children, className }: TableHeadProps) {
  return (
    <thead className={classNames(styles.thead, className)}>
      {children}
    </thead>
  )
}

function Body({ children, className }: TableBodyProps) {
  return (
    <tbody className={classNames(styles.tbody, className)}>
      {children}
    </tbody>
  )
}

function Row({ children, className, onClick }: TableRowProps) {
  const tableStyle = useContext(TableStyleContext)
  const rowClass = tableStyle === 'default' ? styles.trDefault : styles.tr
  return (
    <tr className={classNames(rowClass, className)} onClick={onClick}>
      {children}
    </tr>
  )
}

function HeaderCell({ children, className, align = 'left' }: TableHeaderCellProps) {
  const tableStyle = useContext(TableStyleContext)
  const thClass = tableStyle === 'default' ? styles.thDefault : styles.th
  return (
    <th className={classNames(thClass, className)} style={{ textAlign: align }}>
      {children}
    </th>
  )
}

function Cell({ children, className, title, mono = true, align = 'left' }: TableCellProps) {
  const tableStyle = useContext(TableStyleContext)
  let cellClass: string
  if (tableStyle === 'default') {
    cellClass = mono ? styles.tdDefault : styles.tdTextDefault
  } else {
    cellClass = mono ? styles.td : styles.tdText
  }
  return (
    <td
      className={classNames(cellClass, className)}
      title={title}
      style={{ textAlign: align }}
    >
      {children}
    </td>
  )
}

export const Table = {
  Wrapper,
  Root,
  Head,
  Body,
  Row,
  HeaderCell,
  Cell,
}
