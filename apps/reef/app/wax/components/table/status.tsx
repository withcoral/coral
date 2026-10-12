import classNames from 'classnames'
import type { PropsWithChildren, Ref } from 'react'

import { Cell } from './cell'
import { Row } from './row'
import * as styles from './table.css'

export type StatusProps = PropsWithChildren<{
  className?: string
  /** The row's index in a virtualized list, for `useVirtualRows().measureRow`. */
  'data-index'?: number
  ref?: Ref<HTMLDivElement>
  /**
   * `table` (the default) pads the line like a whole-table state. `group` keeps
   * it one row tall, for the state of one group among many that still have rows.
   */
  scope?: 'table' | 'group'
}>

/**
 * The row a table shows in place of its rows: a load in flight, an error, an
 * empty catalog, a search that matched nothing. It spans every column, and it
 * takes no hover, because there is nothing under the pointer to take hold of.
 */
export function Status({
  children,
  className,
  'data-index': index,
  ref,
  scope = 'table',
}: StatusProps) {
  return (
    <Row
      className={classNames(styles.statusRow, scope === 'table' && styles.tableStatusRow)}
      data-index={index}
      ref={ref}
    >
      <Cell
        align="center"
        className={classNames(styles.statusCell({ scope }), className)}
        fullWidth
        wrap
      >
        {children}
      </Cell>
    </Row>
  )
}
