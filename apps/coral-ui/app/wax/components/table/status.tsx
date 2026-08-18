import classNames from 'classnames'
import type { PropsWithChildren, Ref } from 'react'

import { Cell } from './cell'
import { Row } from './row'
import * as styles from './table.css'

export type StatusProps = PropsWithChildren<{
  className?: string
  ref?: Ref<HTMLDivElement>
}>

/**
 * The row a table shows in place of its rows: a load in flight, an error, an
 * empty catalog, a search that matched nothing. It spans every column, and it
 * takes no hover, because there is nothing under the pointer to take hold of.
 */
export function Status({ children, className, ref }: StatusProps) {
  return (
    <Row className={styles.statusRow} ref={ref}>
      <Cell align="center" className={classNames(styles.statusCell, className)} fullWidth wrap>
        {children}
      </Cell>
    </Row>
  )
}
