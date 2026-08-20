import classNames from 'classnames'
import type { CSSProperties, PropsWithChildren, Ref } from 'react'

import * as styles from './table.css'

export type RowProps = PropsWithChildren<{
  className?: string
  ref?: Ref<HTMLDivElement>
  /** A virtualizer places a row through this: it owns height and offset. */
  style?: CSSProperties
}>

export function Row({ children, className, ref, style }: RowProps) {
  return (
    <div className={classNames(styles.row, className)} ref={ref} role="row" style={style}>
      {children}
    </div>
  )
}
