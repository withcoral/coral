import classNames from 'classnames'
import type { PropsWithChildren, Ref } from 'react'

import * as styles from './table.css'

export type BodyProps = PropsWithChildren<{
  className?: string
  ref?: Ref<HTMLDivElement>
}>

export function Body({ children, className, ref }: BodyProps) {
  return (
    <div className={classNames(styles.body, className)} ref={ref} role="rowgroup">
      {children}
    </div>
  )
}
