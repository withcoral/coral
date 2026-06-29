import classNames from 'classnames'
import { PropsWithChildren } from 'react'

import * as styles from './list.css'

type ContainerProps = PropsWithChildren<{
  className?: string
  ref?: React.Ref<HTMLDivElement>
  style?: React.CSSProperties
}>

export function Container({ children, className, ref, style }: ContainerProps) {
  return (
    <div className={classNames(styles.container, className)} ref={ref} style={style}>
      {children}
    </div>
  )
}
