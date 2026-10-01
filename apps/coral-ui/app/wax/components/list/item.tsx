import classNames from 'classnames'
import { ElementType, PropsWithChildren, Ref } from 'react'

import * as styles from './list.css'

type ItemProps<T extends ElementType = 'div'> = Omit<React.ComponentPropsWithoutRef<T>, 'ref'> &
  PropsWithChildren & {
    as?: T
    className?: string
    interactive?: boolean
    ref?: Ref<HTMLElement>
    style?: React.CSSProperties
  }

export function Item<T extends ElementType = 'div'>({
  as,
  children,
  className,
  interactive,
  ref,
  style,
  ...props
}: ItemProps<T>) {
  const Component = (as ?? 'div') as ElementType
  return (
    <Component
      className={classNames(styles.item, className, { [styles.interactive]: !!interactive })}
      ref={ref}
      style={style}
      {...props}
    >
      {children}
    </Component>
  )
}
