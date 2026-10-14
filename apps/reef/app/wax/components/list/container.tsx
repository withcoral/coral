import classNames from 'classnames'
import type { ElementType, PropsWithChildren, Ref } from 'react'

import * as styles from './list.css'

type ContainerProps<T extends ElementType = 'div'> = Omit<
  React.ComponentPropsWithoutRef<T>,
  'ref'
> &
  PropsWithChildren<{
    /** An `ol` or `ul` for items that are a list, such as ranked results. */
    as?: T
    className?: string
    ref?: Ref<HTMLElement>
    style?: React.CSSProperties
  }>

export function Container<T extends ElementType = 'div'>({
  as,
  children,
  className,
  ref,
  style,
  ...props
}: ContainerProps<T>) {
  const Component = (as ?? 'div') as ElementType
  return (
    <Component
      className={classNames(styles.container, className)}
      ref={ref}
      style={style}
      {...props}
    >
      {children}
    </Component>
  )
}
