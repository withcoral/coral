import classNames from 'classnames'
import { ElementType, PropsWithChildren, Ref } from 'react'

import * as styles from './card-list.css'

export type ContainerProps<T extends ElementType = 'ul'> = Omit<
  React.ComponentPropsWithoutRef<T>,
  'ref'
> &
  PropsWithChildren & {
    as?: T
    className?: string
    ref?: Ref<HTMLElement>
    style?: React.CSSProperties
  }

export function Container<T extends ElementType = 'ul'>({
  as,
  children,
  className,
  ref,
  style,
  ...props
}: ContainerProps<T>) {
  const Component = (as ?? 'ul') as ElementType
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
