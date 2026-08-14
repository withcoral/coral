import classNames from 'classnames'
import { ElementType, PropsWithChildren, Ref } from 'react'

import * as styles from './card-list.css'

export type ItemProps<T extends ElementType = 'li'> = Omit<
  React.ComponentPropsWithoutRef<T>,
  'ref'
> &
  PropsWithChildren & {
    as?: T
    className?: string
    ref?: Ref<HTMLElement>
    style?: React.CSSProperties
  }

export function Item<T extends ElementType = 'li'>({
  as,
  children,
  className,
  ref,
  style,
  ...props
}: ItemProps<T>) {
  const Component = (as ?? 'li') as ElementType
  return (
    <Component className={classNames(styles.item, className)} ref={ref} style={style} {...props}>
      {children}
    </Component>
  )
}
