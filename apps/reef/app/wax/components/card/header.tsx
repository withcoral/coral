import classNames from 'classnames'
import type { HTMLAttributes } from 'react'
import * as styles from './card.css'

/** Keeps record identity and navigation in view while reading a long card. */
export function Header({
  sticky = false,
  className,
  ...props
}: HTMLAttributes<HTMLElement> & { sticky?: boolean }) {
  return (
    <header
      {...props}
      className={classNames(styles.header, sticky && styles.stickyHeader, className)}
    />
  )
}
