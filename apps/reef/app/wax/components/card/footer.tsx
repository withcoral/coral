import classNames from 'classnames'
import type { HTMLAttributes, Ref } from 'react'
import * as styles from './card.css'

/** Keeps the card's decision controls in view while reading a long card. */
export function Footer({
  sticky = false,
  className,
  ...props
}: HTMLAttributes<HTMLElement> & { sticky?: boolean; ref?: Ref<HTMLElement> }) {
  return (
    <footer
      {...props}
      className={classNames(styles.footer, sticky && styles.stickyFooter, className)}
    />
  )
}
