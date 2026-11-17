import classNames from 'classnames'
import type { ReactNode } from 'react'

import * as styles from './text-link.css'

/**
 * A link inside running text. It takes the size of the text around it. An
 * external link opens in a new tab.
 */
export function TextLink({
  children,
  className,
  external = false,
  href,
}: {
  children: ReactNode
  className?: string
  external?: boolean
  href?: string
}) {
  return (
    <a
      className={classNames(styles.link, className)}
      href={href}
      {...(external && { target: '_blank', rel: 'noopener noreferrer' })}
    >
      {children}
    </a>
  )
}
