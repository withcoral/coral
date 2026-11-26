import classNames from 'classnames'
import type { MouseEventHandler, ReactNode } from 'react'

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
  onClick,
  title,
}: {
  children: ReactNode
  className?: string
  external?: boolean
  href?: string
  /** Runs before the browser follows `href`, such as to move to a place on the page itself. */
  onClick?: MouseEventHandler<HTMLAnchorElement>
  /** Shown as a tooltip, as Markdown's `[text](url "title")` gives it. */
  title?: string
}) {
  return (
    <a
      className={classNames(styles.link, className)}
      href={href}
      onClick={onClick}
      title={title}
      {...(external && { target: '_blank', rel: 'noopener noreferrer' })}
    >
      {children}
    </a>
  )
}
