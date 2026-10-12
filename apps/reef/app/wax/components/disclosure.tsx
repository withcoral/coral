import type { ReactNode } from 'react'
import { Typography } from '@/wax/components'
import * as styles from './disclosure.css'

/** Native, keyboard-accessible optional detail with shared visual treatment. */
export function Disclosure({
  title,
  children,
  open,
  variant = 'default',
}: {
  title: string
  children: ReactNode
  open?: boolean
  /** Inline sits under one line of text with no rule or padding of its own. */
  variant?: 'default' | 'inline'
}) {
  // Inline reads like the small link button beside it, not like a heading.
  const Summary = variant === 'inline' ? Typography.BodySmall : Typography.BodySmallStrong
  return (
    <details className={variant === 'inline' ? styles.rootInline : styles.root} open={open}>
      <Summary
        as="summary"
        className={variant === 'inline' ? styles.summaryInline : styles.summary}
      >
        {title}
      </Summary>
      <div className={styles.content}>{children}</div>
    </details>
  )
}
