import type { ReactNode } from 'react'
import { Link } from 'react-router'

import * as styles from './table.css'

/**
 * The link in a `Table.Row` with `to`, for keyboard users and new tabs. A pointer
 * opens the whole row, so the link looks like the row's text, and the row shows
 * its focus. `Table.useRowCursor` moves focus between these links.
 */
export function RowLink({
  to,
  children,
  'aria-label': ariaLabel,
}: {
  to: string
  children: ReactNode
  'aria-label'?: string
}) {
  return (
    <Link to={to} prefetch="intent" className={styles.rowLink} aria-label={ariaLabel} data-row-link>
      {children}
    </Link>
  )
}
