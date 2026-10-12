import type { ReactNode } from 'react'
import * as styles from './dialog.css'

/** A persistent title and close control for long-form dialog content. */
export function Header({ children }: { children: ReactNode }) {
  return <header className={styles.header}>{children}</header>
}
