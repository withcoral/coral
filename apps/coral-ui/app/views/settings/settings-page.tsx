import type { ReactNode } from 'react'

import { ScrollArea } from '@/wax/components'

import * as styles from './settings.css'

/**
 * The frame every settings page shares: a heading that holds its place and a
 * body that scrolls under it.
 */
export function SettingsPage({
  children,
  header,
}: {
  readonly children: ReactNode
  readonly header: ReactNode
}) {
  return (
    <section className={styles.page}>
      <header className={styles.header}>
        <div className={styles.headerInner}>{header}</div>
      </header>
      <ScrollArea.Container className={styles.scroll} constrainWidth fillContent>
        <div className={styles.body}>{children}</div>
      </ScrollArea.Container>
    </section>
  )
}
