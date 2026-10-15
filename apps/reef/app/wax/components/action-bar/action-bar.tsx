import type { ReactNode } from 'react'

import * as styles from './action-bar.css'

/**
 * Holds a list and the bar of actions on its selection. Put the list's scroll
 * port first: while a `Bar` shows, the port gains room at its end, so its last
 * rows scroll clear of the bar.
 */
export function Frame({ children }: { children: ReactNode }) {
  return <div className={styles.frame}>{children}</div>
}

/**
 * Actions on the current selection, floating over the foot of the list in its
 * `Frame`. Render it only while something is selected: it takes no room
 * otherwise, and nothing moves when it appears.
 */
export function Bar({ children }: { children: ReactNode }) {
  return <div className={styles.bar}>{children}</div>
}
