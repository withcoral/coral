import type { ReactNode } from 'react'

import { NavigationRoot } from '@/wax/components/navigation-frame/navigation-frame'

import * as styles from './app-frame.css'

/**
 * The frame of an app: its navigation beside one page. The page sits in the
 * `main` landmark, inside a fine line with round corners. On a phone the page
 * fills the window, and a `NavigationFrame` in `navigation` turns into a
 * drawer that the page's top bar opens.
 */
export function AppFrame({
  navigation,
  children,
}: {
  /** The sidebar. It brings its own width, padding and background. */
  navigation: ReactNode
  children: ReactNode
}) {
  return (
    <NavigationRoot>
      <div className={styles.layout}>
        {navigation}
        <main className={styles.main}>
          <div className={styles.page}>{children}</div>
        </main>
      </div>
    </NavigationRoot>
  )
}
