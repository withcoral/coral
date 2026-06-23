import { Navbar } from '@/components/navbar/navbar'
import * as styles from './shell.css'

export function Shell({
  children,
  isNavigating = false,
}: {
  children?: React.ReactNode
  isNavigating?: boolean
}) {
  return (
    <div className={styles.root}>
      <Navbar />
      <main className={styles.mainArea}>
        <div className={styles.content}>
          {isNavigating && (
            <div
              aria-label="Loading page"
              className={styles.navigationProgress}
              role="progressbar"
            />
          )}
          {children}
        </div>
      </main>
    </div>
  )
}
