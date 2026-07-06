import { Navbar } from '@/components/navbar/navbar'
import * as styles from './shell.css'

export function Shell({ children }: { children?: React.ReactNode }) {
  return (
    <div className={styles.root}>
      <Navbar />
      <main className={styles.mainArea}>
        <div className={styles.content}>{children}</div>
      </main>
    </div>
  )
}
