import type { PropsWithChildren } from 'react'

import * as styles from './content-container.css'

export function ContentContainer({ children }: PropsWithChildren) {
  return (
    <main className={styles.contentContainer}>
      <div className={styles.content}>{children}</div>
    </main>
  )
}
