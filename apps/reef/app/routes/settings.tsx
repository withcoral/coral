import { Outlet } from 'react-router'

import * as styles from '@/views/settings/settings.css'

export default function SettingsRoute() {
  return (
    <main className={styles.page}>
      <div className={styles.container}>
        <Outlet />
      </div>
    </main>
  )
}
