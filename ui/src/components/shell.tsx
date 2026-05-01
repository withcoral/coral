import { useHashRouter, type Route } from '@/hooks/useHashRouter'
import { Sidebar } from '@/components/sidebar/sidebar'
import * as styles from './shell.css'

export function Shell({
  children,
  onNavigate: onNavigateOverride,
}: {
  children: React.ReactNode
  onNavigate?: (route: Route) => void
}) {
  const { route, navigate } = useHashRouter()

  const handleNavigate = (r: Route) => {
    onNavigateOverride?.(r)
    navigate(r)
  }

  return (
    <div className={styles.root}>
      <Sidebar currentRoute={route} onNavigate={handleNavigate} />
      <div className={styles.mainArea}>
        <div className={styles.content}>{children}</div>
      </div>
    </div>
  )
}
