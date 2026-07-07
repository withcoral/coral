import { useNavigationProgress } from '@/utils/use-navigation-progress'

import * as styles from './navigation-progress-bar.css'

export function NavigationProgressBar() {
  const isNavigating = useNavigationProgress()

  if (!isNavigating) return null

  return <div aria-label="Loading" className={styles.bar} role="progressbar" />
}
