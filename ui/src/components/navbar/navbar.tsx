import { Icon, type IconName } from '@/wax/components/icon'
import { CoralIcon } from '@/wax/components/icon/custom-icons/coral'
import * as styles from './navbar.css'

const NAV_ITEMS: { icon: IconName; label: string; active?: boolean }[] = [
  { icon: 'Activity', label: 'Traces', active: true },
]

export function Navbar() {
  return (
    <nav className={styles.navbar} aria-label="Coral">
      <div className={styles.header}>
        <div className={styles.brandButton}>
          <CoralIcon size={22} />
        </div>
      </div>
      <div className={styles.nav} aria-label="Primary navigation">
        {NAV_ITEMS.map((item) => (
          <button aria-current={item.active ? 'page' : undefined} aria-label={item.label} className={styles.navButton} data-active={item.active ? 'true' : 'false'} disabled={item.active} key={item.label} type="button">
            <Icon name={item.icon} size="20" color="inherit" />
          </button>
        ))}
      </div>
    </nav>
  )
}
