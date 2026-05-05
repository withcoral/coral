import { CoralIcon } from '@/wax/components/icon/custom-icons/coral'
import * as styles from './navbar.css'

export function Navbar() {
  return (
    <nav className={styles.navbar} aria-label="Coral">
      <div className={styles.header}>
        <CoralIcon size={22} />
        <span className={styles.brandName}>Coral</span>
      </div>
      <div className={styles.emptyNav} aria-hidden="true" />
    </nav>
  )
}
