import { useState } from 'react'

import type { IconName } from '@/wax/components/icon'
import { CoralIcon } from '@/wax/components/icon/custom-icons/coral'
import { SidebarButton } from '@/wax/components/sidebar-button/sidebar-button'
import { Tooltip } from '@/wax/components/tooltip'
import * as styles from './navbar.css'

const NAV_ITEMS: { icon: IconName; label: string; active?: boolean }[] = [
  { icon: 'Activity', label: 'Traces', active: true },
]

const QUERY_STREAM_LABEL = 'Query stream'
const PRIMARY_NAVIGATION_ID = 'primary-navigation'

export function Navbar() {
  const [isCollapsed, setIsCollapsed] = useState(false)

  return (
    <nav className={styles.navbar({ isCollapsed })} aria-label="Coral">
      <div className={styles.header}>
        <Tooltip content={QUERY_STREAM_LABEL}>
          <button
            aria-controls={PRIMARY_NAVIGATION_ID}
            aria-expanded={!isCollapsed}
            aria-label={QUERY_STREAM_LABEL}
            className={styles.brandButton}
            onClick={() => setIsCollapsed((value) => !value)}
            type="button"
          >
            <CoralIcon size={22} />
          </button>
        </Tooltip>
      </div>
      <div className={styles.nav} aria-label="Primary navigation" id={PRIMARY_NAVIGATION_ID}>
        {NAV_ITEMS.map((item) => (
          <SidebarButton
            aria-current={item.active ? 'page' : undefined}
            disabled={item.active}
            icon={item.icon}
            isActive={item.active}
            isMinimized={isCollapsed}
            key={item.label}
          >
            {item.label}
          </SidebarButton>
        ))}
      </div>
    </nav>
  )
}
