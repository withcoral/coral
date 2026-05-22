import { useState } from 'react'

import type { IconName } from '@/wax/components/icon'
import { IconButton } from '@/wax/components/button'
import { CoralIcon } from '@/wax/components/icon/custom-icons/coral'
import { SidebarButton } from '@/wax/components/sidebar-button/sidebar-button'
import { Tooltip } from '@/wax/components/tooltip'
import * as styles from './navbar.css'

const NAV_ITEMS: { icon: IconName; label: string; active?: boolean }[] = [
  { icon: 'Activity', label: 'Traces', active: true },
]

const QUERY_STREAM_LABEL = 'Query stream'
const COLLAPSE_SIDEBAR_LABEL = 'Collapse sidebar'
const EXPAND_SIDEBAR_LABEL = 'Expand sidebar'
const PRIMARY_NAVIGATION_ID = 'primary-navigation'

export function Navbar() {
  const [isCollapsed, setIsCollapsed] = useState(false)
  const toggleLabel = isCollapsed ? EXPAND_SIDEBAR_LABEL : COLLAPSE_SIDEBAR_LABEL
  const toggleIconName = isCollapsed ? 'ChevronRight' : 'ChevronLeft'
  const toggleSidebar = () => setIsCollapsed((value) => !value)

  return (
    <nav className={styles.navbar({ isCollapsed })} aria-label="Coral">
      <div className={styles.header({ isCollapsed })}>
        <Tooltip content={QUERY_STREAM_LABEL}>
          <span
            aria-label={QUERY_STREAM_LABEL}
            className={styles.brandMark}
            role="img"
            tabIndex={0}
          >
            <CoralIcon aria-hidden="true" size={22} />
          </span>
        </Tooltip>
        <IconButton
          aria-controls={PRIMARY_NAVIGATION_ID}
          aria-expanded={!isCollapsed}
          ariaLabel={toggleLabel}
          name={toggleIconName}
          onClick={toggleSidebar}
          size="22"
          tooltipText={toggleLabel}
          variant="bare"
        />
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
