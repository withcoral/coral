import classNames from 'classnames'
import { useEffect, useRef } from 'react'

import type { Route } from '@/hooks/useHashRouter'
import type { IconName } from '@/wax/components/icon'
import { IconButton } from '@/wax/components/button'
import { Icon } from '@/wax/components/icon'
import { Tooltip } from '@/wax/components/tooltip'

import { NavItem } from './nav-item'
import * as styles from './sidebar.css'
import { useSidebarState } from './use-sidebar-state'

const NAV_ITEMS: { route: Route; icon: IconName; label: string }[] = [
  { route: 'schema-explorer', icon: 'Database', label: 'Schema explorer' },
]

interface SidebarProps {
  currentRoute: Route
  onNavigate: (route: Route) => void
}

export function Sidebar({ currentRoute, onNavigate }: SidebarProps) {
  const { isMinimized, toggleSidebar } = useSidebarState()
  const containerRef = useRef<HTMLElement>(null)

  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'b') {
        e.preventDefault()
        toggleSidebar()
      }
    }
    window.addEventListener('keydown', handleKeyDown)
    return () => window.removeEventListener('keydown', handleKeyDown)
  }, [toggleSidebar])

  return (
    <nav
      className={classNames(styles.sidebar, { [styles.sidebarMinimized]: isMinimized })}
      data-sidebar-minimized={isMinimized}
      ref={containerRef}
    >
      <div className={styles.header}>
        <div className={styles.brandButton}>
          <Icon name="Coral" size="22" />
          {!isMinimized && <span className={styles.brandName}>Coral</span>}
        </div>
        <div className={styles.toggleButton}>
          <Tooltip content={isMinimized ? 'Expand sidebar (⌘B)' : 'Collapse sidebar (⌘B)'} side="right">
            <IconButton
              ariaLabel={isMinimized ? 'Expand sidebar' : 'Collapse sidebar'}
              name="PanelLeft"
              onClick={toggleSidebar}
              size="32"
              variant="bare"
            />
          </Tooltip>
        </div>
      </div>

      <div className={styles.nav}>
        {NAV_ITEMS.map((item) => (
          <NavItem
            key={item.route}
            icon={item.icon}
            isActive={currentRoute === item.route}
            isMinimized={isMinimized}
            onClick={() => onNavigate(item.route)}
          >
            {item.label}
          </NavItem>
        ))}
      </div>
    </nav>
  )
}
