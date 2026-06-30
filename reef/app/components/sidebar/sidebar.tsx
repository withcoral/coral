import classNames from 'classnames'
import { NavLink, useLocation } from 'react-router'

import { KeyboardShortcut } from '@/wax/components/keyboard-shortcut'
import { IconButton } from '@/wax/components/button'
import { Icon } from '@/wax/components/icon'
import { SidebarButton } from '@/wax/components/sidebar-button/sidebar-button'
import { Typography } from '@/wax/components/typography'
import type { IconName } from '@/wax/components/icon'

import * as styles from './sidebar.css'
import { useSidebarState } from './use-sidebar-state'

interface SidebarProps {
  initialIsMinimized: boolean
}

const NAV_ITEMS = [
  { icon: 'Plug', label: 'Sources', paths: ['/', '/sources'], to: '/sources' },
  { icon: 'Table2', label: 'Schema', paths: ['/schema'], to: '/schema' },
  { icon: 'Activity', label: 'Traces', paths: ['/traces'], to: '/traces' },
] satisfies Array<{ icon: IconName; label: string; paths: string[]; to: string }>

export function Sidebar({ initialIsMinimized }: SidebarProps) {
  const location = useLocation()
  const { isMinimized, toggleSidebar } = useSidebarState(initialIsMinimized)
  const isSettingsActive = location.pathname === '/settings'

  const handleToggleSidebar = (event: KeyboardEvent) => {
    event.preventDefault()
    toggleSidebar()
  }

  return (
    <nav
      aria-label="Coral"
      className={classNames(styles.sidebar, { [styles.sidebarMinimized]: isMinimized })}
      data-sidebar-minimized={isMinimized}
    >
      <div className={styles.header}>
        {isMinimized && (
          <div className={styles.toggleButton}>
            <KeyboardShortcut
              handler={handleToggleSidebar}
              shortcut="$mod+b"
              tooltipContent="Expand sidebar"
              tooltipSide="right"
            >
              <IconButton
                ariaLabel="Expand sidebar"
                name="PanelLeft"
                onClick={toggleSidebar}
                size="32"
                variant="bare"
              />
            </KeyboardShortcut>
          </div>
        )}

        <div className={styles.brandRow}>
          <div className={styles.brandMark}>
            <Icon color="inherit" name="Coral" size="18" />
          </div>
          {!isMinimized && <Typography.Body className={styles.brandLabel}>Coral</Typography.Body>}

          {!isMinimized && (
            <div className={styles.toggleButton}>
              <KeyboardShortcut
                handler={handleToggleSidebar}
                shortcut="$mod+b"
                tooltipContent="Collapse sidebar"
                tooltipSide="right"
              >
                <IconButton
                  ariaLabel="Collapse sidebar"
                  name="PanelLeft"
                  onClick={toggleSidebar}
                  size="32"
                  variant="bare"
                />
              </KeyboardShortcut>
            </div>
          )}
        </div>
      </div>

      <div className={styles.nav}>
        {NAV_ITEMS.map((item) => {
          const isActive = item.paths.includes(location.pathname)

          return (
            <SidebarButton
              aria-label={item.label}
              as={NavLink}
              icon={item.icon}
              isActive={isActive}
              isMinimized={isMinimized}
              key={item.label}
              to={item.to}
            >
              {item.label}
            </SidebarButton>
          )
        })}
      </div>

      <div className={styles.footer}>
        <SidebarButton
          aria-label="Settings"
          as={NavLink}
          icon="Settings"
          isActive={isSettingsActive}
          isMinimized={isMinimized}
          to="/settings"
        >
          Settings
        </SidebarButton>
      </div>
    </nav>
  )
}
