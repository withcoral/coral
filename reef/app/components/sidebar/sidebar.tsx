import classNames from 'classnames'

import { KeyboardShortcut } from '@/wax/components/keyboard-shortcut'
import { IconButton } from '@/wax/components/button'
import { Icon } from '@/wax/components/icon'
import { Typography } from '@/wax/components/typography'

import * as styles from './sidebar.css'
import { useSidebarState } from './use-sidebar-state'

interface SidebarProps {
  initialIsMinimized: boolean
}

export function Sidebar({ initialIsMinimized }: SidebarProps) {
  const { isMinimized, toggleSidebar } = useSidebarState(initialIsMinimized)

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

      <div className={styles.nav} />

      <div className={styles.footer} />
    </nav>
  )
}
