import { useCallback, useEffect, useState } from 'react'

import { breakpoints } from '@/styles/theme'
import type { IconName } from '@/wax/components/icon'
import { IconButton } from '@/wax/components/button'
import { CoralIcon } from '@/wax/components/icon/custom-icons/coral'
import { KeyboardShortcut } from '@/wax/components/keyboard-shortcut'
import { SidebarButton } from '@/wax/components/sidebar-button/sidebar-button'
import * as styles from './navbar.css'

interface NavItem {
  icon: IconName
  isActive?: boolean
  label: string
}

const NAV_ITEMS: NavItem[] = [
  { icon: 'Activity', isActive: true, label: 'Traces' },
]

const QUERY_STREAM_LABEL = 'Query stream'
const COLLAPSE_SIDEBAR_LABEL = 'Collapse sidebar'
const EXPAND_SIDEBAR_LABEL = 'Expand sidebar'
const PRIMARY_NAVIGATION_ID = 'primary-navigation'
const SIDEBAR_COLLAPSE_QUERY = `(max-width: ${breakpoints.sidebarCollapse})`
const MOBILE_QUERY = `(max-width: ${breakpoints.mobile})`

function useMediaQuery(query: string) {
  const [matches, setMatches] = useState(() =>
    typeof window !== 'undefined' && window.matchMedia(query).matches
  )

  useEffect(() => {
    if (typeof window === 'undefined') return

    const mediaQuery = window.matchMedia(query)
    const handleChange = () => setMatches(mediaQuery.matches)

    handleChange()
    mediaQuery.addEventListener('change', handleChange)
    return () => mediaQuery.removeEventListener('change', handleChange)
  }, [query])

  return matches
}

export function Navbar() {
  const shouldCollapseForViewport = useMediaQuery(SIDEBAR_COLLAPSE_QUERY)
  const shouldHideSidebarToggle = useMediaQuery(MOBILE_QUERY)
  const [isCollapsed, setIsCollapsed] = useState(() => shouldCollapseForViewport)

  useEffect(() => {
    if (shouldCollapseForViewport) setIsCollapsed(true)
  }, [shouldCollapseForViewport])

  const toggleLabel = isCollapsed ? EXPAND_SIDEBAR_LABEL : COLLAPSE_SIDEBAR_LABEL
  const toggleSidebar = useCallback(() => setIsCollapsed((value) => !value), [])
  const handleSidebarShortcut = useCallback((event: KeyboardEvent) => {
    event.preventDefault()
    toggleSidebar()
  }, [toggleSidebar])

  return (
    <nav className={styles.navbar({ isCollapsed })} aria-label="Coral">
      <div className={styles.header({ isCollapsed })}>
        <span aria-label={QUERY_STREAM_LABEL} className={styles.brandMark} role="img">
          <CoralIcon aria-hidden="true" size={22} />
        </span>
        {!shouldHideSidebarToggle && (
          <KeyboardShortcut
            handler={handleSidebarShortcut}
            shortcut="mod+b"
            tooltipContent={toggleLabel}
            tooltipSide="right"
          >
            <IconButton
              aria-controls={PRIMARY_NAVIGATION_ID}
              aria-expanded={!isCollapsed}
              ariaLabel={toggleLabel}
              name="PanelLeft"
              onClick={toggleSidebar}
              size="32"
              variant="bare"
            />
          </KeyboardShortcut>
        )}
      </div>
      <div className={styles.nav} aria-label="Primary navigation" id={PRIMARY_NAVIGATION_ID}>
        {NAV_ITEMS.map((item) => (
          <SidebarButton
            aria-current={item.isActive ? 'page' : undefined}
            disabled={item.isActive}
            icon={item.icon}
            isActive={item.isActive}
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
