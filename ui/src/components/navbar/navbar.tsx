import { useCallback } from 'react'

import type { IconName } from '@/wax/components/icon'
import { IconButton } from '@/wax/components/button'
import { CoralIcon } from '@/wax/components/icon/custom-icons/coral'
import { KeyboardShortcut } from '@/wax/components/keyboard-shortcut'
import { SidebarButton } from '@/wax/components/sidebar-button/sidebar-button'
import { Tooltip } from '@/wax/components/tooltip'
import { useRouter, type Route } from '@/lib/router'

import * as styles from './navbar.css'
import { useSidebarState } from './use-sidebar-state'

interface NavItem {
  icon: IconName
  label: string
  target: Route
  matches: Route['kind'][]
}

const NAV_ITEMS: NavItem[] = [
  { icon: 'Activity', label: 'Traces', target: { kind: 'traces' }, matches: ['traces'] },
  { icon: 'Plug', label: 'Sources', target: { kind: 'sources' }, matches: ['sources'] },
]

const QUERY_STREAM_LABEL = 'Query stream'
const COLLAPSE_SIDEBAR_LABEL = 'Collapse sidebar'
const EXPAND_SIDEBAR_LABEL = 'Expand sidebar'
const PRIMARY_NAVIGATION_ID = 'primary-navigation'

function renderBrandMark(isCollapsed: boolean) {
  const brandMark = (
    <span aria-label={QUERY_STREAM_LABEL} className={styles.brandMark} role="img">
      <CoralIcon aria-hidden="true" size={22} />
    </span>
  )

  if (!isCollapsed) return brandMark

  return (
    <Tooltip content={QUERY_STREAM_LABEL} side="right">
      {brandMark}
    </Tooltip>
  )
}

function renderNavItem(
  item: NavItem,
  isCollapsed: boolean,
  isActive: boolean,
  onSelect: (target: Route) => void,
) {
  const button = (
    <SidebarButton
      aria-current={isActive ? 'page' : undefined}
      disabled={isActive}
      icon={item.icon}
      isActive={isActive}
      isMinimized={isCollapsed}
      key={item.label}
      onClick={() => onSelect(item.target)}
    >
      {item.label}
    </SidebarButton>
  )

  if (!isCollapsed) return button

  return (
    <Tooltip content={item.label} key={item.label} side="right">
      <span className={styles.navItemTooltipTrigger}>{button}</span>
    </Tooltip>
  )
}

export function Navbar() {
  const { isCollapsed, shouldHideSidebarToggle, toggleSidebar } = useSidebarState()
  const { location, navigate } = useRouter()

  const toggleLabel = isCollapsed ? EXPAND_SIDEBAR_LABEL : COLLAPSE_SIDEBAR_LABEL
  const handleSidebarShortcut = useCallback(
    (event: KeyboardEvent) => {
      event.preventDefault()
      toggleSidebar()
    },
    [toggleSidebar],
  )
  const onSelect = useCallback((target: Route) => navigate({ route: target }), [navigate])

  return (
    <nav className={styles.navbar({ isCollapsed })} aria-label="Coral">
      <div className={styles.header({ isCollapsed })}>
        <div className={styles.brandIdentity({ isCollapsed })}>
          {renderBrandMark(isCollapsed)}
          {!isCollapsed && <span className={styles.brandName}>Coral</span>}
        </div>
        {!shouldHideSidebarToggle && (
          <div className={styles.toggleSlot}>
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
          </div>
        )}
      </div>
      <div className={styles.nav} aria-label="Primary navigation" id={PRIMARY_NAVIGATION_ID}>
        {NAV_ITEMS.map((item) =>
          renderNavItem(item, isCollapsed, item.matches.includes(location.route.kind), onSelect),
        )}
      </div>
    </nav>
  )
}
