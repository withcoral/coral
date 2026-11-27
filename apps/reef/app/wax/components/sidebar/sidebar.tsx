import classNames from 'classnames'
import { useState, type ReactNode } from 'react'
import { Link, useLocation, type To } from 'react-router'

import { Brand } from '@/wax/components/brand'
import * as Button from '@/wax/components/button'
import { KeyboardShortcut } from '@/wax/components/keyboard-shortcut'
import {
  NavigationClose,
  NavigationFrame,
} from '@/wax/components/navigation-frame/navigation-frame'

import * as styles from './sidebar.css'

/** How the sidebar shows now, which its contents follow. */
export interface SidebarState {
  /** The sidebar is a rail of icons. The drawer on a phone always shows the labels. */
  isMinimized: boolean
  /** The sidebar is the drawer on a phone. */
  mobile: boolean
}

interface SidebarProps {
  /** Names the app beside the brand, such as "console". The landmark and the brand's link take the name too. */
  brandLabel?: string
  /** Where the brand goes. */
  home: To
  /** The person's choice. The app keeps it, so the next load shows the same sidebar. */
  isMinimized: boolean
  /** The person collapsed or expanded the sidebar, with the toggle or its shortcut. */
  onToggle: () => void
  /** The pages and the menus under the masthead. */
  children: (state: SidebarState) => ReactNode
}

/**
 * The navigation of an app, for the `navigation` of an `AppFrame`: a masthead
 * with the brand and the collapse toggle, above the app's own pages and menus.
 * On a desktop it stays beside the page and collapses to a rail of icons. On a
 * phone it is a drawer that the page's top bar opens.
 */
export function Sidebar({ brandLabel, home, isMinimized, onToggle, children }: SidebarProps) {
  const location = useLocation()
  // Only a toggle animates the width, so a page load shows the saved state at once.
  const [hasToggled, setHasToggled] = useState(false)
  const toggle = () => {
    setHasToggled(true)
    onToggle()
  }
  return (
    <NavigationFrame navigationKey={location.key}>
      {(mobile) => (
        <Navigation
          brandLabel={brandLabel}
          home={home}
          isMinimized={!mobile && isMinimized}
          mobile={mobile}
          resized={!mobile && hasToggled}
          toggle={toggle}
        >
          {children}
        </Navigation>
      )}
    </NavigationFrame>
  )
}

function Navigation({
  brandLabel,
  home,
  isMinimized,
  mobile,
  resized,
  toggle,
  children,
}: Pick<SidebarProps, 'brandLabel' | 'home' | 'children'> &
  SidebarState & { resized: boolean; toggle: () => void }) {
  const label = brandLabel ? `Coral ${brandLabel}` : 'Coral'
  const toggleLabel = isMinimized ? 'Expand sidebar' : 'Collapse sidebar'
  return (
    <nav
      aria-label={label}
      className={classNames(
        styles.sidebar,
        isMinimized && styles.minimized,
        resized && styles.resized,
        mobile && styles.drawer,
      )}
      data-sidebar-minimized={isMinimized}
    >
      <div className={classNames(styles.masthead, isMinimized && styles.mastheadMinimized)}>
        <Link
          to={home}
          className={classNames(styles.identity, isMinimized && styles.identityMinimized)}
          aria-label={label}
        >
          <Brand compact={isMinimized} label={brandLabel} />
        </Link>
        {mobile ? (
          <NavigationClose />
        ) : (
          <KeyboardShortcut
            handler={(event) => {
              event.preventDefault()
              toggle()
            }}
            shortcut="$mod+Backslash"
            tooltipContent={toggleLabel}
            tooltipSide={isMinimized ? 'right' : 'bottom'}
          >
            <Button.IconButton
              ariaLabel={toggleLabel}
              name="PanelLeft"
              onClick={toggle}
              size="32"
              variant="bare"
            />
          </KeyboardShortcut>
        )}
      </div>
      {children({ isMinimized, mobile })}
    </nav>
  )
}
