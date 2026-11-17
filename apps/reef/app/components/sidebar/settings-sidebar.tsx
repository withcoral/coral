import { Link, useLocation } from 'react-router'
import {
  currentSettingsItem,
  settingsNav,
  type SettingsReader,
} from '@/components/settings/settings-nav'
import { Typography } from '@/wax/components'
import { SidebarButton } from '@/wax/components/sidebar-button/sidebar-button'
import * as styles from './sidebar.css'

/**
 * The sidebar while you are in the organization's settings. It takes the place
 * of the workspace's switcher and pages: the way back to the app, then the
 * organization's sections.
 */
export function SettingsSidebar({
  reader,
  back,
  isMinimized,
}: {
  reader: SettingsReader
  /** The page you opened the settings from. */
  back: string
  isMinimized: boolean
}) {
  const location = useLocation()
  const nav = settingsNav(reader)
  const current = currentSettingsItem(nav, location.pathname)
  return (
    <div className={isMinimized ? styles.railDestinations : styles.settingsNav}>
      <SidebarButton
        as={Link}
        to={back}
        icon="ChevronLeft"
        variant="muted"
        isMinimized={isMinimized}
        aria-label="Back to workspace"
        title="Back to workspace"
      >
        Back to workspace
      </SidebarButton>
      {/* Someone who is not an organization owner has no section: Coral
          refused the page they opened. */}
      {nav.length > 0 && (
        <div role="group" aria-label="Organization" className={styles.settingsGroup}>
          {!isMinimized && (
            <Typography.BodySmall className={styles.settingsGroupLabel}>
              Organization
            </Typography.BodySmall>
          )}
          {nav.map((item) => (
            <SidebarButton
              key={item.id}
              as={Link}
              to={item.to}
              icon={item.icon}
              aria-label={item.label}
              title={item.label}
              isMinimized={isMinimized}
              isActive={item.id === current?.id}
              aria-current={item.id === current?.id ? 'page' : undefined}
            >
              {item.label}
            </SidebarButton>
          ))}
        </div>
      )}
    </div>
  )
}
