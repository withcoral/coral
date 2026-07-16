import classNames from 'classnames'
import { Link, NavLink, useLocation, useParams } from 'react-router'

import { KeyboardShortcut } from '@/wax/components/keyboard-shortcut'
import { IconButton } from '@/wax/components/button'
import { Icon } from '@/wax/components/icon'
import { SidebarButton } from '@/wax/components/sidebar-button/sidebar-button'
import { Tooltip } from '@/wax/components/tooltip'
import { Typography } from '@/wax/components/typography'
import type { IconName } from '@/wax/components/icon'
import * as Menu from '@/wax/components/menu'
import { getAvatarColorFromSeed } from '@/wax/components/avatar/utils/get-avatar-color'
import type { Workspace } from '@/generated/coral/v1/resources_pb'
import { isCoralDesktopBuild } from '@/lib/coral-desktop'
import { workspacePathForCurrentSection } from '@/lib/workspace-routing'
import { routePath } from '@/routing/routemap'

import * as styles from './sidebar.css'
import { useSidebarState } from './use-sidebar-state'

interface SidebarProps {
  initialIsMinimized: boolean
  workspaces: ReadonlyArray<Pick<Workspace, 'name'>>
}

export function Sidebar({ initialIsMinimized, workspaces }: SidebarProps) {
  const location = useLocation()
  const { workspaceId } = useParams()
  const { isMinimized, toggleSidebar } = useSidebarState(initialIsMinimized)
  const currentWorkspace = workspaces.find((workspace) => workspace.name === workspaceId)
  const workspaceNavTarget = currentWorkspace ?? workspaces[0]
  const workspaceSelectorLabel = workspaceNavTarget?.name ?? 'Coral'
  const workspaceSelectorMarkColor = getAvatarColorFromSeed(workspaceSelectorLabel)
  const sourcesPath = workspaceNavTarget
    ? routePath('workspaceSources', { workspaceId: workspaceNavTarget.name })
    : routePath('home')
  const schemaPath = workspaceNavTarget
    ? routePath('workspaceSchema', { workspaceId: workspaceNavTarget.name })
    : routePath('home')
  const tracesPath = workspaceNavTarget
    ? routePath('workspaceTraces', { workspaceId: workspaceNavTarget.name })
    : routePath('home')
  const navItems = [
    { icon: 'Plug', label: 'Sources', paths: [routePath('home'), sourcesPath], to: sourcesPath },
    { icon: 'Database', label: 'Schema', paths: [schemaPath], to: schemaPath },
    { icon: 'Activity', label: 'Traces', paths: [tracesPath], to: tracesPath },
  ] satisfies Array<{ icon: IconName; label: string; paths: string[]; to: string }>
  const isConnectActive =
    location.pathname === routePath('connect') ||
    location.pathname.startsWith(`${routePath('connect')}/`)
  // Keep the desktop-only connect link stable across SSR and hydration. The
  // actual bridge can only be detected on the client, but the route is included
  // at build time for Electron and omitted from the web build.
  const isDesktopApp = isCoralDesktopBuild()

  const connectButton = (
    <SidebarButton
      aria-label="Connect"
      as={NavLink}
      icon="Cable"
      isActive={isConnectActive}
      isMinimized={isMinimized}
      to={routePath('connect')}
    >
      Connect
    </SidebarButton>
  )

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

        <div className={styles.workspaceSelectorRow}>
          <Menu.Container>
            <Menu.Trigger
              className={styles.workspaceSelector}
              render={<button aria-label="Open workspace menu" type="button" />}
            >
              <span className={styles.workspaceSelectorMark({ color: workspaceSelectorMarkColor })}>
                <Icon color="inherit" name="Coral" size="18" />
              </span>
              {!isMinimized && (
                <>
                  <Tooltip content={workspaceSelectorLabel} showOnlyWhenTruncated side="bottom">
                    <span className={styles.workspaceSelectorLabel}>
                      <Typography.Body>{workspaceSelectorLabel}</Typography.Body>
                    </span>
                  </Tooltip>
                  <span className={styles.workspaceSelectorChevron}>
                    <Icon color="tertiary" name="ChevronDown" size="16" />
                  </span>
                </>
              )}
            </Menu.Trigger>
            <Menu.Content align="start" side="bottom" sideOffset={6}>
              <Menu.Group>
                <Menu.GroupLabel>Workspaces</Menu.GroupLabel>
                {workspaces.length === 0 ? (
                  <Menu.Item disabled>No workspaces</Menu.Item>
                ) : (
                  <Menu.RadioGroup value={workspaceNavTarget?.name}>
                    {workspaces.map((workspace) => (
                      <Menu.RadioItem
                        as={Link}
                        key={workspace.name}
                        to={workspacePathForCurrentSection(workspace.name, location.pathname)}
                        value={workspace.name}
                      >
                        {workspace.name}
                      </Menu.RadioItem>
                    ))}
                  </Menu.RadioGroup>
                )}
              </Menu.Group>
            </Menu.Content>
          </Menu.Container>

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
        {navItems.map((item) => {
          const isActive = item.paths.some(
            (path) => location.pathname === path || location.pathname.startsWith(`${path}/`),
          )
          const button = (
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

          // Collapsed sidebar hides the label — surface it on hover instead.
          return isMinimized ? (
            <Tooltip content={item.label} key={item.label} side="right">
              {button}
            </Tooltip>
          ) : (
            button
          )
        })}
      </div>

      <div className={styles.footer}>
        {isDesktopApp &&
          // Collapsed sidebar hides the label, so surface it on hover instead.
          (isMinimized ? (
            <Tooltip content="Connect" side="right">
              {connectButton}
            </Tooltip>
          ) : (
            connectButton
          ))}
      </div>
    </nav>
  )
}
