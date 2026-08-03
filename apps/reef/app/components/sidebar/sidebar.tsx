import classNames from 'classnames'
import { useEffect, useRef, useState } from 'react'
import { Link, NavLink, useLocation, useMatch, useParams } from 'react-router'

import { KeyboardShortcut } from '@/wax/components/keyboard-shortcut'
import {
  Container as ButtonContainer,
  Icon as ButtonIcon,
  IconButton,
} from '@/wax/components/button'
import { Icon } from '@/wax/components/icon'
import { SidebarButton } from '@/wax/components/sidebar-button/sidebar-button'
import { Tooltip } from '@/wax/components/tooltip'
import { Typography } from '@/wax/components/typography'
import type { IconName } from '@/wax/components/icon'
import * as Menu from '@/wax/components/menu'
import { getAvatarColorFromSeed } from '@/wax/components/avatar/utils/get-avatar-color'
import type { Workspace } from '@/generated/coral/v1/resources_pb'
import {
  DesktopUpdateIndicator,
  useDesktopUpdateState,
} from '@/components/desktop-update-indicator'
import { WorkspaceCreationDialog } from '@/components/workspaces'
import { isCoralDesktopBuild } from '@/lib/coral-desktop'
import { workspacePathForCurrentSection } from '@/lib/workspace-routing'
import { routePath } from '@/routing/routemap'

import * as styles from './sidebar.css'
import { useSidebarState } from './use-sidebar-state'

interface SidebarProps {
  initialIsMinimized: boolean
  workspaces: ReadonlyArray<Pick<Workspace, 'name'>>
}

type NavItem = { icon: IconName; label: string; paths: string[]; to: string }

export function Sidebar({ initialIsMinimized, workspaces }: SidebarProps) {
  const location = useLocation()
  const { workspaceId } = useParams()
  const { isMinimized, toggleSidebar } = useSidebarState(initialIsMinimized)
  const desktop = isCoralDesktopBuild()
  const updateState = useDesktopUpdateState(desktop)
  const [createWorkspaceDialogOpen, setCreateWorkspaceDialogOpen] = useState(false)
  const createWorkspaceDialogSession = useRef(0)
  const createWorkspaceFetcherKey = `create-workspace-${createWorkspaceDialogSession.current}`
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
  const functionsPath = workspaceNavTarget
    ? routePath('workspaceFunctions', { workspaceId: workspaceNavTarget.name })
    : routePath('home')
  const tracesPath = workspaceNavTarget
    ? routePath('workspaceTraces', { workspaceId: workspaceNavTarget.name })
    : routePath('home')
  const workspaceNavItems = [
    { icon: 'Plug', label: 'Sources', paths: [routePath('home'), sourcesPath], to: sourcesPath },
    { icon: 'Database', label: 'Schema', paths: [schemaPath], to: schemaPath },
    { icon: 'Braces', label: 'Functions', paths: [functionsPath], to: functionsPath },
    { icon: 'Activity', label: 'Traces', paths: [tracesPath], to: tracesPath },
  ] satisfies NavItem[]
  const settingsPath = routePath('settings')
  const isSettingsRoute = Boolean(useMatch({ end: false, path: settingsPath }))
  const settingsNavItems: NavItem[] = desktop
    ? [{ icon: 'Settings', label: 'MCP Clients', paths: [settingsPath], to: settingsPath }]
    : []
  const navItems = isSettingsRoute ? settingsNavItems : workspaceNavItems
  const settingsHomeButton = (
    <ButtonContainer ariaLabel="Home" as={Link} size="22" to={routePath('home')} variant="bare">
      <ButtonIcon name="ChevronLeft" />
    </ButtonContainer>
  )
  const settingsHeader = (
    <>
      {isMinimized ? (
        <Tooltip content="Home" side="right">
          {settingsHomeButton}
        </Tooltip>
      ) : (
        settingsHomeButton
      )}
      {!isMinimized && (
        <span className={styles.workspaceSelectorLabel}>
          <Typography.Body>Settings</Typography.Body>
        </span>
      )}
    </>
  )

  // Temporary: re-entry point into the onboarding flow.
  const onboardingButton = (
    <SidebarButton
      aria-label="Onboarding"
      as={Link}
      icon="Sparkles"
      isMinimized={isMinimized}
      to={routePath('onboarding')}
    >
      Onboarding
    </SidebarButton>
  )

  const handleToggleSidebar = (event: KeyboardEvent) => {
    event.preventDefault()
    toggleSidebar()
  }

  useEffect(() => {
    setCreateWorkspaceDialogOpen(false)
  }, [location.key])

  const handleCreateWorkspaceDialogOpenChange = (open: boolean) => {
    if (open) createWorkspaceDialogSession.current += 1
    setCreateWorkspaceDialogOpen(open)
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
          {isSettingsRoute ? (
            settingsHeader
          ) : (
            <>
              <Menu.Container>
                <Menu.Trigger
                  className={styles.workspaceSelector}
                  render={
                    <ButtonContainer ariaLabel="Open workspace menu" size="32" variant="bare" />
                  }
                >
                  <span
                    className={styles.workspaceSelectorMark({ color: workspaceSelectorMarkColor })}
                  >
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
                  <Menu.Separator />
                  <Menu.Item
                    icon="Plus"
                    onClick={() => handleCreateWorkspaceDialogOpenChange(true)}
                  >
                    Create workspace
                  </Menu.Item>
                  <Menu.Separator />
                  <Menu.Item icon="Settings" to={settingsPath}>
                    Settings
                  </Menu.Item>
                </Menu.Content>
              </Menu.Container>

              <WorkspaceCreationDialog
                fetcherKey={createWorkspaceFetcherKey}
                onOpenChange={handleCreateWorkspaceDialogOpenChange}
                open={createWorkspaceDialogOpen}
              />
            </>
          )}

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
      {isMinimized ? (
        <Tooltip content="Onboarding" side="right">
          {onboardingButton}
        </Tooltip>
      ) : (
        onboardingButton
      )}
      {updateState.status !== 'idle' && updateState.status !== 'unsupported' && (
        <DesktopUpdateIndicator isMinimized={isMinimized} state={updateState} />
      )}
    </nav>
  )
}
