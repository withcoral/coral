import classNames from 'classnames'
import { useId, useRef, useState } from 'react'
import { Form, Link, useLocation } from 'react-router'
import type { BrowserAuth } from '@/auth/types'
import { Button, Menu, ScrollArea } from '@/wax/components'
import { Avatar } from '@/wax/components/avatar'
import { Brand } from '@/wax/components/brand'
import { Icon } from '@/wax/components/icon'
import type { IconName } from '@/wax/components/icon'
import { KeyboardShortcut } from '@/wax/components/keyboard-shortcut'
import { SidebarBadge, SidebarButton } from '@/wax/components/sidebar-button/sidebar-button'
import {
  NavigationClose,
  NavigationFrame,
} from '@/wax/components/navigation-frame/navigation-frame'
import { useTheme } from '@/wax/theme/theme-provider'
import { ActionDialog, NameInput } from '@/views/management/forms'
import { ArchiveWorkspaceButton } from '@/views/management/archive-workspace'
import { RestoreWorkspaceButton } from '@/views/management/restore-workspace'
import type { FormContext, WorkspaceView } from '@/views/management/types'
import type { ActionCounts } from '@/lib/activity-live-types'
import * as styles from './sidebar.css'
import { useSidebarState } from './use-sidebar-state'

export type SidebarWorkspace = Pick<
  WorkspaceView,
  'displayName' | 'path' | 'effectiveRole' | 'mcpEndpoint' | 'etag' | 'state'
>

interface SidebarProps {
  /** Pending actions by Workspace path. Null or a null count shows no badge. */
  actionCounts?: ActionCounts | null
  organizationOwner?: boolean
  /** A published Coral release newer than this one. The shell passes it only to organization owners. */
  newerRelease?: NewerRelease
  auth?: BrowserAuth
  userName?: string
  initialIsMinimized: boolean
  workspaces?: SidebarWorkspace[]
  /** Archived Workspaces you are a member of. Each opens on its Activity. */
  archivedWorkspaces?: SidebarWorkspace[]
  workspace?: SidebarWorkspace
  form?: FormContext
}

export interface NewerRelease {
  version: string
  notesUrl: string
}

interface Destination {
  to: string
  icon: IconName
  label: string
  /** Only the exact path is current; child paths belong to other destinations. */
  exact?: boolean
  /** Another path prefix this destination owns. */
  also?: string
  pending?: Pending
}

interface Pending {
  badge: string
  /** The count in words, for the accessible name. */
  text: string
}

// Both open outside Coral; Desktop hands new-window links to the browser. The
// Slack link signs the person in with Slack and invites them to a shared
// channel with the Coral team.
const externalLinks: { href: string; icon: IconName; label: string }[] = [
  {
    href: 'https://slack.withcoral.com/connect',
    icon: 'Slack',
    label: 'Chat on Slack',
  },
  {
    href: 'https://github.com/withcoral/coral',
    icon: 'GitHub',
    label: 'Star on GitHub',
  },
]

/** Undefined shows no badge: a zero count, or a Workspace without a known count. An
 * unknown count has nothing for you to do, and the Activity page reports it. */
function pending(count: string | null | undefined): Pending | undefined {
  if (!count || count === '0') return undefined
  return {
    badge: count.length > 3 ? '999+' : count,
    text: `${count} pending ${count === '1' ? 'action' : 'actions'}`,
  }
}

/** The accessible name carries the count the badge shows. */
function named(label: string, count?: Pending) {
  return count ? `${label}. ${count.text}` : label
}

function workspaceDestinations(
  workspace: SidebarWorkspace,
  people: boolean,
  count?: Pending,
): Destination[] {
  const owner = workspace.effectiveRole === 'owner'
  const w = workspace.path
  return [
    { to: w, icon: 'Home', label: 'Overview', exact: true },
    // Stable Run links are Activity detail, outside the Activity path.
    {
      to: `${w}/activity`,
      icon: 'Activity',
      label: 'Activity',
      also: `${w}/runs/`,
      pending: count,
    },
    { to: `${w}/providers`, icon: 'Plug', label: 'Providers' },
    { to: `${w}/accounts`, icon: 'KeyRound', label: 'Accounts' },
    { to: `${w}/functions`, icon: 'SquareFunction', label: 'Functions' },
    ...(owner ? [{ to: `${w}/agents`, icon: 'Bot' as const, label: 'Agents' }] : []),
    // A local deployment has no other people to manage.
    ...(people ? [{ to: `${w}/members`, icon: 'Users' as const, label: 'Members' }] : []),
  ]
}

export function Sidebar({ initialIsMinimized, ...props }: SidebarProps) {
  const { isMinimized, toggleSidebar, hasToggled } = useSidebarState(initialIsMinimized)
  const location = useLocation()
  return (
    <NavigationFrame navigationKey={location.key}>
      {(mobile) => (
        <SidebarNavigation
          {...props}
          isMinimized={!mobile && isMinimized}
          toggleSidebar={toggleSidebar}
          resized={!mobile && hasToggled}
          mobile={mobile}
        />
      )}
    </NavigationFrame>
  )
}

function SidebarNavigation({
  actionCounts,
  organizationOwner,
  newerRelease,
  auth = { mode: 'disabled' },
  userName,
  workspaces = [],
  archivedWorkspaces = [],
  workspace,
  form,
  isMinimized,
  toggleSidebar,
  resized,
  mobile,
}: Omit<SidebarProps, 'initialIsMinimized'> & {
  isMinimized: boolean
  toggleSidebar: () => void
  resized: boolean
  mobile: boolean
}) {
  const location = useLocation()
  const accountLabel = auth.mode === 'disabled' ? 'Local user' : userName?.trim() || 'Account'
  const people = auth.mode === 'required'
  const logout = useRef<HTMLFormElement>(null)
  const create = useRef<HTMLButtonElement>(null)
  // Desktop and the mobile drawer can mount this navigation together.
  const treeId = useId()
  const [createAt, setCreateAt] = useState<string | null>(null)
  // This sidebar survives redirects. A completed creation must close its dialog,
  // and Back must not reopen a form whose request has already committed.
  if (createAt !== null && createAt !== location.key) setCreateAt(null)
  const setCreate = (open: boolean) => setCreateAt(open ? location.key : null)
  // Several Workspaces can be open. Navigating into a Workspace opens it; the
  // headers toggle it without navigating.
  const [openPaths, setOpenPaths] = useState<ReadonlySet<string>>(
    () => new Set(workspace ? [workspace.path] : []),
  )
  const [openFor, setOpenFor] = useState(workspace?.path)
  if (openFor !== workspace?.path) {
    setOpenFor(workspace?.path)
    if (workspace && !openPaths.has(workspace.path))
      setOpenPaths(new Set(openPaths).add(workspace.path))
  }
  const toggle = (path: string) => {
    const next = new Set(openPaths)
    if (!next.delete(path)) next.add(path)
    setOpenPaths(next)
  }
  // You open a Workspace only as its member, so every entry here is one of yours.
  // The route's Workspace is fresher than the lists, and stands in when they
  // failed to load. Its state decides its group, even while you have it open.
  const currentArchived = workspace?.state === 'archived'
  const place = (list: SidebarWorkspace[], here: boolean) =>
    !workspace
      ? list
      : !here
        ? list.filter((w) => w.path !== workspace.path)
        : list.some((w) => w.path === workspace.path)
          ? list.map((w) => (w.path === workspace.path ? workspace : w))
          : [workspace, ...list]
  const tree = place(workspaces, !currentArchived)
  const archived = place(archivedWorkspaces, currentArchived)
  // The group opens when you open an archived Workspace, or archive the one you are in.
  const currentArchivedPath = currentArchived ? workspace?.path : undefined
  const [archivedOpen, setArchivedOpen] = useState(currentArchived)
  const [archivedOpenFor, setArchivedOpenFor] = useState(currentArchivedPath)
  if (archivedOpenFor !== currentArchivedPath) {
    setArchivedOpenFor(currentArchivedPath)
    if (currentArchivedPath) setArchivedOpen(true)
  }
  const archivedLabel = currentArchived
    ? `Archived: ${workspace?.displayName}`
    : 'Archived workspaces'
  const isCurrent = (d: Destination) =>
    d.exact
      ? location.pathname === d.to
      : location.pathname.startsWith(d.to) ||
        Boolean(d.also && location.pathname.startsWith(d.also))
  const countFor = (w: SidebarWorkspace) => pending(actionCounts?.[w.path])
  const links = (destinations: Destination[]) =>
    destinations.map((d) => (
      <SidebarButton
        key={d.to}
        as={Link}
        to={d.to}
        icon={d.icon}
        badge={d.pending?.badge}
        aria-label={named(d.label, d.pending)}
        title={named(d.label, d.pending)}
        isMinimized={isMinimized}
        isActive={isCurrent(d)}
        aria-current={isCurrent(d) ? 'page' : undefined}
      >
        {d.label}
      </SidebarButton>
    ))
  // An owner's notice of a newer release joins the help links, so it keeps their spacing.
  const release = newerRelease && (
    <SidebarButton
      as="a"
      href={newerRelease.notesUrl}
      target="_blank"
      rel="noreferrer"
      icon="CircleArrowUp"
      variant="accent"
      isMinimized={isMinimized}
      aria-label={`Coral ${newerRelease.version} is available`}
      title={`Coral ${newerRelease.version} is available`}
      className={classNames(isMinimized && styles.minimizedControl)}
    >
      Coral {newerRelease.version} is available
    </SidebarButton>
  )

  return (
    <nav
      aria-label="Coral"
      className={classNames(
        styles.sidebar,
        isMinimized && styles.sidebarMinimized,
        resized && styles.sidebarResized,
        mobile && styles.drawerSidebar,
      )}
      data-sidebar-minimized={isMinimized}
    >
      <div className={classNames(styles.header, isMinimized && styles.minimizedColumn)}>
        <Link
          to="/"
          className={classNames(styles.identity, isMinimized && styles.minimizedControl)}
          aria-label="Coral"
        >
          <Brand compact={isMinimized} />
        </Link>
        {mobile ? (
          <NavigationClose />
        ) : (
          <KeyboardShortcut
            handler={(event) => {
              event.preventDefault()
              toggleSidebar()
            }}
            shortcut="$mod+Backslash"
            tooltipContent={isMinimized ? 'Expand sidebar' : 'Collapse sidebar'}
            tooltipSide={isMinimized ? 'right' : 'bottom'}
          >
            <Button.IconButton
              ariaLabel={isMinimized ? 'Expand sidebar' : 'Collapse sidebar'}
              name="PanelLeft"
              onClick={toggleSidebar}
              size="32"
              variant="bare"
            />
          </KeyboardShortcut>
        )}
      </div>
      {isMinimized && (
        <div className={styles.rail} role="group" aria-label="Workspaces">
          {tree.map((w) => {
            const current = w.path === workspace?.path
            const count = countFor(w)
            // The current Workspace shows its count on Activity; the others on their tile.
            const tileCount = current ? undefined : count
            return (
              <div key={w.path} className={styles.railWorkspace}>
                <Link
                  to={w.path}
                  aria-label={named(w.displayName, tileCount)}
                  title={named(w.displayName, tileCount)}
                  className={classNames(styles.railTile, current && styles.railTileCurrent)}
                >
                  <Avatar name={w.displayName} seed={w.path} shape="square" size="28" />
                  <SidebarBadge badge={tileCount?.badge} isMinimized />
                </Link>
                {current && (
                  <div className={styles.railDestinations}>
                    {links(workspaceDestinations(w, people, count))}
                  </div>
                )}
              </div>
            )
          })}
          {/* One tile holds every archived Workspace. It shows as current in any of them. */}
          {archived.length > 0 && (
            <div className={styles.railWorkspace}>
              <SidebarButton
                icon="Archive"
                aria-label={archivedLabel}
                title={archivedLabel}
                aria-current={currentArchived ? 'page' : undefined}
                isMinimized
                className={classNames(currentArchived && styles.railTileCurrent)}
                onClick={() => {
                  setArchivedOpen(true)
                  toggleSidebar()
                }}
              >
                Archived
              </SidebarButton>
            </div>
          )}
        </div>
      )}
      {!isMinimized && (
        <div className={styles.tree} role="group" aria-labelledby={`${treeId}-label`}>
          <div className={styles.groupLabel}>
            <span id={`${treeId}-label`}>Workspaces</span>
            {form && (
              <Button.IconButton
                ref={create}
                ariaLabel="Create workspace"
                name="Plus"
                onClick={() => setCreate(true)}
                size="22"
                variant="bare"
              />
            )}
          </div>
          <ScrollArea.Container
            className={styles.treeScroll}
            constrainWidth
            height="auto"
            scrollDirection="vertical"
            surface="main"
            width="calc(100% + 12px)"
          >
            <div className={styles.treeList}>
              {tree.map((w, index) => {
                const open = openPaths.has(w.path)
                const id = `${treeId}-${index}`
                const owner = w.effectiveRole === 'owner'
                const actions = (Number(Boolean(w.mcpEndpoint)) + Number(owner)) as 0 | 1 | 2
                const count = countFor(w)
                // An open Workspace shows its count on Activity; a closed one on its name.
                const headerCount = open ? undefined : count
                return (
                  <div key={w.path} className={styles.workspace}>
                    <div className={styles.workspaceHeader}>
                      <SidebarButton
                        id={`${id}-header`}
                        icon={open ? 'ChevronDown' : 'ChevronRight'}
                        aria-expanded={open}
                        aria-controls={open ? id : undefined}
                        aria-label={named(w.displayName, headerCount)}
                        title={named(w.displayName, headerCount)}
                        badge={headerCount?.badge}
                        className={classNames(
                          styles.headerButton,
                          styles.headerActionsSpace[actions],
                        )}
                        isActive={!open && w.path === workspace?.path}
                        onClick={() => toggle(w.path)}
                      >
                        {w.displayName}
                      </SidebarButton>
                      {actions > 0 && (
                        <div className={styles.workspaceActions}>
                          {w.mcpEndpoint && (
                            <Button.CopyButton
                              ariaLabel={`Copy ${w.displayName} workspace MCP URL`}
                              textToCopy={w.mcpEndpoint}
                              tooltipText="Copy workspace MCP URL"
                              size="22"
                              variant="bare"
                            />
                          )}
                          {owner && form && <ArchiveWorkspaceButton workspace={w} form={form} />}
                        </div>
                      )}
                    </div>
                    {open && (
                      <div
                        id={id}
                        role="group"
                        aria-labelledby={`${id}-header`}
                        className={styles.workspaceChildren}
                      >
                        {links(workspaceDestinations(w, people, count))}
                      </div>
                    )}
                  </div>
                )
              })}
              {archived.length > 0 && (
                <div className={styles.archived}>
                  <SidebarButton
                    id={`${treeId}-archived`}
                    icon={archivedOpen ? 'ChevronDown' : 'ChevronRight'}
                    aria-expanded={archivedOpen}
                    aria-controls={archivedOpen ? `${treeId}-archived-list` : undefined}
                    variant="muted"
                    onClick={() => setArchivedOpen(!archivedOpen)}
                  >
                    Archived
                  </SidebarButton>
                  {archivedOpen && (
                    <div
                      id={`${treeId}-archived-list`}
                      role="group"
                      aria-labelledby={`${treeId}-archived`}
                      className={styles.workspaceChildren}
                    >
                      {archived.map((w) => {
                        const owner = w.effectiveRole === 'owner'
                        const current = w.path === workspace?.path
                        return (
                          <div key={w.path} className={styles.workspaceHeader}>
                            <SidebarButton
                              as={Link}
                              to={`${w.path}/activity`}
                              icon="Archive"
                              aria-label={`${w.displayName}, archived`}
                              title={w.displayName}
                              isActive={current}
                              variant="muted"
                              aria-current={current ? 'page' : undefined}
                              className={classNames(
                                styles.headerButton,
                                styles.headerActionsSpace[owner ? 1 : 0],
                              )}
                            >
                              {w.displayName}
                            </SidebarButton>
                            {owner && (
                              <div className={styles.workspaceActions}>
                                <RestoreWorkspaceButton workspace={w} form={form} compact />
                              </div>
                            )}
                          </div>
                        )
                      })}
                    </div>
                  )}
                </div>
              )}
            </div>
          </ScrollArea.Container>
        </div>
      )}
      {form && (
        <ActionDialog
          open={createAt === location.key}
          onOpenChange={setCreate}
          finalFocus={create}
          title="Create workspace"
          description="Create a workspace to manage providers and run programs. You become its first owner."
          form={form}
          intent="createWorkspace"
          action="/workspaces"
        >
          <NameInput label="Workspace name" />
        </ActionDialog>
      )}
      {!isMinimized && (
        <div className={styles.externalLinks}>
          {release}
          {externalLinks.map((link) => (
            <SidebarButton
              key={link.href}
              as="a"
              href={link.href}
              target="_blank"
              rel="noreferrer"
              icon={link.icon}
              aria-label={link.label}
              title={link.label}
            >
              {link.label}
            </SidebarButton>
          ))}
        </div>
      )}
      <div className={classNames(styles.footer, isMinimized && styles.minimizedColumn)}>
        {isMinimized && release}
        {isMinimized && (
          <Menu.Container>
            <Menu.Trigger
              render={
                <Button.Container
                  ariaLabel="Get help"
                  title="Get help"
                  variant="bare"
                  className={styles.minimizedControl}
                />
              }
            >
              <Icon name="CircleQuestionMark" size="18" />
            </Menu.Trigger>
            <Menu.Content align="end" side="right">
              {externalLinks.map((link) => (
                <Menu.Item key={link.href} href={link.href} icon={link.icon}>
                  {link.label}
                </Menu.Item>
              ))}
            </Menu.Content>
          </Menu.Container>
        )}
        <Menu.Container>
          <Menu.Trigger
            render={
              <Button.Container
                ariaLabel={`Open account menu for ${accountLabel}`}
                title={accountLabel}
                variant="bare"
                className={classNames(
                  styles.accountTrigger,
                  isMinimized ? styles.minimizedControl : styles.footerAccount,
                )}
              />
            }
          >
            <Icon name="User" size="18" />
            {!isMinimized && <Button.Text>{accountLabel}</Button.Text>}
          </Menu.Trigger>
          <Menu.Content align="start" side="top">
            <AppearanceMenu />
            <Menu.Separator />
            {organizationOwner && (
              <>
                {people && (
                  <Menu.Item to="/organization/members" icon="Users">
                    Members
                  </Menu.Item>
                )}
                <Menu.Item to="/organization/workspaces" icon="Folders">
                  Workspaces
                </Menu.Item>
                <Menu.Separator />
              </>
            )}
            {auth.mode === 'required' ? (
              <Form action="/logout" method="post" ref={logout}>
                <input name="csrf" type="hidden" value={auth.csrfToken} />
                <Menu.Item icon="LogOut" onClick={() => logout.current?.requestSubmit()}>
                  Sign out
                </Menu.Item>
              </Form>
            ) : (
              <Menu.Item disabled>Local user</Menu.Item>
            )}
          </Menu.Content>
        </Menu.Container>
      </div>
    </nav>
  )
}

const appearanceOptions = [
  { value: 'system', label: 'System' },
  { value: 'light', label: 'Light' },
  { value: 'dark', label: 'Dark' },
] as const

function AppearanceMenu() {
  const { setTheme, themePreference } = useTheme()

  return (
    <Menu.Submenu>
      <Menu.SubmenuTrigger icon="SunMoon">Appearance</Menu.SubmenuTrigger>
      <Menu.SubmenuContent>
        <Menu.RadioGroup onValueChange={setTheme} value={themePreference}>
          {appearanceOptions.map((option) => (
            <Menu.RadioItem closeOnClick={false} key={option.value} value={option.value}>
              {option.label}
            </Menu.RadioItem>
          ))}
        </Menu.RadioGroup>
      </Menu.SubmenuContent>
    </Menu.Submenu>
  )
}
