import classNames from 'classnames'
import { Fragment, useRef, useState } from 'react'
import { Form, Link, useLocation } from 'react-router'
import type { BrowserAuth } from '@/auth/types'
import { Button, Menu, ScrollArea } from '@/wax/components'
import { Avatar } from '@/wax/components/avatar'
import { Brand } from '@/wax/components/brand'
import type { IconName } from '@/wax/components/icon'
import { KeyboardShortcut } from '@/wax/components/keyboard-shortcut'
import { SidebarBadge, SidebarButton } from '@/wax/components/sidebar-button/sidebar-button'
import {
  NavigationClose,
  NavigationFrame,
} from '@/wax/components/navigation-frame/navigation-frame'
import { useCopy } from '@/wax/components/button/use-copy'
import { useTheme } from '@/wax/theme/theme-provider'
import { ActionDialog, NameInput } from '@/views/management/forms'
import type { FormContext, WorkspaceView } from '@/views/management/types'
import type { ActionCount } from '@/lib/activity-live-types'
import { settingsNav } from '@/components/settings/settings-nav'
import { useReturnTo } from '@/components/settings/use-return-to'
import { SettingsSidebar } from './settings-sidebar'
import * as styles from './sidebar.css'
import { useSidebarState } from './use-sidebar-state'

/** The numbers a badge shows. The sidebar has no use for revisions. */
type WorkspaceCount = Omit<ActionCount, 'revisions'>

export type SidebarWorkspace = Pick<
  WorkspaceView,
  'displayName' | 'path' | 'effectiveRole' | 'mcpEndpoint' | 'etag' | 'state'
>

interface SidebarProps {
  /** Pending actions by Workspace path. Null or a null count shows no badge. */
  actionCounts?: Record<string, WorkspaceCount | null> | null
  /** A published Coral release newer than this one. The shell passes it only to organization owners. */
  newerRelease?: NewerRelease
  auth?: BrowserAuth
  /**
   * In the organization's settings, their nav replaces the workspace's switcher
   * and pages. The shell decides which, from the route it matched.
   */
  area?: 'work' | 'settings'
  /** Decides which sections the organization's settings show. */
  organizationOwner?: boolean
  /** False when the deployment turned secret stores off. */
  secretStores?: boolean
  userName?: string
  initialIsMinimized: boolean
  workspaces?: SidebarWorkspace[]
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

// The help menu's links open outside Coral; Desktop hands new-window links to
// the browser. The Slack link redirects to the community Slack's invite.
const helpLinks: { href: string; icon: IconName; label: string }[] = [
  {
    href: 'https://withcoral.com/docs',
    icon: 'BookOpen',
    label: 'Read the docs',
  },
  {
    href: 'https://withcoral.com/slack',
    icon: 'Slack',
    label: 'Chat on Slack',
  },
  {
    href: 'https://github.com/withcoral/coral',
    icon: 'GitHub',
    label: 'Star on GitHub',
  },
  {
    href: 'https://github.com/withcoral/coral/issues/new',
    icon: 'MessageSquare',
    label: 'Give feedback',
  },
]

/** Undefined shows no badge: a zero count, or a Workspace without a known count. An
 * unknown count is never reported as zero. `unit` names what the count counts. */
function pending(count: string | undefined, unit: string): Pending | undefined {
  if (!count || count === '0') return undefined
  return {
    badge: count.length > 3 ? '999+' : count,
    text: `${count} pending ${unit}${count === '1' ? '' : 's'}`,
  }
}

/** The pending actions in every Workspace but the current one, which shows its own on its pages. */
function pendingElsewhere(
  workspaces: SidebarWorkspace[],
  current: SidebarWorkspace | undefined,
  counts: SidebarProps['actionCounts'],
): Pending | undefined {
  let sum = 0n
  for (const w of workspaces) {
    const total = w.path === current?.path ? undefined : counts?.[w.path]?.total
    if (total && /^\d+$/.test(total)) sum += BigInt(total)
  }
  const count = pending(String(sum), 'action')
  return count && current ? { ...count, text: `${count.text} in other workspaces` } : count
}

/**
 * Copies one Workspace's MCP URL from its row in the switcher's menu. With a
 * pointer it takes the avatar's place while you point at the row. The menu
 * stays open, and the item's icon and name say that the copy happened. It is an
 * item of its own beside the Workspace's, so the arrow keys reach it.
 */
function CopyEndpointItem({ workspace: w }: { workspace: SidebarWorkspace }) {
  const [copied, copy] = useCopy()
  const name = `Copy ${w.displayName} MCP URL`
  return (
    <Menu.Item
      icon={copied ? 'Check' : 'Copy'}
      ariaLabel={copied ? `${name}: copied` : name}
      tooltip={copied ? 'Copied' : 'Copy MCP URL'}
      closeOnClick={false}
      className={styles.menuRowAction}
      onClick={() => void copy(w.mcpEndpoint)}
    />
  )
}

/** The accessible name carries the count the badge shows. */
function named(label: string, count?: Pending) {
  return count ? `${label}. ${count.text}` : label
}

function workspaceDestinations(
  workspace: SidebarWorkspace,
  people: boolean,
  count?: WorkspaceCount | null,
): Destination[] {
  const owner = workspace.effectiveRole === 'owner'
  const w = workspace.path
  // Stable Run links are Activity detail, outside the Activity path.
  const activity: Destination = {
    to: `${w}/activity`,
    icon: 'Activity',
    label: 'Activity',
    also: `${w}/runs/`,
  }
  // An archived Workspace has only its history to show.
  if (workspace.state === 'archived') return [activity]
  return [
    { to: w, icon: 'Home', label: 'Overview', exact: true },
    {
      to: `${w}/approvals`,
      icon: 'ShieldCheck',
      label: 'Approvals',
      pending: pending(count?.approvals, 'approval'),
    },
    activity,
    // Each provider's own pages sit under its resource path, outside Tools.
    { to: `${w}/tools`, icon: 'Wrench', label: 'Tools', also: `${w}/providers/` },
    {
      to: `${w}/accounts`,
      icon: 'KeyRound',
      label: 'Accounts',
      pending: pending(count?.connections, 'connection request'),
    },
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
  newerRelease,
  auth = { mode: 'disabled' },
  area = 'work',
  organizationOwner = false,
  secretStores = true,
  userName,
  workspaces = [],
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
  // Without a name there are no initials to show, so the account keeps the user icon.
  const name = auth.mode === 'required' ? userName?.trim() : undefined
  const accountLabel = auth.mode === 'disabled' ? 'Local user' : name || 'Account'
  const people = auth.mode === 'required'
  const inSettings = area === 'settings'
  const reader = { organizationOwner, people, secretStores }
  // A tab that opened on the settings goes back home.
  const back = useReturnTo(inSettings, '/')
  // The brand opens the Overview of the workspace you are in. In the settings
  // that is the workspace you came from. Home picks one when there is none.
  const cameFrom = inSettings ? /^\/workspaces\/[^/?#]+/.exec(back)?.[0] : undefined
  const home = workspace?.path ?? cameFrom ?? '/'
  const logout = useRef<HTMLFormElement>(null)
  // You open a Workspace only as its member, so every entry here is one of yours.
  // The route's Workspace is fresher than the list, and stands in when the list
  // failed to load. The switcher lists only active Workspaces, so one you
  // archive while you have it open leaves the list.
  const active = !workspace
    ? workspaces
    : workspace.state === 'archived'
      ? workspaces.filter((w) => w.path !== workspace.path)
      : workspaces.some((w) => w.path === workspace.path)
        ? workspaces.map((w) => (w.path === workspace.path ? workspace : w))
        : [workspace, ...workspaces]
  const isCurrent = (d: Destination) =>
    d.exact
      ? location.pathname === d.to
      : location.pathname.startsWith(d.to) ||
        Boolean(d.also && location.pathname.startsWith(d.also))
  // The current Workspace's pages. A page outside any Workspace shows none.
  const destinations = workspace && (
    <div
      role="group"
      aria-label={workspace.displayName}
      className={isMinimized ? styles.railDestinations : styles.destinations}
    >
      {workspaceDestinations(workspace, people, actionCounts?.[workspace.path]).map((d) => (
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
      ))}
    </div>
  )
  const navigation = inSettings ? (
    <SettingsSidebar reader={reader} back={back} isMinimized={isMinimized} />
  ) : (
    destinations
  )
  // An owner's notice of a newer release leads the footer, above the help menu.
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
          to={home}
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
      <div className={isMinimized ? styles.rail : styles.workspace}>
        {!inSettings && (
          <WorkspaceSwitcher
            workspace={workspace}
            active={active}
            actionCounts={actionCounts}
            form={form}
            isMinimized={isMinimized}
          />
        )}
        {isMinimized ? (
          navigation
        ) : (
          // Only the pages scroll, so the switcher and the footer stay put.
          <ScrollArea.Container
            className={styles.destinationsScroll}
            constrainWidth
            height="auto"
            scrollDirection="vertical"
            width="calc(100% + 12px)"
          >
            {navigation}
          </ScrollArea.Container>
        )}
      </div>
      <div className={styles.footer}>
        {release}
        <Menu.Container>
          <Menu.Trigger
            render={
              <SidebarButton
                aria-label="Help and feedback"
                icon="CircleQuestionMark"
                isMinimized={isMinimized}
                title="Help and feedback"
              />
            }
          >
            Help and feedback
          </Menu.Trigger>
          <Menu.Content align={isMinimized ? 'end' : 'start'} side={isMinimized ? 'right' : 'top'}>
            {helpLinks.map((link) => (
              <Menu.Item key={link.href} href={link.href} icon={link.icon}>
                {link.label}
              </Menu.Item>
            ))}
          </Menu.Content>
        </Menu.Container>
        <Menu.Container>
          <Menu.Trigger
            render={
              <SidebarButton
                aria-label={`Open account menu for ${accountLabel}`}
                icon={name ? <Avatar name={name} letters={2} /> : 'User'}
                isMinimized={isMinimized}
                title={accountLabel}
              />
            }
          >
            {accountLabel}
          </Menu.Trigger>
          <Menu.Content align={isMinimized ? 'end' : 'start'} side={isMinimized ? 'right' : 'top'}>
            <AppearanceMenu />
            <Menu.Separator />
            {/* Every member can read the usage data; an organization owner administers the rest. */}
            <Menu.Item to={settingsNav(reader)[0].to} icon="Settings">
              Settings
            </Menu.Item>
            <Menu.Separator />
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

/**
 * Names the current Workspace and opens the menu that switches to another.
 * Each row also copies its Workspace's MCP URL, and the menu creates a Workspace.
 */
function WorkspaceSwitcher({
  workspace,
  active,
  actionCounts,
  form,
  isMinimized,
}: {
  workspace?: SidebarWorkspace
  active: SidebarWorkspace[]
  actionCounts: SidebarProps['actionCounts']
  form?: FormContext
  isMinimized: boolean
}) {
  const location = useLocation()
  const trigger = useRef<HTMLButtonElement>(null)
  const [createAt, setCreateAt] = useState<string | null>(null)
  // This sidebar survives redirects. A completed creation must close its dialog,
  // and Back must not reopen a form whose request has already committed.
  if (createAt !== null && createAt !== location.key) setCreateAt(null)
  const setCreate = (open: boolean) => setCreateAt(open ? location.key : null)
  const currentArchived = workspace?.state === 'archived'
  // The other Workspaces sit behind the menu, so the trigger carries their count.
  const elsewhere = pendingElsewhere(active, workspace, actionCounts)
  const label = named(
    workspace
      ? `Workspace: ${workspace.displayName}${currentArchived ? ', archived' : ''}`
      : 'Workspaces',
    elsewhere,
  )
  const avatar = (w: SidebarWorkspace, size: '20' | '28') => (
    <Avatar name={w.displayName} seed={w.path} shape="square" size={size} />
  )
  const menuAvatar = (w: SidebarWorkspace) => (
    <span className={styles.menuAvatar}>{avatar(w, '20')}</span>
  )
  // Only an active Workspace shows its avatar. An archived one shows the archive's icon.
  const shown = currentArchived ? undefined : workspace
  const icon: IconName = currentArchived ? 'Archive' : 'Folders'
  const sections = [
    active.length > 0 && (
      <Menu.RadioGroup key="active" ariaLabel="Workspaces" value={workspace?.path ?? ''}>
        {active.map((w) => {
          // The current Workspace shows its counts on its pages; the others here.
          const count =
            w.path === workspace?.path
              ? undefined
              : pending(actionCounts?.[w.path]?.total, 'action')
          return (
            // One row: the Workspace, then the action that copies its MCP URL.
            <div key={w.path} role="none" className={styles.menuRow}>
              <Menu.RadioItem
                as={Link}
                to={w.path}
                value={w.path}
                aria-label={named(w.displayName, count)}
                className={styles.menuRowItem}
                leading={menuAvatar(w)}
                trailing={
                  <span className={styles.menuCount}>
                    <SidebarBadge badge={count?.badge} />
                  </span>
                }
              >
                {w.displayName}
              </Menu.RadioItem>
              {w.mcpEndpoint && <CopyEndpointItem workspace={w} />}
            </div>
          )
        })}
      </Menu.RadioGroup>
    ),
    form && (
      <Menu.Item key="create" icon="Plus" onClick={() => setCreate(true)}>
        Create workspace
      </Menu.Item>
    ),
  ].filter((section): section is React.ReactElement => Boolean(section))

  return (
    <>
      <Menu.Container>
        <Menu.Trigger
          render={
            <SidebarButton
              ref={trigger}
              icon={shown ? avatar(shown, isMinimized ? '28' : '20') : icon}
              trailingIcon="ChevronDown"
              badge={elsewhere?.badge}
              variant={shown ? 'default' : 'muted'}
              isMinimized={isMinimized}
              aria-label={label}
              title={label}
              className={isMinimized ? styles.railTile : styles.switcher}
            />
          }
        >
          {workspace?.displayName ?? 'Workspaces'}
        </Menu.Trigger>
        <Menu.Content align="start" side={isMinimized ? 'right' : 'bottom'} className={styles.menu}>
          {sections.map((section, index) => (
            <Fragment key={section.key}>
              {index > 0 && <Menu.Separator />}
              {section}
            </Fragment>
          ))}
        </Menu.Content>
      </Menu.Container>
      {form && (
        <ActionDialog
          open={createAt === location.key}
          onOpenChange={setCreate}
          finalFocus={trigger}
          title="Create workspace"
          description="Create a workspace to manage providers and run programs. You become its first owner."
          form={form}
          intent="createWorkspace"
          action="/workspaces"
        >
          <NameInput label="Workspace name" />
        </ActionDialog>
      )}
    </>
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
