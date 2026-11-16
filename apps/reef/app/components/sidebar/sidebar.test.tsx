// @vitest-environment happy-dom

import { act, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { useLocation } from 'react-router'
import { expect, onTestFinished, test } from 'vitest'
import { renderRoute } from '@/test-support/route'
import type { BrowserAuth } from '@/auth/types'
import { Sidebar, type SidebarWorkspace } from './sidebar'
import { SIDEBAR_COOKIE_NAME } from './sidebar-state'

const signedIn: BrowserAuth = { mode: 'required', csrfToken: 'test-csrf' }
const listed: SidebarWorkspace[] = [
  workspace('ops', 'Operations', 'owner'),
  workspace('research', 'Research', 'member'),
]

const researchLinks = [
  '/workspaces/research',
  '/workspaces/research/approvals',
  '/workspaces/research/activity',
  '/workspaces/research/tools',
  '/workspaces/research/accounts',
  '/workspaces/research/members',
]

// Catches: the sidebar keeping one workspace's pages after you switch to another.
test('the sidebar shows the current workspace, and the switcher opens another', async () => {
  const { container } = render('/workspaces/ops/tools')
  expect(switcher()).toHaveAccessibleName('Workspace: Operations')
  expect(links(container)).toEqual(ownerLinks('ops'))

  await userEvent.click(switcher())
  expect(screen.getByRole('menuitemradio', { name: 'Operations' })).toBeChecked()
  const research = screen.getByRole('menuitemradio', { name: 'Research' })
  expect(research).not.toBeChecked()
  await userEvent.click(research)
  await waitFor(() => expect(switcher()).toHaveAccessibleName('Workspace: Research'))
  expect(links(container)).toEqual(researchLinks)
})

test('a member workspace hides owner-only destinations', async () => {
  const { container } = render('/workspaces/research')
  expect(links(container)).not.toContain('/workspaces/research/agents')
})

// Catches: the organization's pages, which have no workspace, showing one's pages.
test('with no workspace open, the switcher lists the workspaces and shows no pages', async () => {
  const { container } = render('/')
  expect(switcher()).toHaveAccessibleName('Workspaces')
  expect(links(container)).toEqual([])
  await userEvent.click(switcher())
  const items = screen.getAllByRole('menuitemradio')
  expect(items.map((item) => [item.getAttribute('aria-label'), item.getAttribute('href')])).toEqual(
    [
      ['Operations', '/workspaces/ops'],
      ['Research', '/workspaces/research'],
    ],
  )
  for (const item of items) expect(item).not.toBeChecked()
})

// Catches: a row copying another workspace's URL, the menu closing before you
// see that the copy happened, or what changes a workspace coming back beside the
// list you switch from.
test('each workspace row in the switcher copies its own MCP URL', async () => {
  render('/workspaces/ops')
  let copied = ''
  const clipboard = Object.getOwnPropertyDescriptor(navigator, 'clipboard')
  Object.defineProperty(navigator, 'clipboard', {
    configurable: true,
    value: { writeText: async (text: string) => void (copied = text) },
  })
  try {
    await userEvent.click(switcher())
    // The switcher is for switching: every other command is a row's copy, or creates a workspace.
    expect(
      screen
        .getAllByRole('menuitem')
        .map((item) => item.getAttribute('aria-label') ?? item.textContent),
    ).toEqual(['Copy Operations MCP URL', 'Copy Research MCP URL', 'Create workspace'])
    await userEvent.click(screen.getByRole('menuitem', { name: 'Copy Research MCP URL' }))
    expect(
      await screen.findByRole('menuitem', { name: 'Copy Research MCP URL: copied' }),
    ).toBeInTheDocument()
  } finally {
    if (clipboard) Object.defineProperty(navigator, 'clipboard', clipboard)
    else delete (navigator as { clipboard?: Clipboard }).clipboard
  }
  expect(copied).toBe('https://coral.example.com/mcp/research')
  // The menu stays open, and you stay in the workspace you were in.
  expect(switcher()).toHaveAccessibleName('Workspace: Operations')
})

// Catches: the current workspace vanishing when the shell could not list workspaces.
test('an open active workspace missing from the lists still appears', async () => {
  const { container } = render('/workspaces/unlisted', {
    extra: workspace('unlisted', 'Unlisted', 'owner'),
  })
  expect(switcher()).toHaveAccessibleName('Workspace: Unlisted')
  expect(links(container)).toEqual(ownerLinks('unlisted'))
  await userEvent.click(switcher())
  expect(screen.getByRole('menuitemradio', { name: 'Unlisted' })).toBeChecked()
})

// Catches: an open archived workspace showing pages it cannot open, or the
// switcher listing it among the workspaces you can work in.
test.each([false, true])(
  'an open archived workspace shows as archived, with only its activity (collapsed: %s)',
  async (minimized) => {
    const { container } = render('/workspaces/old/activity', {
      extra: workspace('old', 'Old ops', 'owner', 'archived'),
      minimized,
    })
    expect(switcher()).toHaveAccessibleName('Workspace: Old ops, archived')
    expect(links(container)).toEqual(['/workspaces/old/activity'])
    expect(activityLink()).toHaveAttribute('aria-current', 'page')
    await userEvent.click(switcher())
    expect(
      screen.getAllByRole('menuitemradio').map((item) => item.getAttribute('aria-label')),
    ).toEqual(['Operations', 'Research'])
    for (const item of screen.getAllByRole('menuitemradio')) expect(item).not.toBeChecked()
    // An archived workspace takes no new calls, so the menu offers no endpoint for it.
    expect(screen.queryByRole('menuitem', { name: /^Copy Old ops/ })).toBeNull()
  },
)

// Catches: archiving the workspace you are in keeping the pages it can no longer
// open, or keeping it in the switcher's list.
test('archiving the current workspace leaves it open as archived', async () => {
  const ops = workspace('ops', 'Operations', 'owner')
  const options: RenderOptions = { active: [ops, listed[1]] }
  const { container, router } = render('/workspaces/ops/activity', options)
  // The archive revalidates the shell's list and the route; the path stays.
  options.active = [listed[1]]
  options.extra = { ...ops, state: 'archived' }
  await act(async () => router.navigate('/workspaces/ops/activity', { replace: true }))
  expect(switcher()).toHaveAccessibleName('Workspace: Operations, archived')
  expect(links(container)).toEqual(['/workspaces/ops/activity'])
  await userEvent.click(switcher())
  expect(
    screen.getAllByRole('menuitemradio').map((item) => item.getAttribute('aria-label')),
  ).toEqual(['Research'])
})

// Catches: Settings opening on a section a member cannot open. Who sees which
// section is the settings nav module's rule, tested there.
test('the account menu opens the settings on the first section you can open', async () => {
  render('/workspaces/ops/approvals')
  await userEvent.click(screen.getByRole('button', { name: /^Open account menu/ }))
  expect(await screen.findByRole('menuitem', { name: 'Settings' })).toHaveAttribute(
    'href',
    '/usage-data',
  )
})

// Catches: the organization's settings keeping the workspace's switcher and pages.
test('in the organization settings their nav takes the place of the switcher and the pages', () => {
  render('/organization/workspaces', { area: 'settings', organizationOwner: true })
  expect(screen.queryByRole('button', { name: /^(Workspace: |Workspaces)/ })).toBeNull()
  expect(screen.queryByRole('link', { name: 'Tools' })).toBeNull()
  const organization = within(screen.getByRole('group', { name: 'Organization' }))
  expect(organization.getByRole('link', { name: 'Workspaces' })).toHaveAttribute(
    'aria-current',
    'page',
  )
})

const brand = () => screen.getByRole('link', { name: 'Coral' })

// Catches: the brand taking you to another workspace than the one you are in,
// or than the one you opened the settings from.
test('the brand opens the Overview of the workspace you are in', async () => {
  sessionStorage.clear()
  onTestFinished(() => sessionStorage.clear())
  const options: RenderOptions = {}
  const { router } = render('/workspaces/research/approvals', options)
  expect(brand()).toHaveAttribute('href', '/workspaces/research')
  options.area = 'settings'
  await act(async () => router.navigate('/usage-data'))
  expect(brand()).toHaveAttribute('href', '/workspaces/research')
  options.area = 'work'
  await act(async () => router.navigate('/'))
  expect(brand()).toHaveAttribute('href', '/')
})

// Catches: "Back to workspace" dropping you on another page than the one you left,
// or leading nowhere in a tab that opened on the settings.
test('Back to workspace returns to the page you opened the settings from', async () => {
  sessionStorage.clear()
  onTestFinished(() => sessionStorage.clear())
  const settings: RenderOptions = { area: 'settings' }
  const opened = render('/usage-data', settings)
  expect(screen.getByRole('link', { name: 'Back to workspace' })).toHaveAttribute('href', '/')
  opened.unmount()

  const options: RenderOptions = {}
  const { router } = render('/workspaces/research/approvals?review=r1', options)
  options.area = 'settings'
  await act(async () => router.navigate('/usage-data'))
  expect(screen.getByRole('link', { name: 'Back to workspace' })).toHaveAttribute(
    'href',
    '/workspaces/research/approvals?review=r1',
  )
})

// Catches: the collapsed rail losing the switcher's menu or the workspace's pages.
test('collapsed, the switcher is one tile above the current workspace pages', async () => {
  const { container } = render('/workspaces/ops/tools', { minimized: true })
  expect(links(container)).toEqual(ownerLinks('ops'))
  await userEvent.click(switcher())
  expect(screen.getByRole('menuitemradio', { name: 'Research' })).toHaveAttribute(
    'href',
    '/workspaces/research',
  )
})

test('the mod+backslash shortcut collapses and expands the sidebar', async () => {
  // Toggling saves the preference, which later renders would read.
  onTestFinished(clearSidebarCookie)
  render('/workspaces/ops')
  await userEvent.keyboard('{Control>}[Backslash]{/Control}')
  expect(screen.getByRole('button', { name: 'Expand sidebar' })).toBeInTheDocument()
  await userEvent.keyboard('{Control>}[Backslash]{/Control}')
  expect(screen.getByRole('button', { name: 'Collapse sidebar' })).toBeInTheDocument()
})

test('the sidebar preference holds for the next load', async () => {
  onTestFinished(clearSidebarCookie)
  const reload = (view: ReturnType<typeof render>) => {
    view.unmount()
    return render('/workspaces/ops')
  }
  let view = render('/workspaces/ops')
  await userEvent.click(screen.getByRole('button', { name: 'Collapse sidebar' }))
  view = reload(view)
  await userEvent.click(screen.getByRole('button', { name: 'Expand sidebar' }))
  reload(view)
  expect(screen.getByRole('button', { name: 'Collapse sidebar' })).toBeInTheDocument()
})

test.each([false, true])(
  'the help menu holds the external links (collapsed: %s)',
  async (minimized) => {
    render('/workspaces/ops', { minimized })
    expect(screen.queryByRole('link', { name: 'Chat on Slack' })).not.toBeInTheDocument()
    await userEvent.click(screen.getByRole('button', { name: 'Help and feedback' }))
    const items = ['Read the docs', 'Chat on Slack', 'Star on GitHub', 'Give feedback'].map(
      (name) => screen.getByRole('menuitem', { name }),
    )
    expect(items.map((a) => [a.getAttribute('href'), a.getAttribute('target')])).toEqual([
      ['https://withcoral.com/docs', '_blank'],
      ['https://withcoral.com/slack', '_blank'],
      ['https://github.com/withcoral/coral', '_blank'],
      ['https://github.com/withcoral/coral/issues/new', '_blank'],
    ])
  },
)

// Catches: a local deployment, where nobody else can sign in, offering a
// workspace's members to manage.
test.each<[string, boolean, BrowserAuth]>([
  ['a signed-in deployment', true, signedIn],
  ['a local deployment', false, { mode: 'disabled' }],
])('%s shows the workspace members: %s', (_, shown, auth) => {
  const { container } = render('/workspaces/ops', { auth })
  expect(links(container).includes('/workspaces/ops/members')).toBe(shown)
})

test('the account menu keeps the chosen appearance for the next load', async () => {
  onTestFinished(() => localStorage.clear())
  render('/workspaces/ops')
  await userEvent.click(screen.getByRole('button', { name: /^Open account menu/ }))
  await userEvent.click(screen.getByRole('menuitem', { name: 'Appearance' }))
  expect(await screen.findByRole('menuitemradio', { name: 'System' })).toBeChecked()
  await userEvent.click(screen.getByRole('menuitemradio', { name: 'Light' }))
  expect(screen.getByRole('menuitemradio', { name: 'Light' })).toBeChecked()
  // The pre-hydration script in root.tsx reads this key before the first paint.
  expect(localStorage.getItem('coral:theme')).toBe('"light"')
})

// Catches: initials made from a placeholder, such as "LU" for the local user.
// Which letters a name gives is the avatar's rule, shown in its story.
test.each<[string, RenderOptions, string, string | null]>([
  ['a named user', { userName: 'James Summerfield' }, 'James Summerfield', 'JS'],
  ['a user without a profile', {}, 'Account', null],
  ['the local user', { auth: { mode: 'disabled' } }, 'Local user', null],
])('the account button shows initials only for %s', (_, options, label, initials) => {
  render('/workspaces/ops', options)
  const account = screen.getByRole('button', { name: `Open account menu for ${label}` })
  expect(account.textContent).toBe(`${initials ?? ''}${label}`)
})

const counts = {
  '/workspaces/ops': { total: '5', approvals: '3', connections: '2' },
  '/workspaces/research': { total: '5', approvals: '1', connections: '4' },
}

function activityLink() {
  return screen.getByRole('link', { name: /^Activity/ })
}
function accountsLink() {
  return screen.getByRole('link', { name: /^Accounts/ })
}
function approvalsLink() {
  return screen.getByRole('link', { name: /^Approvals/ })
}

test.each(['/workspaces/ops/activity', '/workspaces/ops/runs/r'])(
  'marks Activity current on its list and stable Run detail: %s',
  (path) => {
    render(path)
    expect(activityLink()).toHaveAttribute('href', '/workspaces/ops/activity')
    expect(activityLink()).toHaveAttribute('aria-current', 'page')
  },
)
// Catches: Tools losing its mark on a provider's own pages, which keep the
// provider's resource path, or keeping it on the Accounts page.
test.each([
  ['/workspaces/ops/tools', 'page'],
  ['/workspaces/ops/tools/private', 'page'],
  ['/workspaces/ops/providers/p/settings', 'page'],
  ['/workspaces/ops/accounts', null],
])('marks Tools current on the tools and on each provider page: %s', (path, current) => {
  render(path)
  const tools = screen.getByRole('link', { name: /^Tools/ })
  expect(tools).toHaveAttribute('href', '/workspaces/ops/tools')
  expect(tools.getAttribute('aria-current')).toBe(current)
})
test('Approvals sits directly under Overview and shows with nothing pending', () => {
  render('/workspaces/ops/approvals')
  const hrefs = within(screen.getByRole('group', { name: 'Operations' }))
    .getAllByRole('link')
    .map((link) => link.getAttribute('href'))
  expect(hrefs.indexOf('/workspaces/ops/approvals')).toBe(hrefs.indexOf('/workspaces/ops') + 1)
  expect(approvalsLink()).toHaveAccessibleName('Approvals')
  expect(approvalsLink()).toHaveAttribute('aria-current', 'page')
})
// Catches: pending actions in the workspaces behind the switcher going unseen.
test.each([false, true])(
  'the current workspace shows each count on its kind, the others their total on the switcher (collapsed: %s)',
  async (minimized) => {
    render('/workspaces/ops/tools', {
      minimized,
      active: [...listed, workspace('lab', 'Lab', 'member')],
      actionCounts: {
        ...counts,
        '/workspaces/lab': { total: '2', approvals: '2', connections: '0' },
      },
    })
    expect(approvalsLink()).toHaveAccessibleName('Approvals. 3 pending approvals')
    expect(accountsLink()).toHaveAccessibleName('Accounts. 2 pending connection requests')
    expect(activityLink()).toHaveAccessibleName('Activity')
    expect(switcher()).toHaveAccessibleName(
      'Workspace: Operations. 7 pending actions in other workspaces',
    )
    if (!minimized) {
      // The label and the count are separate text, so neither reads as "Approvals3".
      expect(within(approvalsLink()).getByText('Approvals')).toBeInTheDocument()
      expect(within(approvalsLink()).getByText('3')).toBeInTheDocument()
      expect(within(accountsLink()).getByText('2')).toBeInTheDocument()
      expect(activityLink().textContent).toBe('Activity')
      expect(within(switcher()).getByText('7')).toBeInTheDocument()
    }
    await userEvent.click(switcher())
    expect(screen.getByRole('menuitemradio', { name: 'Operations' })).toBeInTheDocument()
    const research = screen.getByRole('menuitemradio', { name: 'Research. 5 pending actions' })
    expect(within(research).getByText('5')).toBeInTheDocument()
    expect(
      screen.getByRole('menuitemradio', { name: 'Lab. 2 pending actions' }),
    ).toBeInTheDocument()
  },
)
// Catches: a workspace whose count did not load hiding the counts that did.
test('the switcher counts only the workspaces with a known count', () => {
  render('/workspaces/ops', { actionCounts: { ...counts, '/workspaces/research': null } })
  expect(switcher()).toHaveAccessibleName('Workspace: Operations')
})
test('a single request reads in the singular', () => {
  render('/workspaces/ops', {
    actionCounts: { '/workspaces/ops': { total: '2', approvals: '1', connections: '1' } },
  })
  expect(approvalsLink()).toHaveAccessibleName('Approvals. 1 pending approval')
  expect(accountsLink()).toHaveAccessibleName('Accounts. 1 pending connection request')
})
test.each([
  [
    'omits a zero count',
    { '/workspaces/ops': { total: '0', approvals: '0', connections: '0' } },
    'Approvals',
    undefined,
  ],
  ['omits a workspace without a count', {}, 'Approvals', undefined],
  [
    'bounds the visible badge and keeps the complete count in the accessible name',
    {
      '/workspaces/ops': {
        total: '9007199254740993',
        approvals: '9007199254740993',
        connections: '0',
      },
    },
    'Approvals. 9007199254740993 pending approvals',
    '999+',
  ],
  // An unknown count is never a zero: it shows no badge and reports nothing.
  ['omits an unavailable count', { '/workspaces/ops': null }, 'Approvals', undefined],
  ['omits every count when none loaded', null, 'Approvals', undefined],
])('%s', (_, actionCounts, name, badge) => {
  render('/workspaces/ops', { actionCounts })
  expect(approvalsLink()).toHaveAccessibleName(name)
  if (badge) expect(within(approvalsLink()).getByText(badge)).toBeInTheDocument()
  else expect(approvalsLink().textContent).toBe('Approvals')
})

function workspace(
  id: string,
  displayName: string,
  effectiveRole: string,
  state = 'active',
): SidebarWorkspace {
  return {
    path: `/workspaces/${id}`,
    displayName,
    effectiveRole,
    mcpEndpoint: `https://coral.example.com/mcp/${id}`,
    etag: 'etag',
    state,
  }
}

function ownerLinks(id: string) {
  const w = `/workspaces/${id}`
  return [
    w,
    `${w}/approvals`,
    `${w}/activity`,
    `${w}/tools`,
    `${w}/accounts`,
    `${w}/agents`,
    `${w}/members`,
  ]
}

interface RenderOptions {
  /** The shell's list. It is read on each render, so a test can change it and navigate. */
  active?: SidebarWorkspace[]
  /** The route's workspace when the shell does not list it: it failed to, or it is archived. */
  extra?: SidebarWorkspace
  /** The shell decides the area from the route it matched; a test names it. */
  area?: 'work' | 'settings'
  minimized?: boolean
  actionCounts?: Parameters<typeof Sidebar>[0]['actionCounts']
  auth?: BrowserAuth
  userName?: string
  organizationOwner?: boolean
}

function render(initialEntry: string, options: RenderOptions = {}) {
  function Harness() {
    const { pathname } = useLocation()
    const { active = listed, extra } = options
    const current = [...active, extra].find(
      (w) => w && (pathname === w.path || pathname.startsWith(`${w.path}/`)),
    )
    return (
      <Sidebar
        auth={options.auth ?? signedIn}
        area={options.area}
        organizationOwner={options.organizationOwner ?? false}
        userName={options.userName}
        initialIsMinimized={options.minimized ?? false}
        workspaces={active}
        workspace={current}
        actionCounts={options.actionCounts}
        form={{ csrf: 'test-csrf', requestId: 'test-request' }}
      />
    )
  }
  return renderRoute(<Harness />, { entry: initialEntry })
}

/**
 * Deletes the saved preference with a past expiry. happy-dom expires a
 * `Max-Age=0` cookie only after the clock ticks, so a render in the same
 * millisecond still reads its empty value as "expanded".
 */
function clearSidebarCookie() {
  document.cookie = `${SIDEBAR_COOKIE_NAME}=; Expires=Thu, 01 Jan 1970 00:00:00 GMT; Path=/`
}

/** The button that names the current workspace and opens the menu of the others. */
function switcher() {
  return screen.getByRole('button', { name: /^(Workspace: |Workspaces)/ })
}

/** The current workspace's pages. */
function links(container: HTMLElement) {
  return [...container.querySelectorAll('[role="group"] a')].map((a) => a.getAttribute('href'))
}
