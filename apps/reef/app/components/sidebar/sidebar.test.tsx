// @vitest-environment happy-dom

import { act, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { useLocation } from 'react-router'
import { expect, onTestFinished, test } from 'vitest'
import { renderRoute } from '@/test-support/route'
import type { BrowserAuth } from '@/auth/types'
import type { ActionCounts } from '@/lib/activity-live-types'
import { Sidebar, type SidebarWorkspace } from './sidebar'
import { SIDEBAR_COOKIE_NAME } from './sidebar-state'

const signedIn: BrowserAuth = { mode: 'required', csrfToken: 'test-csrf' }
const listed: SidebarWorkspace[] = [
  workspace('ops', 'Operations', 'owner'),
  workspace('research', 'Research', 'member'),
]

const researchLinks = [
  '/workspaces/research',
  '/workspaces/research/activity',
  '/workspaces/research/providers',
  '/workspaces/research/accounts',
  '/workspaces/research/functions',
  '/workspaces/research/members',
]

test('only the current workspace starts open, and opening another keeps it open', async () => {
  const { container } = render('/workspaces/ops/providers')
  expect(links(container)).toEqual(ownerLinks('ops'))

  await userEvent.click(header('Research'))
  expect(header('Operations')).toHaveAttribute('aria-expanded', 'true')
  expect(links(container)).toEqual([...ownerLinks('ops'), ...researchLinks])

  await userEvent.click(header('Operations'))
  expect(links(container)).toEqual(researchLinks)
})

test('navigating into a workspace opens it without closing the others', async () => {
  const { container, router } = render('/workspaces/research')
  await act(async () => router.navigate('/workspaces/ops/agents'))
  expect(links(container)).toEqual([...ownerLinks('ops'), ...researchLinks])
})

test('a member workspace hides owner-only destinations', async () => {
  const { container } = render('/workspaces/research')
  expect(links(container)).not.toContain('/workspaces/research/agents')
})

test('only an owner can archive a workspace, from its header', async () => {
  render('/workspaces/research')
  expect(screen.getByRole('button', { name: 'Archive Operations' })).toBeInTheDocument()
  expect(screen.queryByRole('button', { name: 'Archive Research' })).toBeNull()
})

test('every workspace header copies its own workspace MCP URL', async () => {
  render('/workspaces/research')
  const copy = screen.getByRole('button', { name: 'Copy Research workspace MCP URL' })
  let copied = ''
  const clipboard = Object.getOwnPropertyDescriptor(navigator, 'clipboard')
  Object.defineProperty(navigator, 'clipboard', {
    configurable: true,
    value: { writeText: async (text: string) => void (copied = text) },
  })
  try {
    await act(async () => copy.click())
  } finally {
    if (clipboard) Object.defineProperty(navigator, 'clipboard', clipboard)
    else delete (navigator as { clipboard?: Clipboard }).clipboard
  }
  expect(copied).toBe('https://coral.example.com/mcp/research')
})

// Catches: the current workspace vanishing when the shell could not list workspaces.
test('an open active workspace missing from the lists still appears', async () => {
  const { container } = render('/workspaces/unlisted', {
    extra: workspace('unlisted', 'Unlisted', 'owner'),
  })
  expect(header('Unlisted')).toHaveAttribute('aria-expanded', 'true')
  expect(links(container)).toEqual(ownerLinks('unlisted'))
})

// Catches: an unlisted archived workspace joining the active tree with destinations it cannot use.
test('an open archived workspace missing from the lists still appears as archived', async () => {
  render('/workspaces/unlisted/activity', {
    extra: workspace('unlisted', 'Unlisted', 'owner', 'archived'),
  })
  expect(header('Archived')).toHaveAttribute('aria-expanded', 'true')
  expect(screen.getByRole('link', { name: 'Unlisted, archived' })).toHaveAttribute(
    'aria-current',
    'page',
  )
  expect(screen.queryByRole('button', { name: 'Unlisted' })).toBeNull()
})

// Catches: an archived workspace leaving the sidebar, or joining the active tree
// with destinations it cannot use.
test('archived workspaces wait in a collapsed group that opens on the current one', async () => {
  const { router } = render('/workspaces/ops', {
    archived: [workspace('old', 'Old ops', 'owner', 'archived')],
  })
  expect(header('Archived')).toHaveAttribute('aria-expanded', 'false')
  expect(screen.queryByRole('link', { name: 'Old ops, archived' })).toBeNull()

  await act(async () => router.navigate('/workspaces/old/activity'))
  expect(header('Archived')).toHaveAttribute('aria-expanded', 'true')
  const entry = screen.getByRole('link', { name: 'Old ops, archived' })
  expect(entry).toHaveAttribute('href', '/workspaces/old/activity')
  expect(entry).toHaveAttribute('aria-current', 'page')
  expect(screen.queryByRole('button', { name: 'Old ops' })).toBeNull()
  expect(screen.getByRole('button', { name: 'Restore Old ops' })).toBeInTheDocument()
})

// Catches: archiving the workspace you are in hiding it from the sidebar: the
// active tree drops it while the Archived group stays shut.
test('archiving the current workspace opens the archived group on it', async () => {
  const ops = workspace('ops', 'Operations', 'owner')
  const options: RenderOptions = { active: [ops] }
  const { router } = render('/workspaces/ops/activity', options)
  // The archive revalidates the shell's lists and the route; the path stays.
  options.active = []
  options.archived = [{ ...ops, state: 'archived' }]
  await act(async () => router.navigate('/workspaces/ops/activity', { replace: true }))
  expect(header('Archived')).toHaveAttribute('aria-expanded', 'true')
  expect(screen.getByRole('link', { name: 'Operations, archived' })).toHaveAttribute(
    'aria-current',
    'page',
  )
})

// Catches: the collapsed sidebar losing your place in an archived workspace.
test('collapsed, one archived tile marks every archived workspace and opens the group', async () => {
  // Opening the group expands the sidebar, which saves the preference.
  onTestFinished(clearSidebarCookie)
  const archived = [
    workspace('old', 'Old ops', 'owner', 'archived'),
    workspace('older', 'Older ops', 'owner', 'archived'),
  ]
  const { router } = render('/workspaces/ops', { minimized: true, archived })
  expect(screen.getByRole('button', { name: 'Archived workspaces' })).not.toHaveAttribute(
    'aria-current',
  )
  for (const [path, name] of [
    ['/workspaces/old/activity', 'Old ops'],
    ['/workspaces/older/activity', 'Older ops'],
  ]) {
    await act(async () => router.navigate(path))
    expect(screen.getByRole('button', { name: `Archived: ${name}` })).toHaveAttribute(
      'aria-current',
      'page',
    )
  }
  await userEvent.click(screen.getByRole('button', { name: 'Archived: Older ops' }))
  expect(header('Archived')).toHaveAttribute('aria-expanded', 'true')
  expect(screen.getByRole('link', { name: 'Older ops, archived' })).toHaveAttribute(
    'aria-current',
    'page',
  )
})

// Catches: a member seeing organization pages they cannot open, or an
// organization owner losing the way to the workspaces page.
test.each([
  ['an organization owner', true, ['/organization/members', '/organization/workspaces']],
  ['an organization member', false, []],
])('the account menu links the organization pages for %s only', async (_, owner, hrefs) => {
  render('/workspaces/ops', { organizationOwner: owner })
  await userEvent.click(screen.getByRole('button', { name: /^Open account menu/ }))
  await screen.findByRole('menuitem', { name: 'Appearance' })
  const items = ['Members', 'Workspaces'].flatMap((name) =>
    screen.queryAllByRole('menuitem', { name }),
  )
  expect(items.map((item) => item.getAttribute('href'))).toEqual(hrefs)
})

test('collapsed, every workspace is a tile and only the current one shows its destinations', async () => {
  render('/workspaces/ops/providers', { minimized: true })
  const rail = screen.getByRole('group', { name: 'Workspaces' })
  const tiles = [...rail.querySelectorAll(':scope > div > a')]
  expect(tiles.map((a) => [a.getAttribute('aria-label'), a.getAttribute('href')])).toEqual([
    ['Operations', '/workspaces/ops'],
    ['Research', '/workspaces/research'],
  ])
  expect([...rail.querySelectorAll('a')].map((a) => a.getAttribute('href'))).toEqual([
    '/workspaces/ops',
    '/workspaces/ops',
    '/workspaces/ops/activity',
    '/workspaces/ops/providers',
    '/workspaces/ops/accounts',
    '/workspaces/ops/functions',
    '/workspaces/ops/agents',
    '/workspaces/ops/members',
    '/workspaces/research',
  ])
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
      ['https://slack.withcoral.com/connect', '_blank'],
      ['https://github.com/withcoral/coral', '_blank'],
      ['https://github.com/withcoral/coral/issues/new', '_blank'],
    ])
  },
)

// Catches: a local deployment, where nobody else can sign in, offering pages
// to manage other people, or losing the organization's other pages with them.
test.each<[string, boolean, BrowserAuth]>([
  ['a signed-in deployment', true, signedIn],
  ['a local deployment', false, { mode: 'disabled' }],
])('%s shows people management: %s', async (_, shown, auth) => {
  const { container } = render('/workspaces/ops', { organizationOwner: true, auth })
  expect(links(container).includes('/workspaces/ops/members')).toBe(shown)
  await userEvent.click(screen.getByRole('button', { name: /^Open account menu/ }))
  const members = screen.queryByRole('menuitem', { name: 'Members' })
  expect(members !== null).toBe(shown)
  expect(screen.getByRole('menuitem', { name: 'Workspaces' })).toBeInTheDocument()
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

const counts = { '/workspaces/ops': '3', '/workspaces/research': '5' }

function activityLink() {
  return screen.getByRole('link', { name: /^Activity/ })
}

test.each(['/workspaces/ops/activity', '/workspaces/ops/runs/r'])(
  'marks Activity current on its list and stable Run detail: %s',
  (path) => {
    render(path)
    expect(activityLink()).toHaveAttribute('href', '/workspaces/ops/activity')
    expect(activityLink()).toHaveAttribute('aria-current', 'page')
  },
)
test('an open workspace shows its count on Activity, a closed one on its name', () => {
  render('/workspaces/ops/providers', { actionCounts: counts })
  expect(activityLink()).toHaveAccessibleName('Activity. 3 pending actions')
  expect(header('Operations')).toHaveAccessibleName('Operations')
  expect(header(/^Research/)).toHaveAccessibleName('Research. 5 pending actions')
  // The label and the count are separate text, so neither reads as "Activity3".
  expect(within(activityLink()).getByText('Activity')).toBeInTheDocument()
  expect(within(activityLink()).getByText('3')).toBeInTheDocument()
  expect(within(header(/^Research/)).getByText('5')).toBeInTheDocument()
})
test('collapsed, the current workspace shows its count on Activity and the others on their tile', () => {
  render('/workspaces/ops/providers', { minimized: true, actionCounts: counts })
  const rail = screen.getByRole('group', { name: 'Workspaces' })
  expect(within(rail).getByRole('link', { name: 'Operations' })).toBeInTheDocument()
  expect(activityLink()).toHaveAccessibleName('Activity. 3 pending actions')
  expect(
    within(rail).getByRole('link', { name: 'Research. 5 pending actions' }),
  ).toBeInTheDocument()
})
test.each([
  ['omits a zero count', { '/workspaces/ops': '0' }, 'Activity', undefined],
  ['omits a workspace without a count', {}, 'Activity', undefined],
  [
    'bounds the visible badge and keeps the complete count in the accessible name',
    { '/workspaces/ops': '9007199254740993' },
    'Activity. 9007199254740993 pending actions',
    '999+',
  ],
  // An unknown count leaves nothing for you to do; Activity reports it.
  ['omits an unavailable count', { '/workspaces/ops': null }, 'Activity', undefined],
  ['omits every count when none loaded', null, 'Activity', undefined],
])('%s', (_, actionCounts, name, badge) => {
  render('/workspaces/ops', { actionCounts })
  expect(activityLink()).toHaveAccessibleName(name)
  if (badge) expect(within(activityLink()).getByText(badge)).toBeInTheDocument()
  else expect(activityLink().textContent).toBe('Activity')
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
    `${w}/activity`,
    `${w}/providers`,
    `${w}/accounts`,
    `${w}/functions`,
    `${w}/agents`,
    `${w}/members`,
  ]
}

interface RenderOptions {
  /** The shell's lists. They are read on each render, so a test can change them and navigate. */
  active?: SidebarWorkspace[]
  archived?: SidebarWorkspace[]
  /** The route's workspace when the shell could not list it. */
  extra?: SidebarWorkspace
  minimized?: boolean
  actionCounts?: ActionCounts | null
  organizationOwner?: boolean
  auth?: BrowserAuth
}

function render(initialEntry: string, options: RenderOptions = {}) {
  function Harness() {
    const { pathname } = useLocation()
    const { active = listed, archived = [], extra } = options
    const current = [...active, extra, ...archived].find(
      (w) => w && (pathname === w.path || pathname.startsWith(`${w.path}/`)),
    )
    return (
      <Sidebar
        auth={options.auth ?? signedIn}
        initialIsMinimized={options.minimized ?? false}
        workspaces={active}
        archivedWorkspaces={archived}
        workspace={current}
        actionCounts={options.actionCounts}
        organizationOwner={options.organizationOwner ?? false}
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

/** A workspace header: the button that opens and closes its destinations. */
function header(name: string | RegExp) {
  return screen.getByRole('button', { name })
}

/** The destinations inside open workspace groups. */
function links(container: HTMLElement) {
  return [
    ...container.querySelectorAll('[role="group"] [role="group"] a, [aria-label="Workspaces"] a'),
  ].map((a) => a.getAttribute('href'))
}
