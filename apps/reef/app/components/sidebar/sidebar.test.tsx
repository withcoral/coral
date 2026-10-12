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
  expect(settingsLinks(container)).not.toContain('/workspaces/research/settings')
})

test('settings is a header control for owners, not a destination', async () => {
  const { container } = render('/workspaces/ops/settings')
  expect(links(container)).not.toContain('/workspaces/ops/settings')
  expect(settingsLinks(container)).toEqual(['/workspaces/ops/settings'])
  const cog = container.querySelector('a[href="/workspaces/ops/settings"]')!
  expect(cog).toHaveAttribute('aria-current', 'page')
})

test('every workspace header copies its MCP endpoint', async () => {
  render('/workspaces/research')
  const copy = screen.getByRole('button', { name: 'Copy Research MCP endpoint' })
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

// Catches: an archived workspace losing its navigation while you have it open.
// The list holds only active workspaces.
test('an open workspace missing from the active list still appears', async () => {
  const archived = workspace('archived', 'Archived', 'owner')
  const { container } = render('/workspaces/archived', archived)
  expect(header('Archived')).toHaveAttribute('aria-expanded', 'true')
  expect(links(container)).toEqual(ownerLinks('archived'))
})

// Catches: a member seeing organization pages they cannot open, or an
// organization owner losing the way to the workspaces page.
test.each([
  ['an organization owner', true, ['/organization/members', '/organization/workspaces']],
  ['an organization member', false, []],
])('the account menu links the organization pages for %s only', async (_, owner, hrefs) => {
  render('/workspaces/ops', undefined, false, undefined, owner)
  await userEvent.click(screen.getByRole('button', { name: /^Open account menu/ }))
  await screen.findByRole('menuitem', { name: 'Appearance' })
  const items = ['Members', 'Workspaces'].flatMap((name) =>
    screen.queryAllByRole('menuitem', { name }),
  )
  expect(items.map((item) => item.getAttribute('href'))).toEqual(hrefs)
})

test('collapsed, every workspace is a tile and only the current one shows its destinations', async () => {
  render('/workspaces/ops/providers', undefined, true)
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

test('collapsed, the help menu holds the external links', async () => {
  render('/workspaces/ops', undefined, true)
  expect(screen.queryByRole('link', { name: 'Chat on Slack' })).not.toBeInTheDocument()
  await userEvent.click(screen.getByRole('button', { name: 'Get help' }))
  const items = ['Chat on Slack', 'Star on GitHub'].map((name) =>
    screen.getByRole('menuitem', { name }),
  )
  expect(items.map((a) => [a.getAttribute('href'), a.getAttribute('target')])).toEqual([
    ['https://slack.withcoral.com/connect', '_blank'],
    ['https://github.com/withcoral/coral', '_blank'],
  ])
})

// Catches: a local deployment, where nobody else can sign in, offering pages
// to manage other people, or losing the organization's other pages with them.
test.each<[string, boolean, BrowserAuth]>([
  ['a signed-in deployment', true, signedIn],
  ['a local deployment', false, { mode: 'disabled' }],
])('%s shows people management: %s', async (_, shown, auth) => {
  const { container } = render('/workspaces/ops', undefined, false, undefined, true, auth)
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
  render('/workspaces/ops/providers', undefined, false, counts)
  expect(activityLink()).toHaveAccessibleName('Activity. 3 pending actions')
  expect(header('Operations')).toHaveAccessibleName('Operations')
  expect(header(/^Research/)).toHaveAccessibleName('Research. 5 pending actions')
  // The label and the count are separate text, so neither reads as "Activity3".
  expect(within(activityLink()).getByText('Activity')).toBeInTheDocument()
  expect(within(activityLink()).getByText('3')).toBeInTheDocument()
  expect(within(header(/^Research/)).getByText('5')).toBeInTheDocument()
})
test('collapsed, the current workspace shows its count on Activity and the others on their tile', () => {
  render('/workspaces/ops/providers', undefined, true, counts)
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
  [
    'distinguishes an unavailable count from zero',
    { '/workspaces/ops': null },
    'Activity. Pending actions unavailable',
    '?',
  ],
  [
    'marks every count unavailable when the stream is',
    null,
    'Activity. Pending actions unavailable',
    '?',
  ],
])('%s', (_, actionCounts, name, badge) => {
  render('/workspaces/ops', undefined, false, actionCounts)
  expect(activityLink()).toHaveAccessibleName(name)
  if (badge) expect(within(activityLink()).getByText(badge)).toBeInTheDocument()
  else expect(activityLink().textContent).toBe('Activity')
})

function workspace(id: string, displayName: string, effectiveRole: string): SidebarWorkspace {
  return {
    path: `/workspaces/${id}`,
    displayName,
    effectiveRole,
    mcpEndpoint: `https://coral.example.com/mcp/${id}`,
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

function render(
  initialEntry: string,
  extra?: SidebarWorkspace,
  minimized = false,
  actionCounts?: ActionCounts | null,
  organizationOwner = false,
  auth: BrowserAuth = signedIn,
) {
  function Harness() {
    const { pathname } = useLocation()
    const current = [...listed, extra].find(
      (w) => w && (pathname === w.path || pathname.startsWith(`${w.path}/`)),
    )
    return (
      <Sidebar
        auth={auth}
        initialIsMinimized={minimized}
        workspaces={listed}
        workspace={current}
        actionCounts={actionCounts}
        organizationOwner={organizationOwner}
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

function settingsLinks(container: HTMLElement) {
  return [...container.querySelectorAll('a[href$="/settings"]')].map((a) => a.getAttribute('href'))
}
