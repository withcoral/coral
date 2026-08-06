import { createMemoryRouter, data, RouterProvider } from 'react-router'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-react'

import { Sidebar } from './sidebar'
import type { SidebarMembership } from './sidebar'
import { SIDEBAR_COOKIE_NAME } from './sidebar-state'
import { WorkspaceRole } from '@/generated/coral/v1/workspaces_pb'
import { validateWorkspaceName } from '@/lib/workspace-name'
import { routePath, routePattern } from '@/routing/routemap'
import { createDesktopApi } from '@/test-utils/desktop-api'

const MEMBERSHIPS: SidebarMembership[] = [
  { role: WorkspaceRole.OWNER, workspace: { name: 'default' } },
  { role: WorkspaceRole.MEMBER, workspace: { name: 'analytics' } },
]

async function renderSidebar(
  initialIsMinimized: boolean,
  initialEntry = routePath('home'),
  memberships: SidebarMembership[] = [],
) {
  const router = createMemoryRouter(
    [
      {
        action: async ({ request }) => {
          const formData = await request.formData()
          const nameValue = formData.get('name')
          const name = typeof nameValue === 'string' ? nameValue : ''
          if (formData.get('intent') !== 'create') {
            return data({ error: 'Unsupported workspace action.', name }, { status: 400 })
          }
          const error = validateWorkspaceName(name)
          return data({ error: error ?? '', name }, { status: error ? 400 : 200 })
        },
        path: routePattern('workspaces'),
      },
      {
        element: <Sidebar initialIsMinimized={initialIsMinimized} memberships={memberships} />,
        path: routePattern('workspaceSource'),
      },
      {
        element: <Sidebar initialIsMinimized={initialIsMinimized} memberships={memberships} />,
        path: routePattern('workspaceSources'),
      },
      {
        element: <Sidebar initialIsMinimized={initialIsMinimized} memberships={memberships} />,
        path: routePattern('workspaceSchemaTable'),
      },
      {
        element: <Sidebar initialIsMinimized={initialIsMinimized} memberships={memberships} />,
        path: routePattern('workspaceSchema'),
      },
      {
        element: <Sidebar initialIsMinimized={initialIsMinimized} memberships={memberships} />,
        path: routePattern('workspaceFunctions'),
      },
      {
        element: <Sidebar initialIsMinimized={initialIsMinimized} memberships={memberships} />,
        path: routePattern('workspaceTrace'),
      },
      {
        element: <Sidebar initialIsMinimized={initialIsMinimized} memberships={memberships} />,
        path: routePattern('workspaceTraces'),
      },
      {
        element: <Sidebar initialIsMinimized={initialIsMinimized} memberships={memberships} />,
        path: '*',
      },
    ],
    { initialEntries: [initialEntry] },
  )

  return Object.assign(await render(<RouterProvider router={router} />), { router })
}

describe('Sidebar', () => {
  beforeEach(() => {
    document.cookie = `${SIDEBAR_COOKIE_NAME}=; Max-Age=0; Path=/`
  })

  afterEach(async () => {
    delete window.coralDesktop
    vi.unstubAllEnvs()
    document.cookie = `${SIDEBAR_COOKIE_NAME}=; Max-Age=0; Path=/`
    await page.viewport(1024, 768)
  })

  it('collapses to the icon rail on mobile while preserving preference', async () => {
    await page.viewport(1024, 768)

    const screen = await renderSidebar(false)
    const sidebar = screen.getByRole('navigation', { name: 'Coral' })
    const brandLabel = screen.getByText('Coral')

    await expect.element(sidebar).toHaveAttribute('data-sidebar-minimized', 'false')
    await expect.element(sidebar).toHaveStyle({ flexBasis: '180px', minWidth: '0px' })
    await expect.element(brandLabel).toBeVisible()

    await page.viewport(375, 768)

    await expect.element(brandLabel).not.toBeVisible()
    await expect.element(sidebar).toHaveAttribute('data-sidebar-minimized', 'false')
    await expect.element(sidebar).toHaveStyle({ flexBasis: '58px', minWidth: '58px' })

    await page.viewport(1024, 768)

    await expect.element(brandLabel).toBeVisible()
  })

  it('toggles between expanded and minimized states', async () => {
    const screen = await renderSidebar(false)
    const sidebar = screen.getByRole('navigation', { name: 'Coral' })

    await screen.getByRole('button', { name: 'Collapse sidebar' }).click()

    await expect.element(sidebar).toHaveAttribute('data-sidebar-minimized', 'true')
    await expect.element(screen.getByRole('button', { name: 'Expand sidebar' })).toBeVisible()
  })

  it('restores the minimized state from the client cookie', async () => {
    document.cookie = `${SIDEBAR_COOKIE_NAME}=true; Path=/`

    const screen = await renderSidebar(false)
    const sidebar = screen.getByRole('navigation', { name: 'Coral' })

    await expect.element(sidebar).toHaveAttribute('data-sidebar-minimized', 'true')
    await expect.element(screen.getByRole('button', { name: 'Expand sidebar' })).toBeVisible()
  })

  it('ignores similarly named cookies when restoring the minimized state', async () => {
    document.cookie = `not_${SIDEBAR_COOKIE_NAME}=true; Path=/`

    const screen = await renderSidebar(false)
    const sidebar = screen.getByRole('navigation', { name: 'Coral' })

    await expect.element(sidebar).toHaveAttribute('data-sidebar-minimized', 'false')
    await expect.element(screen.getByRole('button', { name: 'Collapse sidebar' })).toBeVisible()
  })

  it('keeps source navigation in the active workspace', async () => {
    const screen = await renderSidebar(false, '/workspaces/analytics/sources/github', MEMBERSHIPS)

    await expect
      .element(screen.getByRole('link', { name: 'Sources' }))
      .toHaveAttribute('href', '/workspaces/analytics/sources')
  })

  it('keeps trace navigation in the active workspace', async () => {
    const screen = await renderSidebar(false, '/workspaces/analytics/traces/trace-1', MEMBERSHIPS)

    await expect
      .element(screen.getByRole('link', { name: 'Traces' }))
      .toHaveAttribute('href', '/workspaces/analytics/traces')
  })

  it('keeps schema navigation in the active workspace', async () => {
    const screen = await renderSidebar(
      false,
      routePath('workspaceSchemaTable', {
        schemaName: 'github',
        tableName: 'issues',
        workspaceId: 'analytics',
      }),
      MEMBERSHIPS,
    )

    await expect
      .element(screen.getByRole('link', { name: 'Schema' }))
      .toHaveAttribute('href', routePath('workspaceSchema', { workspaceId: 'analytics' }))
  })

  it('keeps function navigation in the active workspace', async () => {
    const screen = await renderSidebar(
      false,
      routePath('workspaceFunctions', { workspaceId: 'analytics' }),
      MEMBERSHIPS,
    )

    await expect
      .element(screen.getByRole('link', { name: 'Functions' }))
      .toHaveAttribute('href', routePath('workspaceFunctions', { workspaceId: 'analytics' }))
  })

  it('shows the active workspace and lists every local workspace', async () => {
    const screen = await renderSidebar(false, '/workspaces/analytics/sources', MEMBERSHIPS)

    await expect.element(screen.getByText('analytics', { exact: true })).toBeVisible()
    await screen.getByRole('button', { name: 'Open workspace menu' }).click()
    const defaultWorkspace = screen.getByRole('menuitemradio', { name: 'default' })
    const activeWorkspace = screen.getByRole('menuitemradio', { name: 'analytics' })

    await expect.element(defaultWorkspace).toBeVisible()
    await expect.element(defaultWorkspace).toHaveAttribute('aria-checked', 'false')
    await expect.element(activeWorkspace).toBeVisible()
    await expect.element(activeWorkspace).toHaveAttribute('aria-checked', 'true')
    expect(defaultWorkspace.element().querySelector('svg')).toBeNull()
    expect(activeWorkspace.element().querySelector('svg')).not.toBeNull()
  })

  it('keeps each membership role available on its workspace entry', async () => {
    const screen = await renderSidebar(false, '/workspaces/analytics/sources', MEMBERSHIPS)

    await screen.getByRole('button', { name: 'Open workspace menu' }).click()

    // The role has to survive loader -> props -> render: erasing it anywhere drops the attribute.
    await expect
      .element(screen.getByRole('menuitemradio', { name: 'default' }))
      .toHaveAttribute('data-workspace-role', 'owner')
    await expect
      .element(screen.getByRole('menuitemradio', { name: 'analytics' }))
      .toHaveAttribute('data-workspace-role', 'member')
  })

  it('renders only the memberships that carry a workspace', async () => {
    const screen = await renderSidebar(false, '/workspaces/analytics/sources', [
      ...MEMBERSHIPS,
      { role: WorkspaceRole.MEMBER },
    ])

    await screen.getByRole('button', { name: 'Open workspace menu' }).click()

    expect(
      [...document.querySelectorAll('[role="menuitemradio"]')].map((item) => item.textContent),
    ).toEqual(['default', 'analytics'])
  })

  it('ships no membership management controls', async () => {
    const screen = await renderSidebar(false, '/workspaces/analytics/sources', MEMBERSHIPS)

    await screen.getByRole('button', { name: 'Open workspace menu' }).click()

    // Member management is an explicit product non-goal for this release; the workspace menu
    // offers workspace switching, creation, and settings, and nothing that edits membership.
    expect(
      [...document.querySelectorAll('[role="menuitem"]')].map((item) => item.textContent),
    ).toEqual(['Create workspace', 'Settings'])
    expect(document.body.textContent).not.toMatch(/member|invite|people|remove|manage access/i)
  })

  it('opens and closes the create workspace dialog from the workspace menu', async () => {
    const screen = await renderSidebar(false, '/workspaces/analytics/sources', MEMBERSHIPS)

    await screen.getByRole('button', { name: 'Open workspace menu' }).click()
    await screen.getByRole('menuitem', { name: 'Create workspace' }).click()
    await expect.element(screen.getByRole('heading', { name: 'Create workspace' })).toBeVisible()

    await screen.getByRole('button', { name: 'Close' }).click()
    await expect
      .element(screen.getByRole('heading', { name: 'Create workspace' }))
      .not.toBeInTheDocument()
  })

  it('always shows settings in the workspace menu', async () => {
    const screen = await renderSidebar(false, '/workspaces/analytics/sources', MEMBERSHIPS)

    await screen.getByRole('button', { name: 'Open workspace menu' }).click()
    const createWorkspace = screen.getByRole('menuitem', { name: 'Create workspace' }).element()
    const settings = screen.getByRole('menuitem', { name: 'Settings' })
    const settingsElement = settings.element()
    const separatorBetween = [...document.querySelectorAll('[role="separator"]')].some(
      (separator) =>
        Boolean(
          createWorkspace.compareDocumentPosition(separator) & Node.DOCUMENT_POSITION_FOLLOWING,
        ) &&
        Boolean(
          separator.compareDocumentPosition(settingsElement) & Node.DOCUMENT_POSITION_FOLLOWING,
        ),
    )

    expect(separatorBetween).toBe(true)

    await settings.click()
    await expect.poll(() => screen.router.state.location.pathname).toBe(routePath('settings'))
  })

  it('shows no settings navigation items on the web', async () => {
    window.coralDesktop = createDesktopApi({
      getUpdateState: vi.fn(async () => ({ status: 'ready' as const, version: '0.9.0' })),
    })
    const screen = await renderSidebar(false, routePath('settings'), MEMBERSHIPS)

    await expect.element(screen.getByRole('link', { name: 'Home' })).toBeVisible()
    await expect
      .element(screen.getByRole('button', { name: 'Open workspace menu' }))
      .not.toBeInTheDocument()
    await expect.element(screen.getByRole('link', { name: 'MCP Clients' })).not.toBeInTheDocument()
    await expect.element(screen.getByRole('link', { name: 'Sources' })).not.toBeInTheDocument()
    await expect.element(screen.getByRole('link', { name: 'Schema' })).not.toBeInTheDocument()
    await expect.element(screen.getByRole('link', { name: 'Traces' })).not.toBeInTheDocument()
    expect(window.coralDesktop.getUpdateState).not.toHaveBeenCalled()
  })

  it('shows MCP Clients in desktop settings navigation', async () => {
    vi.stubEnv('CORAL_DESKTOP_APP', 'true')
    const screen = await renderSidebar(false, routePath('settings'), MEMBERSHIPS)

    await expect.element(screen.getByRole('link', { name: 'MCP Clients' })).toBeVisible()
  })

  it('shows desktop update state persistently in the sidebar footer', async () => {
    window.coralDesktop = createDesktopApi({
      getUpdateState: vi.fn(async () => ({ status: 'available' as const, version: '0.9.0' })),
    })
    vi.stubEnv('CORAL_DESKTOP_APP', 'true')
    const screen = await renderSidebar(false, routePath('home'), MEMBERSHIPS)

    await expect
      .element(
        screen.getByRole('status', {
          name: 'Coral 0.9.0 is available and will download automatically.',
        }),
      )
      .toBeVisible()
  })

  it('keeps the create workspace dialog closed after back and forward navigation', async () => {
    const tracesPath = routePath('workspaceTraces', { workspaceId: 'analytics' })
    const sourcesPath = routePath('workspaceSources', { workspaceId: 'analytics' })
    const screen = await renderSidebar(false, tracesPath, MEMBERSHIPS)

    await screen.getByRole('link', { name: 'Sources' }).click()
    await expect.poll(() => screen.router.state.location.pathname).toBe(sourcesPath)
    await screen.getByRole('button', { name: 'Open workspace menu' }).click()
    await screen.getByRole('menuitem', { name: 'Create workspace' }).click()
    await expect.element(screen.getByRole('heading', { name: 'Create workspace' })).toBeVisible()

    await screen.router.navigate(-1)
    await expect.poll(() => screen.router.state.location.pathname).toBe(tracesPath)
    await expect
      .element(screen.getByRole('heading', { name: 'Create workspace' }))
      .not.toBeInTheDocument()

    await screen.router.navigate(1)
    await expect.poll(() => screen.router.state.location.pathname).toBe(sourcesPath)
    await expect
      .element(screen.getByRole('heading', { name: 'Create workspace' }))
      .not.toBeInTheDocument()
  })

  it('keeps validation errors while open and clears them when reopening', async () => {
    const screen = await renderSidebar(false, '/workspaces/analytics/sources', MEMBERSHIPS)

    await screen.getByRole('button', { name: 'Open workspace menu' }).click()
    await screen.getByRole('menuitem', { name: 'Create workspace' }).click()
    await screen.getByLabelText('Workspace name').fill('New Team')
    await screen.getByRole('button', { name: 'Create workspace' }).click()

    await expect
      .element(screen.getByRole('alert'))
      .toHaveTextContent('Workspace name may only contain lowercase letters, numbers, and hyphens')
    await expect.element(screen.getByRole('heading', { name: 'Create workspace' })).toBeVisible()

    await screen.getByRole('button', { name: 'Close' }).click()
    await screen.getByRole('button', { name: 'Open workspace menu' }).click()
    await screen.getByRole('menuitem', { name: 'Create workspace' }).click()

    await expect.element(screen.getByRole('alert')).not.toBeInTheDocument()
  })

  it.each([
    [
      routePath('workspaceSources', { workspaceId: 'analytics' }),
      routePath('workspaceSources', { workspaceId: 'default' }),
    ],
    [
      routePath('workspaceSource', { sourceName: 'github', workspaceId: 'analytics' }),
      routePath('workspaceSources', { workspaceId: 'default' }),
    ],
    [
      routePath('workspaceSchema', { workspaceId: 'analytics' }),
      routePath('workspaceSchema', { workspaceId: 'default' }),
    ],
    [
      routePath('workspaceSchemaTable', {
        schemaName: 'github',
        tableName: 'issues',
        workspaceId: 'analytics',
      }),
      routePath('workspaceSchema', { workspaceId: 'default' }),
    ],
    [
      routePath('workspaceFunctions', { workspaceId: 'analytics' }),
      routePath('workspaceFunctions', { workspaceId: 'default' }),
    ],
    [
      routePath('workspaceTrace', { traceId: 'trace-1', workspaceId: 'analytics' }),
      routePath('workspaceTraces', { workspaceId: 'default' }),
    ],
  ])('switches workspaces within the current section from %s', async (currentPath, targetPath) => {
    const screen = await renderSidebar(false, currentPath, MEMBERSHIPS)

    await screen.getByRole('button', { name: 'Open workspace menu' }).click()
    await expect
      .element(screen.getByRole('menuitemradio', { name: 'default' }))
      .toHaveAttribute('href', targetPath)
  })

  it('falls back to the first workspace outside a canonical workspace route', async () => {
    const screen = await renderSidebar(false, routePath('home'), MEMBERSHIPS)

    await expect.element(screen.getByText('default', { exact: true })).toBeVisible()
    await expect
      .element(screen.getByRole('link', { name: 'Sources' }))
      .toHaveAttribute('href', '/workspaces/default/sources')
    await expect
      .element(screen.getByRole('link', { name: 'Schema' }))
      .toHaveAttribute('href', routePath('workspaceSchema', { workspaceId: 'default' }))
  })

  it('falls back to Coral and an empty menu when no workspace exists', async () => {
    const screen = await renderSidebar(false)

    await expect.element(screen.getByText('Coral', { exact: true })).toBeVisible()
    await screen.getByRole('button', { name: 'Open workspace menu' }).click()
    await expect.element(screen.getByRole('menuitem', { name: 'No workspaces' })).toBeVisible()
  })

  it('links to the onboarding flow', async () => {
    const screen = await renderSidebar(false, routePath('home'), MEMBERSHIPS)

    await expect
      .element(screen.getByRole('link', { name: 'Onboarding' }))
      .toHaveAttribute('href', routePath('onboarding'))
  })

  it('keeps collapsed navigation labels available through tooltips', async () => {
    const screen = await renderSidebar(true, '/workspaces/analytics/sources', MEMBERSHIPS)

    await screen.getByRole('link', { name: 'Sources' }).hover()
    await expect
      .poll(() => document.querySelector('[data-base-ui-portal]')?.textContent)
      .toContain('Sources')
  })
})
