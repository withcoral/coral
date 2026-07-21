import { createMemoryRouter, data, RouterProvider } from 'react-router'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-react'

import { Sidebar } from './sidebar'
import { SIDEBAR_COOKIE_NAME } from './sidebar-state'
import { validateWorkspaceName } from '@/lib/workspace-name'
import { routePath, routePattern } from '@/routing/routemap'
import type { BrowserAuth } from '@/auth/types'

const WORKSPACES = [{ name: 'default' }, { name: 'analytics' }]

async function renderSidebar(
  initialIsMinimized: boolean,
  initialEntry = routePath('home'),
  workspaces: Array<{ name: string }> = [],
  auth: BrowserAuth = { mode: 'disabled' },
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
        element: (
          <Sidebar auth={auth} initialIsMinimized={initialIsMinimized} workspaces={workspaces} />
        ),
        path: routePattern('workspaceSource'),
      },
      {
        element: (
          <Sidebar auth={auth} initialIsMinimized={initialIsMinimized} workspaces={workspaces} />
        ),
        path: routePattern('workspaceSources'),
      },
      {
        element: (
          <Sidebar auth={auth} initialIsMinimized={initialIsMinimized} workspaces={workspaces} />
        ),
        path: routePattern('workspaceSchemaTable'),
      },
      {
        element: (
          <Sidebar auth={auth} initialIsMinimized={initialIsMinimized} workspaces={workspaces} />
        ),
        path: routePattern('workspaceSchema'),
      },
      {
        element: (
          <Sidebar auth={auth} initialIsMinimized={initialIsMinimized} workspaces={workspaces} />
        ),
        path: routePattern('workspaceTrace'),
      },
      {
        element: (
          <Sidebar auth={auth} initialIsMinimized={initialIsMinimized} workspaces={workspaces} />
        ),
        path: routePattern('workspaceTraces'),
      },
      {
        element: (
          <Sidebar auth={auth} initialIsMinimized={initialIsMinimized} workspaces={workspaces} />
        ),
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
    const screen = await renderSidebar(false, '/workspaces/analytics/sources/github', WORKSPACES)

    await expect
      .element(screen.getByRole('link', { name: 'Sources' }))
      .toHaveAttribute('href', '/workspaces/analytics/sources')
  })

  it('keeps trace navigation in the active workspace', async () => {
    const screen = await renderSidebar(false, '/workspaces/analytics/traces/trace-1', WORKSPACES)

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
      WORKSPACES,
    )

    await expect
      .element(screen.getByRole('link', { name: 'Schema' }))
      .toHaveAttribute('href', routePath('workspaceSchema', { workspaceId: 'analytics' }))
  })

  it('shows the active workspace and lists every local workspace', async () => {
    const screen = await renderSidebar(false, '/workspaces/analytics/sources', WORKSPACES)

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

  it('opens and closes the create workspace dialog from the workspace menu', async () => {
    const screen = await renderSidebar(false, '/workspaces/analytics/sources', WORKSPACES)

    await screen.getByRole('button', { name: 'Open workspace menu' }).click()
    await screen.getByRole('menuitem', { name: 'Create workspace' }).click()
    await expect.element(screen.getByRole('heading', { name: 'Create workspace' })).toBeVisible()

    await screen.getByRole('button', { name: 'Close' }).click()
    await expect
      .element(screen.getByRole('heading', { name: 'Create workspace' }))
      .not.toBeInTheDocument()
  })

  it('keeps the create workspace dialog closed after back and forward navigation', async () => {
    const tracesPath = routePath('workspaceTraces', { workspaceId: 'analytics' })
    const sourcesPath = routePath('workspaceSources', { workspaceId: 'analytics' })
    const screen = await renderSidebar(false, tracesPath, WORKSPACES)

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
    const screen = await renderSidebar(false, '/workspaces/analytics/sources', WORKSPACES)

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
      routePath('workspaceTrace', { traceId: 'trace-1', workspaceId: 'analytics' }),
      routePath('workspaceTraces', { workspaceId: 'default' }),
    ],
  ])('switches workspaces within the current section from %s', async (currentPath, targetPath) => {
    const screen = await renderSidebar(false, currentPath, WORKSPACES)

    await screen.getByRole('button', { name: 'Open workspace menu' }).click()
    await expect
      .element(screen.getByRole('menuitemradio', { name: 'default' }))
      .toHaveAttribute('href', targetPath)
  })

  it('falls back to the first workspace outside a canonical workspace route', async () => {
    const screen = await renderSidebar(false, routePath('settings'), WORKSPACES)

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

  it('keeps collapsed navigation labels available through tooltips', async () => {
    const screen = await renderSidebar(true, '/workspaces/analytics/sources', WORKSPACES)

    await screen.getByRole('link', { name: 'Sources' }).hover()
    await expect
      .poll(() => document.querySelector('[data-base-ui-portal]')?.textContent)
      .toContain('Sources')
  })

  it('shows logout only for hosted auth and submits its CSRF token', async () => {
    const local = await renderSidebar(false, '/workspaces/analytics/sources', WORKSPACES)
    await expect.element(local.getByRole('button', { name: 'Sign out' })).not.toBeInTheDocument()

    const hosted = await renderSidebar(false, '/workspaces/analytics/sources', WORKSPACES, {
      csrfToken: 'hosted-csrf-token',
      mode: 'required',
    })
    const signOut = hosted.getByRole('button', { name: 'Sign out' })
    const form = signOut.element().closest('form')

    await expect.element(signOut).toBeVisible()
    expect(form?.getAttribute('action')).toBe('/logout')
    expect(form?.getAttribute('method')).toBe('post')
    expect(form?.querySelector<HTMLInputElement>('input[name="csrf"]')?.value).toBe(
      'hosted-csrf-token',
    )
  })
})
