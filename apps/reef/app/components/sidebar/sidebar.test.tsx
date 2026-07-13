import { createMemoryRouter, RouterProvider } from 'react-router'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-react'

import { Sidebar } from './sidebar'
import { SIDEBAR_COOKIE_NAME } from './sidebar-state'
import { routePath, routePattern } from '@/routing/routemap'

const WORKSPACES = [{ name: 'default' }, { name: 'analytics' }]

async function renderSidebar(
  initialIsMinimized: boolean,
  initialEntry = routePath('home'),
  workspaces: Array<{ name: string }> = [],
) {
  const router = createMemoryRouter(
    [
      {
        element: <Sidebar initialIsMinimized={initialIsMinimized} workspaces={workspaces} />,
        path: routePattern('workspaceSource'),
      },
      {
        element: <Sidebar initialIsMinimized={initialIsMinimized} workspaces={workspaces} />,
        path: routePattern('workspaceSources'),
      },
      {
        element: <Sidebar initialIsMinimized={initialIsMinimized} workspaces={workspaces} />,
        path: routePattern('workspaceSchemaTable'),
      },
      {
        element: <Sidebar initialIsMinimized={initialIsMinimized} workspaces={workspaces} />,
        path: routePattern('workspaceSchema'),
      },
      {
        element: <Sidebar initialIsMinimized={initialIsMinimized} workspaces={workspaces} />,
        path: routePattern('workspaceTrace'),
      },
      {
        element: <Sidebar initialIsMinimized={initialIsMinimized} workspaces={workspaces} />,
        path: routePattern('workspaceTraces'),
      },
      {
        element: <Sidebar initialIsMinimized={initialIsMinimized} workspaces={workspaces} />,
        path: '*',
      },
    ],
    { initialEntries: [initialEntry] },
  )

  return render(<RouterProvider router={router} />)
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
})
