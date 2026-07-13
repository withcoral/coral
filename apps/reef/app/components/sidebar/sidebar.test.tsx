import { createMemoryRouter, RouterProvider } from 'react-router'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-react'

import { Sidebar } from './sidebar'
import { SIDEBAR_COOKIE_NAME } from './sidebar-state'
import { routePath, routePattern } from '@/routing/routemap'

async function renderSidebar(initialIsMinimized: boolean, initialEntry = routePath('home')) {
  const router = createMemoryRouter(
    [
      {
        element: <Sidebar initialIsMinimized={initialIsMinimized} />,
        path: '/workspaces/:workspaceId/sources/:sourceName?',
      },
      {
        element: <Sidebar initialIsMinimized={initialIsMinimized} />,
        path: routePattern('workspaceSchemaTable'),
      },
      {
        element: <Sidebar initialIsMinimized={initialIsMinimized} />,
        path: routePattern('workspaceSchema'),
      },
      {
        element: <Sidebar initialIsMinimized={initialIsMinimized} />,
        path: '/workspaces/:workspaceId/traces/:traceId?',
      },
      {
        element: <Sidebar initialIsMinimized={initialIsMinimized} />,
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
    const screen = await renderSidebar(false, '/workspaces/analytics/sources/github')

    await expect
      .element(screen.getByRole('link', { name: 'Sources' }))
      .toHaveAttribute('href', '/workspaces/analytics/sources')
  })

  it('keeps trace navigation in the active workspace', async () => {
    const screen = await renderSidebar(false, '/workspaces/analytics/traces/trace-1')

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
    )

    await expect
      .element(screen.getByRole('link', { name: 'Schema' }))
      .toHaveAttribute('href', routePath('workspaceSchema', { workspaceId: 'analytics' }))
  })
})
