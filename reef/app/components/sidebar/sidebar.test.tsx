import { createMemoryRouter, RouterProvider } from 'react-router'
import { afterEach, describe, expect, it } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-react'

import { Sidebar } from './sidebar'

async function renderSidebar(initialIsMinimized: boolean) {
  const router = createMemoryRouter(
    [
      {
        element: <Sidebar initialIsMinimized={initialIsMinimized} />,
        path: '/',
      },
    ],
    { initialEntries: ['/'] },
  )

  return render(<RouterProvider router={router} />)
}

describe('Sidebar', () => {
  afterEach(async () => {
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
})
