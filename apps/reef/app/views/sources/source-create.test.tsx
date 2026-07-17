import { createMemoryRouter, RouterProvider, useNavigate } from 'react-router'
import { describe, expect, it } from 'vitest'
import { render } from 'vitest-browser-react'

import { SourceCreateDialog } from './source-create'

function RoutedSourceCreateDialog() {
  const navigate = useNavigate()
  return (
    <SourceCreateDialog
      actionData={undefined}
      open
      onOpenChange={(open) => {
        if (!open) navigate('/workspaces/analytics/sources')
      }}
    />
  )
}

async function renderSourceCreate() {
  const router = createMemoryRouter(
    [
      {
        element: <RoutedSourceCreateDialog />,
        path: '/workspaces/:workspaceId/sources/install',
      },
      {
        element: <div>Sources catalog</div>,
        path: '/workspaces/:workspaceId/sources',
      },
    ],
    { initialEntries: ['/workspaces/analytics/sources/install'] },
  )

  return { router, screen: await render(<RouterProvider router={router} />) }
}

describe('SourceCreateDialog', () => {
  it('renders as a routed modal and cancels back to the workspace source catalog', async () => {
    const { router, screen } = await renderSourceCreate()

    await expect.element(screen.getByRole('dialog')).toBeVisible()
    await expect.element(screen.getByRole('heading', { name: 'Create source' })).toBeVisible()
    await screen.getByRole('button', { name: 'Cancel' }).click()

    expect(router.state.location.pathname).toBe('/workspaces/analytics/sources')
  })

  it('opens each wizard step as a nested dialog', async () => {
    const { screen } = await renderSourceCreate()

    await expect.poll(activeDialogCount).toBe(1)

    await screen.getByLabelText('Name').fill('weather_api')
    await screen.getByRole('button', { name: 'Next' }).click()

    await expect.element(screen.getByText('Step 2 of 3 — Connection')).toBeVisible()
    await expect.poll(activeDialogCount).toBe(2)

    await screen
      .getByLabelText('OpenAPI descriptor URL')
      .fill('https://weather.example/openapi.yaml')
    await screen.getByRole('button', { name: 'Next' }).click()

    await expect.element(screen.getByText('Step 3 of 3 — Credentials')).toBeVisible()
    await expect.poll(activeDialogCount).toBe(3)

    await screen.getByRole('button', { name: 'Back' }).click()

    await expect.element(screen.getByText('Step 2 of 3 — Connection')).toBeVisible()
    await expect.poll(activeDialogCount).toBe(2)
  })
})

function activeDialogCount() {
  return document.querySelectorAll('[role="dialog"]:not([data-ending-style])').length
}
