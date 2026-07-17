import { createMemoryRouter, RouterProvider, useActionData, useNavigate } from 'react-router'
import { describe, expect, it } from 'vitest'
import { userEvent } from 'vitest/browser'
import { render } from 'vitest-browser-react'

import type { SourceDiscoveryData } from '@/routes/source-discovery'
import type { SourcesActionData } from '@/routes/sources-action'

import { SourceCreateDialog } from './source-create'

type SuccessfulDiscovery = Extract<SourceDiscoveryData, { status: 'success' }>

const DISCOVERY: SuccessfulDiscovery = {
  auth: { kind: 'bearer', label: 'Bearer token' },
  description: 'Weather observations and forecasts',
  format: 'openapi-yaml' as const,
  name: 'weather_api',
  status: 'success' as const,
  url: 'https://weather.example/openapi.yaml',
}

function RoutedSourceCreateDialog() {
  const navigate = useNavigate()
  const actionData = useActionData<SourcesActionData>()
  return (
    <SourceCreateDialog
      actionData={actionData}
      discoveryPath="/workspaces/analytics/sources/discover"
      open
      onOpenChange={(open) => {
        if (!open) navigate('/workspaces/analytics/sources')
      }}
    />
  )
}

async function renderSourceCreate(
  discovery: SuccessfulDiscovery = DISCOVERY,
  action?: () => Promise<SourcesActionData>,
) {
  const router = createMemoryRouter(
    [
      {
        action,
        element: <RoutedSourceCreateDialog />,
        path: '/workspaces/:workspaceId/sources/install',
      },
      {
        element: <div>Sources catalog</div>,
        path: '/workspaces/:workspaceId/sources',
      },
      {
        loader: () => ({ ...discovery, auth: { ...discovery.auth } }),
        path: '/workspaces/:workspaceId/sources/discover',
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

    await screen.getByLabelText('Source URL').fill(DISCOVERY.url)
    await screen.getByRole('button', { name: 'Next' }).click()

    await expect.element(screen.getByText('Step 2 of 3 — Details')).toBeVisible()
    await expect.poll(activeDialogCount).toBe(2)
    await expect.element(screen.getByLabelText('Name')).toHaveValue('weather_api')
    await expect
      .element(screen.getByLabelText('Description (optional)'))
      .toHaveValue('Weather observations and forecasts')
    await expect.element(screen.getByText('Detected an OpenAPI YAML document.')).toBeVisible()

    await screen.getByRole('button', { name: 'Next' }).click()

    await expect.element(screen.getByText('Step 3 of 3 — Credentials')).toBeVisible()
    await expect.poll(activeDialogCount).toBe(3)

    await screen.getByRole('button', { name: 'Back' }).click()

    await expect.element(screen.getByText('Step 2 of 3 — Details')).toBeVisible()
    await expect.poll(activeDialogCount).toBe(2)
  })

  it.each(['coral', 'coral_admin', 'public'])('rejects reserved source name %s', async (name) => {
    const { screen } = await renderSourceCreate()

    await screen.getByLabelText('Source URL').fill(DISCOVERY.url)
    await screen.getByRole('button', { name: 'Next' }).click()
    await expect.element(screen.getByLabelText('Name')).toBeVisible()
    const nameInput = screen.getByLabelText('Name')
    await nameInput.clear()
    await nameInput.fill(name)

    await expect
      .element(screen.getByText(`“${name}” is reserved by Coral. Choose another source name.`))
      .not.toBeInTheDocument()
    await userEvent.tab()
    await expect
      .element(screen.getByText(`“${name}” is reserved by Coral. Choose another source name.`))
      .toBeVisible()
    await expect.element(screen.getByRole('button', { name: 'Next' })).toBeDisabled()
  })

  it('shows a syntax error only after the name field is blurred', async () => {
    const { screen } = await renderSourceCreate()

    await screen.getByLabelText('Source URL').fill(DISCOVERY.url)
    await screen.getByRole('button', { name: 'Next' }).click()
    const nameInput = screen.getByLabelText('Name')
    await nameInput.clear()
    await nameInput.fill('weather-api')

    await expect
      .element(
        screen.getByText(
          'Use lowercase letters, digits, and underscores; the name must start with a letter.',
        ),
      )
      .not.toBeInTheDocument()
    await userEvent.tab()
    await expect
      .element(
        screen.getByText(
          'Use lowercase letters, digits, and underscores; the name must start with a letter.',
        ),
      )
      .toBeVisible()
  })

  it('uses accessible Base UI radios for source choices', async () => {
    const { screen } = await renderSourceCreate()

    await screen.getByLabelText('Source URL').fill(DISCOVERY.url)
    await screen.getByRole('button', { name: 'Next' }).click()

    const sourceTypeGroup = screen.getByRole('radiogroup', { name: 'Source type' })
    await expect.element(sourceTypeGroup).toBeVisible()
    await expect.element(screen.getByRole('radio', { name: 'REST API (OpenAPI)' })).toBeChecked()
    await screen.getByRole('radio', { name: 'MCP server' }).click()
    await expect.element(screen.getByRole('radio', { name: 'MCP server' })).toBeChecked()
  })

  it('keeps type selection editable when the URL is not an OpenAPI document', async () => {
    const url = 'https://tools.example/mcp'
    const { screen } = await renderSourceCreate({
      auth: { kind: 'unknown', label: '' },
      description: '',
      format: 'unknown',
      name: 'tools',
      status: 'success',
      url,
    })

    await screen.getByLabelText('Source URL').fill(url)
    await screen.getByRole('button', { name: 'Next' }).click()

    await expect.element(screen.getByText('No OpenAPI document was detected.')).toBeVisible()
    await screen.getByRole('radio', { name: 'MCP server' }).click()
    await expect.element(screen.getByRole('radio', { name: 'MCP server' })).toBeChecked()
  })

  it('prefills MCP for endpoints detected from their URL', async () => {
    const url = 'https://tools.example/mcp'
    const { screen } = await renderSourceCreate({
      auth: { kind: 'unknown', label: '' },
      description: '',
      format: 'mcp',
      name: 'mcp',
      status: 'success',
      url,
    })

    await screen.getByLabelText('Source URL').fill(url)
    await screen.getByRole('button', { name: 'Next' }).click()

    await expect.element(screen.getByText('Detected an MCP endpoint from its URL.')).toBeVisible()
    await expect.element(screen.getByRole('radio', { name: 'MCP server' })).toBeChecked()
  })

  it('prefills detected header authentication on the credentials step', async () => {
    const { screen } = await renderSourceCreate({
      ...DISCOVERY,
      auth: { headerName: 'X-Api-Key', kind: 'header', label: 'Header X-Api-Key' },
    })

    await screen.getByLabelText('Source URL').fill(DISCOVERY.url)
    await screen.getByRole('button', { name: 'Next' }).click()
    await expect.element(screen.getByLabelText('Name')).toBeVisible()
    await screen.getByRole('button', { name: 'Next' }).click()

    await expect
      .element(screen.getByText('Detected authentication: Header X-Api-Key.'))
      .toBeVisible()
    await expect.element(screen.getByRole('radio', { name: 'Custom header' })).toBeChecked()
    await expect.element(screen.getByLabelText('Header name')).toHaveValue('X-Api-Key')
  })

  it('builds a single-surface DSL v4 manifest with top-level inputs', async () => {
    const { screen } = await renderSourceCreate()

    await screen.getByLabelText('Source URL').fill(DISCOVERY.url)
    await screen.getByRole('button', { name: 'Next' }).click()
    await expect.element(screen.getByLabelText('Name')).toBeVisible()

    const manifest = submittedManifest()
    expect(manifest).toContain('\ninputs:\n  API_TOKEN:\n    kind: secret\n')
    expect(manifest).toContain('\nsurface:\n  type: openapi\n')
    expect(manifest).not.toContain('\nsurfaces:\n')
  })

  it('keeps the credentials dialog height stable across authentication choices', async () => {
    const { screen } = await renderSourceCreate()

    await screen.getByLabelText('Source URL').fill(DISCOVERY.url)
    await screen.getByRole('button', { name: 'Next' }).click()
    await expect.element(screen.getByLabelText('Name')).toBeVisible()
    await screen.getByRole('button', { name: 'Next' }).click()

    await expect.element(screen.getByRole('radio', { name: 'Bearer token' })).toBeChecked()
    const initialHeight = activeDialogHeight()

    await screen.getByRole('radio', { name: 'None' }).click()
    await expect.element(screen.getByRole('radio', { name: 'None' })).toBeChecked()
    await expect.poll(activeDialogHeight).toBe(initialHeight)

    await screen.getByRole('radio', { name: 'Custom header' }).click()
    await expect.element(screen.getByRole('radio', { name: 'Custom header' })).toBeChecked()
    await expect.poll(activeDialogHeight).toBe(initialHeight)
  })

  it('keeps the credentials dialog open when source creation fails', async () => {
    const message = 'The OpenAPI descriptor could not be loaded.'
    const { screen } = await renderSourceCreate(DISCOVERY, async () => {
      await new Promise((resolve) => setTimeout(resolve, 500))
      return { intent: 'import', message, name: DISCOVERY.name, status: 'error' }
    })

    await screen.getByLabelText('Source URL').fill(DISCOVERY.url)
    await screen.getByRole('button', { name: 'Next' }).click()
    await expect.element(screen.getByLabelText('Name')).toBeVisible()
    await screen.getByRole('button', { name: 'Next' }).click()
    await screen.getByRole('radio', { name: 'None' }).click()
    const createButton = screen.getByRole('button', { name: 'Create source' })
    createButton.element().focus()
    await expect.element(createButton).toHaveFocus()
    await createButton.click()
    await expect.element(screen.getByRole('button', { name: 'Creating…' })).toBeVisible()
    await userEvent.keyboard('{Escape}')

    await expect.element(screen.getByText(message)).toBeVisible()
    await expect.poll(activeDialogCount).toBe(3)
  })
})

function activeDialogCount() {
  return document.querySelectorAll('[role="dialog"]:not([data-ending-style])').length
}

function submittedManifest() {
  const input = document.querySelector<HTMLInputElement>('input[name="manifest_yaml"]')
  if (!input) throw new Error('Manifest input not found')
  return input.value
}

function activeDialogHeight() {
  const dialog = document.querySelector<HTMLElement>(
    '[role="dialog"]:not([aria-hidden="true"]):not([data-ending-style])',
  )
  if (!dialog) throw new Error('Active dialog not found')
  return dialog.offsetHeight
}
