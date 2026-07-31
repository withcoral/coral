import { createMemoryRouter, RouterProvider, useActionData, useNavigate } from 'react-router'
import { describe, expect, it, vi } from 'vitest'
import { userEvent } from 'vitest/browser'
import { render } from 'vitest-browser-react'

import type { SourceDiscoveryData } from '@/routes/source-discovery'
import type { SourcesActionData } from '@/routes/sources-action'

import { SourceCreateDialog } from './source-create'

type SuccessfulDiscovery = Extract<SourceDiscoveryData, { status: 'success' }>
type DiscoveryLoader = (request: Request) => SourceDiscoveryData | Promise<SourceDiscoveryData>

const DISCOVERY: SuccessfulDiscovery = {
  auth: { kind: 'bearer', label: 'a bearer token' },
  description: 'Weather observations and forecasts',
  format: 'openapi-yaml' as const,
  name: 'weather_api',
  serverUrl: 'https://weather.example/v1',
  status: 'success' as const,
  title: 'Weather API',
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
  discovery: SuccessfulDiscovery | DiscoveryLoader = DISCOVERY,
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
        loader: ({ request }) =>
          typeof discovery === 'function'
            ? discovery(request)
            : { ...discovery, auth: { ...discovery.auth } },
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

  it('confirms before discarding a draft with user-entered information', async () => {
    const { router, screen } = await renderSourceCreate()

    await screen.getByLabelText('Source URL').fill('https://example.com/openapi.yaml')
    await screen.getByRole('button', { name: 'Cancel' }).click()

    const confirmation = screen.getByRole('dialog', { name: 'Discard source draft?' })
    await expect.element(confirmation).toBeVisible()
    await confirmation.getByRole('button', { name: 'Keep editing' }).click()
    await expect.element(confirmation).not.toBeInTheDocument()
    expect(router.state.location.pathname).toBe('/workspaces/analytics/sources/install')

    await userEvent.keyboard('{Escape}')
    await screen
      .getByRole('dialog', { name: 'Discard source draft?' })
      .getByRole('button', { name: 'Discard' })
      .click()

    expect(router.state.location.pathname).toBe('/workspaces/analytics/sources')
  })

  it('does not submit the import form when Enter is pressed with an invalid URL', async () => {
    const action = vi.fn(async () => ({
      intent: 'import' as const,
      message: 'The import form should not have been submitted.',
      name: 'weather_api',
      status: 'error' as const,
    }))
    const { screen } = await renderSourceCreate(DISCOVERY, action)

    await screen.getByLabelText('Source URL').fill(DISCOVERY.url)
    await screen.getByRole('button', { name: 'Next' }).click()
    await expect.element(screen.getByText('Step 2/3')).toBeVisible()
    await screen.getByRole('button', { name: 'Back' }).click()

    const urlInput = screen.getByLabelText('Source URL')
    await urlInput.fill('oioioi')
    urlInput.element().focus()
    await userEvent.keyboard('{Enter}')

    expect(action).not.toHaveBeenCalled()
    await expect.element(screen.getByText('Step 1/3')).toBeVisible()
    await expect.element(screen.getByText('Step 2/3')).not.toBeInTheDocument()
  })

  it('opens each wizard step as a nested dialog', async () => {
    const { screen } = await renderSourceCreate()

    await expect.poll(activeDialogCount).toBe(1)

    await screen.getByLabelText('Source URL').fill(DISCOVERY.url)
    await screen.getByRole('button', { name: 'Next' }).click()

    await expect.element(screen.getByText('Step 2/3')).toBeVisible()
    await expect.poll(activeDialogCount).toBe(2)
    await expect.element(screen.getByLabelText('Name')).toHaveValue('weather_api')
    await expect
      .element(screen.getByLabelText('Description (optional)'))
      .toHaveValue('Weather observations and forecasts')
    await expect
      .element(
        screen.getByText(
          'Detected the name, description, base URL, and type from the URL provided. Review the details below.',
        ),
      )
      .toBeVisible()

    await screen.getByRole('button', { name: 'Next' }).click()

    await expect.element(screen.getByText('Step 3/3')).toBeVisible()
    await expect.poll(activeDialogCount).toBe(3)

    await screen.getByRole('button', { name: 'Back' }).click()

    await expect.element(screen.getByText('Step 2/3')).toBeVisible()
    await expect.poll(activeDialogCount).toBe(2)
  })

  it('resets source-specific draft state when the URL changes', async () => {
    const discovery: SuccessfulDiscovery = {
      ...DISCOVERY,
      auth: {
        headerName: 'X-Weather-Key',
        kind: 'header',
        label: 'an API key in the X-Weather-Key header',
      },
    }
    const { screen } = await renderSourceCreate(discovery)

    await screen.getByLabelText('Source URL').fill(discovery.url)
    await screen.getByRole('button', { name: 'Next' }).click()
    await expect
      .element(screen.getByLabelText('Description (optional)'))
      .toHaveValue(discovery.description)

    await screen.getByRole('button', { name: 'Next' }).click()
    await expect.element(screen.getByLabelText('Header name')).toHaveValue('X-Weather-Key')
    activeTokenInput().focus()
    await userEvent.keyboard('weather-secret')

    await screen.getByRole('button', { name: 'Back' }).click()
    await expect.element(screen.getByText('Step 3/3')).not.toBeInTheDocument()
    await screen.getByRole('button', { name: 'Back' }).click()
    await expect.element(screen.getByText('Step 2/3')).not.toBeInTheDocument()
    const replacementUrl = 'https://status.example/openapi.yaml'
    Object.assign(discovery, {
      auth: { kind: 'unknown', label: '' },
      description: '',
      name: 'status_api',
      serverUrl: 'https://status.example/v2',
      url: replacementUrl,
    })
    await screen.getByLabelText('Source URL').fill(replacementUrl)
    await screen.getByRole('button', { name: 'Next' }).click()

    await expect.element(screen.getByLabelText('Name')).toHaveValue('status_api')
    await expect.element(screen.getByLabelText('Description (optional)')).toHaveValue('')
    await expect.element(screen.getByLabelText('Base URL')).toHaveValue('https://status.example/v2')

    await screen.getByRole('button', { name: 'Next' }).click()
    await expect.element(screen.getByRole('radio', { name: 'Bearer token' })).toBeChecked()
    expect(activeTokenInput().value).toBe('')
    await screen.getByRole('radio', { name: 'Custom header' }).click()
    await expect.element(screen.getByLabelText('Header name')).toHaveValue('')
  })

  it('ignores a discovery response after the URL changes', async () => {
    const replacement: SuccessfulDiscovery = {
      auth: { kind: 'unknown', label: '' },
      description: '',
      format: 'unknown',
      name: 'status_api',
      serverUrl: '',
      status: 'success',
      title: '',
      url: 'https://status.example/openapi.yaml',
    }
    let resolveFirst: ((result: SourceDiscoveryData) => void) | undefined
    const firstRequest = new Promise<SourceDiscoveryData>((resolve) => {
      resolveFirst = resolve
    })
    const { screen } = await renderSourceCreate((request) => {
      const url = new URL(request.url).searchParams.get('url')
      return url === DISCOVERY.url ? firstRequest : replacement
    })

    await screen.getByLabelText('Source URL').fill(DISCOVERY.url)
    await screen.getByRole('button', { name: 'Next' }).click()
    await expect.element(screen.getByRole('button', { name: 'Inspecting…' })).toBeVisible()
    await screen.getByLabelText('Source URL').fill(replacement.url)
    if (!resolveFirst) throw new Error('First discovery request did not start')
    resolveFirst(DISCOVERY)

    await expect.element(screen.getByRole('button', { name: 'Next' })).toBeVisible()
    await expect.element(screen.getByText('Step 2/3')).not.toBeInTheDocument()
    await screen.getByRole('button', { name: 'Next' }).click()

    await expect.element(screen.getByLabelText('Name')).toHaveValue('status_api')
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
      serverUrl: '',
      status: 'success',
      title: '',
      url,
    })

    await screen.getByLabelText('Source URL').fill(url)
    await screen.getByRole('button', { name: 'Next' }).click()

    await expect
      .element(screen.getByText('No OpenAPI document was detected. Fill in the details below.'))
      .toBeVisible()
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
      serverUrl: '',
      status: 'success',
      title: '',
      url,
    })

    await screen.getByLabelText('Source URL').fill(url)
    await screen.getByRole('button', { name: 'Next' }).click()

    await expect
      .element(screen.getByText('Detected an MCP endpoint from its URL. Review the details below.'))
      .toBeVisible()
    await expect.element(screen.getByRole('radio', { name: 'MCP server' })).toBeChecked()
  })

  it('prefills detected header authentication on the credentials step', async () => {
    const { screen } = await renderSourceCreate({
      ...DISCOVERY,
      auth: {
        headerName: 'X-Api-Key',
        kind: 'header',
        label: 'an API key in the X-Api-Key header',
      },
    })

    await screen.getByLabelText('Source URL').fill(DISCOVERY.url)
    await screen.getByRole('button', { name: 'Next' }).click()
    await expect.element(screen.getByLabelText('Name')).toBeVisible()
    await screen.getByRole('button', { name: 'Next' }).click()

    await expect
      .element(screen.getByText('Detected an API key in the X-Api-Key header.'))
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
    expect(manifest).toContain('\n  base_url: "https://weather.example/v1"\n')
    expect(manifest).not.toContain('\nsurfaces:\n')
  })

  it('blocks an OpenAPI source until the base URL is a valid URL', async () => {
    const { screen } = await renderSourceCreate({ ...DISCOVERY, serverUrl: '' })

    await screen.getByLabelText('Source URL').fill(DISCOVERY.url)
    await screen.getByRole('button', { name: 'Next' }).click()
    await expect.element(screen.getByLabelText('Base URL')).toHaveValue('')
    await expect.element(screen.getByRole('button', { name: 'Next' })).toBeDisabled()

    const baseUrlInput = screen.getByLabelText('Base URL')
    await baseUrlInput.fill('weather.example')
    await userEvent.tab()
    await expect.element(screen.getByText('Enter a valid URL, including the scheme.')).toBeVisible()
    await expect.element(screen.getByRole('button', { name: 'Next' })).toBeDisabled()

    await baseUrlInput.fill('https://weather.example/v1')
    await expect.element(screen.getByRole('button', { name: 'Next' })).toBeEnabled()
  })

  it('disables the base URL for MCP sources and omits it from the manifest', async () => {
    const { screen } = await renderSourceCreate()

    await screen.getByLabelText('Source URL').fill(DISCOVERY.url)
    await screen.getByRole('button', { name: 'Next' }).click()
    await expect.element(screen.getByLabelText('Base URL')).toBeEnabled()

    await screen.getByRole('radio', { name: 'MCP server' }).click()
    await expect.element(screen.getByLabelText('Base URL')).toBeDisabled()
    await expect
      .element(
        screen.getByText(
          'MCP servers are reached at the source URL, so they have no separate base URL.',
        ),
      )
      .toBeVisible()
    expect(submittedManifest()).not.toContain('base_url')

    await screen.getByRole('radio', { name: 'REST API (OpenAPI)' }).click()
    await expect.element(screen.getByLabelText('Base URL')).toBeEnabled()
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
    await expect
      .element(screen.getByText('This endpoint doesn’t require credentials.'))
      .toBeVisible()
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

function activeTokenInput() {
  const input = [
    ...document.querySelectorAll<HTMLInputElement>('input[placeholder="Paste token"]'),
  ].find((candidate) => !candidate.disabled)
  if (!input) throw new Error('Active token input not found')
  return input
}
