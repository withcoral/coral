import { tableFromArrays, tableToIPC } from 'apache-arrow'
import { createMemoryRouter, RouterProvider, useActionData, useLoaderData } from 'react-router'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render } from 'vitest-browser-react'

import type { CatalogEntry } from '@/lib/sources'

const { executeSql } = vi.hoisted(() => ({ executeSql: vi.fn() }))
const WORKSPACE_ID = 'default'

vi.mock('@/lib/coral-clients', () => ({
  getQueryClient: async () => ({ executeSql }),
}))

import OnboardingRoute, { SourcesStep } from './onboarding'

function TestOnboardingRoute() {
  const props = {
    actionData: useActionData(),
    loaderData: useLoaderData(),
  } as Parameters<typeof OnboardingRoute>[0]
  return <OnboardingRoute {...props} />
}

const github: CatalogEntry = {
  description: 'Query repositories, pull requests, and issues.',
  inputSpecs: [],
  installed: false,
  name: 'github',
  origin: 'bundled',
  version: '1.0.0',
}

const installedGithub: CatalogEntry = {
  ...github,
  installed: true,
  source: {
    name: 'github',
    origin: 'bundled',
    secrets: [],
    variables: [],
    version: '1.0.0',
  },
}

describe('onboarding sources step', () => {
  beforeEach(() => {
    executeSql.mockReset()
  })

  it('opens the installation dialog when an unconfigured source card is clicked', async () => {
    const router = createMemoryRouter(
      [
        {
          action: () => null,
          element: <SourcesStep actionData={undefined} entries={[github]} loadError={null} />,
          path: '/',
        },
      ],
      { initialEntries: ['/'] },
    )
    const screen = await render(<RouterProvider router={router} />)

    await screen.getByRole('button', { name: /github/i }).click()

    await expect.element(screen.getByRole('dialog')).toBeVisible()
    await expect.element(screen.getByRole('button', { name: 'Add source' })).toBeVisible()
  })

  it('opens the source manager when a configured source card is clicked', async () => {
    const router = createMemoryRouter(
      [
        {
          action: () => null,
          element: (
            <SourcesStep actionData={undefined} entries={[installedGithub]} loadError={null} />
          ),
          path: '/',
        },
      ],
      { initialEntries: ['/'] },
    )
    const screen = await render(<RouterProvider router={router} />)

    await screen.getByRole('button', { name: /github/i }).click()

    await expect.element(screen.getByRole('dialog')).toBeVisible()
    await expect.element(screen.getByRole('button', { name: 'Remove' })).toBeVisible()
    await expect.element(screen.getByRole('button', { name: 'Close' })).toBeVisible()
  })

  it('removes a configured source and makes it available to add again', async () => {
    let installed = true
    const submittedAction = vi.fn()
    const router = createMemoryRouter(
      [
        {
          action: async ({ request }) => {
            const formData = await request.formData()
            submittedAction(formData.get('_intent'), formData.get('name'))
            installed = false
            return { intent: 'delete', name: 'github', status: 'success' }
          },
          Component: TestOnboardingRoute,
          loader: () => ({
            entries: [installed ? installedGithub : github],
            loadError: null,
            workspaceId: WORKSPACE_ID,
          }),
          path: '/onboarding',
        },
      ],
      { initialEntries: ['/onboarding'] },
    )
    const screen = await render(<RouterProvider router={router} />)

    await screen.getByRole('button', { name: /github/i }).click()
    await screen.getByRole('button', { name: 'Remove' }).click()
    const confirmation = screen.getByRole('dialog', { name: 'Remove github?' })
    await confirmation.getByRole('button', { name: 'Remove' }).click()

    await expect.element(screen.getByRole('dialog')).not.toBeInTheDocument()
    expect(submittedAction).toHaveBeenCalledWith('delete', 'github')

    await screen.getByRole('button', { name: /github/i }).click()
    await expect.element(screen.getByRole('button', { name: 'Add source' })).toBeVisible()
  })

  it('does not reopen a dismissed removal error', async () => {
    const router = createMemoryRouter(
      [
        {
          action: () => ({
            intent: 'delete',
            message: 'Removal failed',
            name: 'github',
            status: 'error',
          }),
          Component: TestOnboardingRoute,
          loader: () => ({
            entries: [installedGithub],
            loadError: null,
            workspaceId: WORKSPACE_ID,
          }),
          path: '/onboarding',
        },
      ],
      { initialEntries: ['/onboarding'] },
    )
    const screen = await render(<RouterProvider router={router} />)

    await screen.getByRole('button', { name: /github/i }).click()
    await screen.getByRole('button', { name: 'Remove' }).click()
    const confirmation = screen.getByRole('dialog', { name: 'Remove github?' })
    await confirmation.getByRole('button', { name: 'Remove' }).click()

    await expect.element(confirmation.getByText('Removal failed')).toBeVisible()
    await confirmation.getByRole('button', { name: 'Cancel' }).click()
    await expect.element(confirmation).not.toBeInTheDocument()
    await screen.getByRole('button', { name: 'Close' }).click()

    await screen.getByRole('button', { name: /github/i }).click()

    await expect.element(screen.getByRole('dialog')).toBeVisible()
    await expect
      .element(screen.getByRole('dialog', { name: 'Remove github?' }))
      .not.toBeInTheDocument()
    await expect.element(screen.getByText('Removal failed')).not.toBeInTheDocument()
  })

  it('closes the installation dialog after a successful source install', async () => {
    const router = createMemoryRouter(
      [
        {
          action: () => ({ intent: 'install', name: 'github', status: 'success' }),
          Component: TestOnboardingRoute,
          loader: () => ({ entries: [github], loadError: null, workspaceId: WORKSPACE_ID }),
          path: '/onboarding',
        },
      ],
      { initialEntries: ['/onboarding'] },
    )
    const screen = await render(<RouterProvider router={router} />)

    await screen.getByRole('button', { name: /github/i }).click()
    await screen.getByRole('button', { name: 'Add source' }).click()

    await expect.element(screen.getByRole('dialog')).not.toBeInTheDocument()

    await screen.getByRole('button', { name: /github/i }).click()
    await expect.element(screen.getByRole('dialog')).toBeVisible()
  })

  it('keeps the installation dialog open when source installation fails and clears a dismissed error', async () => {
    const router = createMemoryRouter(
      [
        {
          action: () => ({
            intent: 'install',
            message: 'Installation failed',
            name: 'github',
            status: 'error',
          }),
          Component: TestOnboardingRoute,
          loader: () => ({ entries: [github], loadError: null, workspaceId: WORKSPACE_ID }),
          path: '/onboarding',
        },
      ],
      { initialEntries: ['/onboarding'] },
    )
    const screen = await render(<RouterProvider router={router} />)

    await screen.getByRole('button', { name: /github/i }).click()
    await screen.getByRole('button', { name: 'Add source' }).click()

    await expect.element(screen.getByRole('dialog')).toBeVisible()
    await expect.element(screen.getByText('Installation failed')).toBeVisible()

    await screen.getByRole('button', { name: 'Cancel' }).click()
    await screen.getByRole('button', { name: /github/i }).click()

    await expect.element(screen.getByRole('dialog')).toBeVisible()
    await expect.element(screen.getByText('Installation failed')).not.toBeInTheDocument()

    await screen.getByRole('button', { name: 'Add source' }).click()
    await expect.element(screen.getByText('Installation failed')).toBeVisible()
  })

  it('recovers when sources fail to reload on the query step', async () => {
    executeSql.mockResolvedValue({
      arrowIpcStream: tableToIPC(
        tableFromArrays({ source: ['github'], tables: BigInt64Array.from([1n]) }),
      ),
    })
    let loadCount = 0
    const loadSources = vi.fn(() => {
      loadCount += 1
      if (loadCount === 2) {
        return { entries: [], loadError: 'Source catalog unavailable', workspaceId: WORKSPACE_ID }
      }
      return { entries: [installedGithub], loadError: null, workspaceId: WORKSPACE_ID }
    })
    const router = createMemoryRouter(
      [
        {
          Component: TestOnboardingRoute,
          loader: loadSources,
          path: '/onboarding',
        },
      ],
      { initialEntries: ['/onboarding'] },
    )
    const screen = await render(<RouterProvider router={router} />)

    await screen.getByRole('link', { name: 'I have connected enough sources' }).click()

    await expect.element(screen.getByText("Couldn't load sources")).toBeVisible()
    await expect.element(screen.getByText('Source catalog unavailable')).toBeVisible()
    await expect.element(screen.getByText('No connected sources')).not.toBeInTheDocument()

    await screen.getByRole('button', { name: 'Retry' }).click()

    await expect.element(screen.getByRole('button', { name: 'Finish setup' })).toBeEnabled()
    expect(loadSources).toHaveBeenCalledTimes(3)
    expect(executeSql).toHaveBeenCalledOnce()
  })

  it('enters the normal app without persisting completion', async () => {
    executeSql.mockResolvedValue({
      arrowIpcStream: tableToIPC(
        tableFromArrays({ source: ['github'], tables: BigInt64Array.from([1n]) }),
      ),
    })
    const props = {
      actionData: undefined,
      loaderData: { entries: [installedGithub], loadError: null, workspaceId: WORKSPACE_ID },
    } as Parameters<typeof OnboardingRoute>[0]
    const router = createMemoryRouter(
      [
        { element: <OnboardingRoute {...props} />, path: '/onboarding' },
        { element: <p>Normal app</p>, path: `/workspaces/${WORKSPACE_ID}/sources` },
      ],
      { initialEntries: ['/onboarding?step=query'] },
    )
    const screen = await render(<RouterProvider router={router} />)
    const finish = screen.getByRole('button', { name: 'Finish setup' })

    await expect.element(finish).toBeEnabled()
    await finish.click()

    await expect.element(screen.getByText('Normal app')).toBeVisible()
    expect(executeSql).toHaveBeenCalledOnce()
  })
})
