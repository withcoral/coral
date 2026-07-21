import { createMemoryRouter, RouterProvider, useActionData, useLoaderData } from 'react-router'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { render } from 'vitest-browser-react'

import { coralAgentSetupPrompt } from '@/components/onboarding/onboarding-next-steps-page'
import { oauthInstallEventToNdjson } from '@/lib/source-oauth-install-stream'
import type { CatalogEntry } from '@/lib/sources'
import { getOnboardingStepState } from '@/components/onboarding/onboarding-steps'
import { OnboardingView, SourcesStep } from '@/views/onboarding/onboarding'

const WORKSPACE_ID = 'default'
const sourcesStep = getOnboardingStepState('sources')
const queryStep = getOnboardingStepState('query')

import OnboardingRoute from './onboarding'

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

const oauthGithub: CatalogEntry = {
  ...github,
  inputSpecs: [
    {
      hint: '',
      input: {
        case: 'secret',
        value: {
          credential: {
            methods: [
              {
                description: '',
                hint: '',
                label: 'OAuth',
                method: { case: 'oauth', value: {} },
              },
            ],
          },
        },
      },
      key: 'GITHUB_TOKEN',
      required: true,
    },
  ],
}

afterEach(() => {
  vi.unstubAllGlobals()
  delete window.coralDesktop
})

describe('onboarding sources step', () => {
  it('opens the installation dialog when an unconfigured source card is clicked', async () => {
    const router = createMemoryRouter(
      [
        {
          action: () => null,
          element: (
            <SourcesStep
              actionData={undefined}
              entries={[github]}
              loadError={null}
              step={sourcesStep}
              workspaceId={WORKSPACE_ID}
            />
          ),
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
            <SourcesStep
              actionData={undefined}
              entries={[installedGithub]}
              loadError={null}
              step={sourcesStep}
              workspaceId={WORKSPACE_ID}
            />
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
            sampleQuery: null,
            step: sourcesStep,
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
            sampleQuery: null,
            step: sourcesStep,
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
          loader: () => ({
            entries: [github],
            loadError: null,
            sampleQuery: null,
            step: sourcesStep,
            workspaceId: WORKSPACE_ID,
          }),
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
          loader: () => ({
            entries: [github],
            loadError: null,
            sampleQuery: null,
            step: sourcesStep,
            workspaceId: WORKSPACE_ID,
          }),
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

  it('stays in onboarding after an OAuth source install', async () => {
    let installed = false
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => {
        installed = true
        return streamedResponse([
          oauthInstallEventToNdjson({ type: 'source', name: 'github', version: '1.0.0' }),
        ])
      }),
    )
    const router = createMemoryRouter(
      [
        {
          action: () => null,
          Component: TestOnboardingRoute,
          loader: () => ({
            entries: [installed ? installedGithub : oauthGithub],
            loadError: null,
            sampleQuery: null,
            step: sourcesStep,
            workspaceId: WORKSPACE_ID,
          }),
          path: '/onboarding',
        },
      ],
      { initialEntries: ['/onboarding'] },
    )
    const screen = await render(<RouterProvider router={router} />)

    await screen.getByRole('button', { name: /github/i }).click()
    await screen.getByRole('button', { name: 'Add source' }).click()

    await expect.element(screen.getByRole('dialog')).not.toBeInTheDocument()
    expect(router.state.location.pathname).toBe('/onboarding')
  })

  it('recovers when sources fail to reload on the query step', async () => {
    const runSampleQuery = vi.fn(() => ({
      rows: [{ source: 'github', tables: '1' }],
      status: 'success' as const,
    }))
    let loadCount = 0
    const loadSources = vi.fn(({ request }: { request: Request }) => {
      loadCount += 1
      if (loadCount === 2) {
        return {
          entries: [],
          loadError: 'Source catalog unavailable',
          sampleQuery: null,
          step: queryStep,
          workspaceId: WORKSPACE_ID,
        }
      }
      const step = getOnboardingStepState(new URL(request.url).searchParams.get('step'))
      return {
        entries: [installedGithub],
        loadError: null,
        sampleQuery: step.step === 'query' ? runSampleQuery() : null,
        step,
        workspaceId: WORKSPACE_ID,
      }
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

    await expect.element(screen.getByRole('button', { name: 'Continue' })).toBeEnabled()
    expect(loadSources).toHaveBeenCalledTimes(3)
    expect(runSampleQuery).toHaveBeenCalledOnce()
  })

  it('shows next steps before entering the normal app', async () => {
    let resolveSampleQuery: ((result: { rows: never[]; status: 'success' }) => void) | undefined
    const sampleQuery = new Promise<{ rows: never[]; status: 'success' }>((resolve) => {
      resolveSampleQuery = resolve
    })
    const getMcpLaunchConfig = vi.fn(async () => ({
      args: ['mcp-stdio'],
      command: '/Applications/Coral.app/Contents/Resources/coral/coral',
    }))
    window.coralDesktop = {
      configureMcp: vi.fn(),
      getMcpLaunchConfig,
      listMcpClients: vi.fn(),
    }
    const loadOnboarding = ({ request }: { request: Request }) => {
      const step = getOnboardingStepState(new URL(request.url).searchParams.get('step'))
      return {
        entries: [installedGithub],
        loadError: null,
        runtime: 'desktop' as const,
        sampleQuery: step.step === 'query' ? sampleQuery : null,
        step,
        workspaceId: WORKSPACE_ID,
      }
    }
    const router = createMemoryRouter(
      [
        {
          Component: TestOnboardingRoute,
          loader: loadOnboarding,
          path: '/onboarding',
        },
        { element: <p>Normal app</p>, path: `/workspaces/${WORKSPACE_ID}/sources` },
      ],
      { initialEntries: ['/onboarding?step=query'] },
    )
    const screen = await render(<RouterProvider router={router} />)
    const continueToNextSteps = screen.getByRole('button', {
      name: 'Continue',
    })

    await expect.element(screen.getByText('Running query')).toBeVisible()
    await expect.element(continueToNextSteps).toBeDisabled()
    resolveSampleQuery?.({ rows: [], status: 'success' })
    await expect.element(continueToNextSteps).toBeEnabled()
    await continueToNextSteps.click()

    await expect
      .element(screen.getByRole('tab', { name: 'AI-assisted' }))
      .toHaveAttribute('aria-selected', 'true')
    expect(
      screen.getByRole('textbox', { exact: true, name: 'Coral agent setup prompt' }).element()
        .textContent,
    ).toBe(coralAgentSetupPrompt('desktop'))
    await screen.getByRole('tab', { name: 'Manual' }).click()
    await expect
      .element(screen.getByLabelText('Coral MCP server configuration', { exact: true }))
      .toHaveTextContent('/Applications/Coral.app/Contents/Resources/coral/coral')
    expect(getMcpLaunchConfig).toHaveBeenCalledOnce()
    await screen.getByRole('button', { name: "Take me to Coral's dashboard" }).click()

    await expect.element(screen.getByText('Normal app')).toBeVisible()
  })

  it('shows sample query errors returned by the onboarding page loader', async () => {
    const props = {
      actionData: undefined,
      loaderData: {
        entries: [installedGithub],
        loadError: null,
        runtime: 'web' as const,
        sampleQuery: { message: 'Coral is unavailable', status: 'error' as const },
        step: queryStep,
        workspaceId: WORKSPACE_ID,
      },
    }
    const router = createMemoryRouter(
      [{ element: <OnboardingView {...props} />, path: '/onboarding' }],
      { initialEntries: ['/onboarding?step=query'] },
    )
    const screen = await render(<RouterProvider router={router} />)

    await expect.element(screen.getByText("Couldn't run the catalog query")).toBeVisible()
    await expect.element(screen.getByText('Coral is unavailable')).toBeVisible()
  })

  it('keeps a rejected deferred query inside the onboarding error state', async () => {
    let rejectSampleQuery: ((reason: Error) => void) | undefined
    const sampleQuery = new Promise<never>((_resolve, reject) => {
      rejectSampleQuery = reject
    })
    const props = {
      actionData: undefined,
      loaderData: {
        entries: [installedGithub],
        loadError: null,
        runtime: 'web' as const,
        sampleQuery,
        step: queryStep,
        workspaceId: WORKSPACE_ID,
      },
    }
    const router = createMemoryRouter(
      [{ element: <OnboardingView {...props} />, path: '/onboarding' }],
      { initialEntries: ['/onboarding?step=query'] },
    )
    const screen = await render(<RouterProvider router={router} />)

    await expect.element(screen.getByText('Running query')).toBeVisible()
    rejectSampleQuery?.(new Error('Server Timeout'))

    await expect.element(screen.getByText("Couldn't run the catalog query")).toBeVisible()
    await expect
      .element(screen.getByText("The sample query couldn't be completed. Try again."))
      .toBeVisible()
    await expect.element(screen.getByText('Oops!')).not.toBeInTheDocument()
  })
})

function streamedResponse(chunks: string[]): Response {
  const encoder = new TextEncoder()
  return new Response(
    new ReadableStream<Uint8Array>({
      start(controller) {
        for (const chunk of chunks) controller.enqueue(encoder.encode(chunk))
        controller.close()
      },
    }),
  )
}
