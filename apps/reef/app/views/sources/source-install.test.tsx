import { Outlet, RouterProvider, createMemoryRouter, useRouteLoaderData } from 'react-router'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { render } from 'vitest-browser-react'

import { oauthInstallEventToNdjson } from '@/lib/source-oauth-install-stream'
import type { CatalogEntry } from '@/lib/sources'
import { shouldRevalidate } from '@/routes/sources'

import { SourceInstallDialog } from './source-install'

const entry: CatalogEntry = {
  description: 'Query GitHub data.',
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
  installed: false,
  name: 'github',
  origin: 'bundled',
  version: '1.0.0',
}

const entryWithMultipleSetupMethods: CatalogEntry = {
  ...entry,
  inputSpecs: [
    {
      hint: 'Choose how to connect.',
      input: {
        case: 'secret',
        value: {
          credential: {
            methods: [
              {
                description: '',
                hint: '',
                label: 'Personal access token',
                method: { case: 'sourceConfig', value: {} },
              },
              {
                description: '',
                hint: '',
                label: 'OAuth',
                method: {
                  case: 'oauth',
                  value: {
                    client: {
                      id: { defaultValue: '', input: 'GITHUB_CLIENT_ID' },
                      secret: { input: 'GITHUB_CLIENT_SECRET' },
                    },
                  },
                },
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

afterEach(() => vi.unstubAllGlobals())

describe('SourceInstallDialog', () => {
  it('uses Wax tabs to switch between source setup methods', async () => {
    const router = createMemoryRouter(
      [
        {
          element: (
            <SourceInstallDialog
              entry={entryWithMultipleSetupMethods}
              open
              onOpenChange={() => undefined}
            />
          ),
          path: '/',
        },
      ],
      { initialEntries: ['/'] },
    )
    const screen = await render(<RouterProvider router={router} />)
    const tokenTab = screen.getByRole('tab', { name: 'Personal access token' })
    const oauthTab = screen.getByRole('tab', { name: 'OAuth' })
    const dialog = screen.getByRole('dialog')

    await expect
      .element(screen.getByRole('tablist', { name: 'Github token setup method' }))
      .toBeVisible()
    await expect.element(tokenTab).toHaveAttribute('aria-selected', 'true')
    const tokenPanel = dialog
      .element()
      .querySelector<HTMLElement>('[role="tabpanel"]:not([hidden])')
    const tokenInput = tokenPanel?.querySelector('input')
    const hint = tokenPanel?.querySelector('p')
    expect(tokenInput).not.toBeNull()
    expect(tokenInput).toHaveAttribute('aria-label', 'Github token')
    expect(hint).not.toBeNull()
    expect(dialog.element().textContent).not.toContain('Github token')
    expect(
      hint!.getBoundingClientRect().top - tokenInput!.getBoundingClientRect().bottom,
    ).toBeLessThan(12)
    const initialDialogHeight = dialog.element().getBoundingClientRect().height

    await oauthTab.click()

    await expect.element(oauthTab).toHaveAttribute('aria-selected', 'true')
    const oauthPanel = dialog
      .element()
      .querySelector<HTMLElement>('[role="tabpanel"]:not([hidden])')
    expect(oauthPanel?.textContent).toContain('Github client id')
    expect(dialog.element().getBoundingClientRect().height).toBe(initialDialogHeight)

    const panels = dialog.element().querySelectorAll<HTMLElement>('[role="tabpanel"]')
    expect(panels).toHaveLength(2)
    expect(panels[0]).toHaveAttribute('hidden')
    expect(panels[0].querySelector('input')).toBeDisabled()
  })

  it('keeps the field label when there is only one setup method', async () => {
    const router = createMemoryRouter(
      [
        {
          element: <SourceInstallDialog entry={entry} open onOpenChange={() => undefined} />,
          path: '/',
        },
      ],
      { initialEntries: ['/'] },
    )
    const screen = await render(<RouterProvider router={router} />)

    await expect.element(screen.getByText('Github token')).toBeVisible()
  })

  it('commits catalog revalidation before leaving the detail route', async () => {
    let installed = false
    let loaderCalls = 0
    const fetchOAuthInstall = vi.fn(async () => {
      installed = true
      return streamedResponse([
        oauthInstallEventToNdjson({ type: 'source', name: 'github', version: '1.0.0' }),
      ])
    })
    vi.stubGlobal('fetch', fetchOAuthInstall)

    const router = createMemoryRouter(
      [
        {
          children: [
            { index: true, element: <CatalogStatus /> },
            {
              element: (
                <SourceInstallDialog
                  entry={entry}
                  open
                  onOpenChange={() => undefined}
                  workspaceId="analytics"
                />
              ),
              path: ':sourceName',
            },
          ],
          element: <Outlet />,
          id: 'sources',
          loader: async ({ request }) => {
            const snapshot = installed
            loaderCalls += 1
            await abortableDelay(request.signal)
            return { installed: snapshot }
          },
          path: '/workspaces/:workspaceId/sources',
          shouldRevalidate,
        },
      ],
      {
        hydrationData: { loaderData: { sources: { installed: false } } },
        initialEntries: ['/workspaces/analytics/sources/github'],
      },
    )
    const screen = await render(<RouterProvider router={router} />)

    await screen.getByRole('button', { name: 'Add source' }).click()

    await expect.element(screen.getByText('Configured')).toBeVisible()
    expect(fetchOAuthInstall).toHaveBeenCalledWith(
      '/workspaces/analytics/sources/github/oauth-install',
      expect.objectContaining({ method: 'POST' }),
    )
    expect(router.state.location.pathname).toBe('/workspaces/analytics/sources')
    expect(loaderCalls).toBe(1)
  })
})

function CatalogStatus() {
  const data = useRouteLoaderData('sources') as { installed: boolean }
  return <div>{data.installed ? 'Configured' : 'Installable'}</div>
}

function abortableDelay(signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const timeout = window.setTimeout(resolve, 25)
    signal.addEventListener(
      'abort',
      () => {
        window.clearTimeout(timeout)
        reject(signal.reason)
      },
      { once: true },
    )
  })
}

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
