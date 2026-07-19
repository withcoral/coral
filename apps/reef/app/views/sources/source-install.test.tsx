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

afterEach(() => vi.unstubAllGlobals())

describe('SourceInstallDialog', () => {
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
