import { createMemoryRouter, RouterProvider } from 'react-router'
import { describe, expect, it } from 'vitest'
import { render } from 'vitest-browser-react'

import { SourceDetailView } from './source-detail'

describe('SourceDetailView', () => {
  it('renders the source dialog from route data passed by the adapter', async () => {
    const router = createMemoryRouter(
      [
        {
          element: (
            <SourceDetailView
              actionData={undefined}
              loaderData={{
                entry: {
                  description: 'Query GitHub data.',
                  installed: false,
                  name: 'github',
                  origin: 'bundled',
                  version: '1.0.0',
                },
                loadError: null,
              }}
              sourcesPath="/workspaces/default/sources"
            />
          ),
          path: '/workspaces/:workspaceId/sources/:sourceName',
        },
      ],
      { initialEntries: ['/workspaces/default/sources/github'] },
    )

    const screen = await render(<RouterProvider router={router} />)

    await expect.element(screen.getByRole('dialog')).toBeVisible()
    await expect.element(screen.getByRole('button', { name: 'Add source' })).toBeVisible()
  })
})
