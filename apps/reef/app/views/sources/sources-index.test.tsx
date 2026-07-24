import { createMemoryRouter, RouterProvider } from 'react-router'
import { describe, expect, it } from 'vitest'
import { render } from 'vitest-browser-react'

import type { CatalogEntry } from '@/lib/sources'

import { SourcesIndex } from './sources-index'

function renderSourcesIndex(entries: CatalogEntry[]) {
  const router = createMemoryRouter(
    [
      {
        element: <SourcesIndex entries={entries} workspaceId="team alpha" />,
        path: '/',
      },
    ],
    { initialEntries: ['/'] },
  )

  return render(<RouterProvider router={router} />)
}

describe('SourcesIndex', () => {
  it('links source creation to the dedicated install route', async () => {
    const screen = await renderSourcesIndex([])

    await expect
      .element(screen.getByRole('link', { name: 'Create source' }))
      .toHaveAttribute('href', '/workspaces/team%20alpha/sources/install')
  })

  it('routes installable sources through the source detail route', async () => {
    const screen = await renderSourcesIndex([
      {
        description: 'Query GitHub data.',
        installed: false,
        name: 'github',
        origin: 'bundled',
        version: '1.0.0',
      },
    ])

    await expect
      .element(screen.getByRole('link', { name: /github/i }))
      .toHaveAttribute('href', '/workspaces/team%20alpha/sources/github')
  })

  it('encodes source names before routing through the source detail route', async () => {
    const screen = await renderSourcesIndex([
      {
        description: 'Reserved URL characters should stay inside the source name.',
        installed: false,
        name: 'foo?bar',
        origin: 'imported',
        version: '1.0.0',
      },
    ])

    await expect
      .element(screen.getByRole('link', { name: /foo\?bar/i }))
      .toHaveAttribute('href', '/workspaces/team%20alpha/sources/foo%3Fbar')
  })
})
