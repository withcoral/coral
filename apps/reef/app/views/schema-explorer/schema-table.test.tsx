import { createMemoryRouter, Outlet, RouterProvider } from 'react-router'
import { expect, it } from 'vitest'
import { render } from 'vitest-browser-react'

import type { SchemaResponse } from '@/lib/schema-explorer'
import { routePath, routePattern } from '@/routing/routemap'

import { SchemaTableView } from './schema-table'

it('shows the catalog in a catalog-qualified table heading', async () => {
  const schema = {
    connectors: [
      {
        catalogName: 'github_v4',
        items: [
          {
            columns: [],
            columnsLoaded: false,
            kind: 'table',
            name: 'issues',
            requiredFilters: [],
          },
        ],
        name: 'api',
      },
    ],
  } satisfies SchemaResponse
  const router = createMemoryRouter(
    [
      {
        children: [
          {
            element: <SchemaTableView columns={[]} />,
            path: 'catalogs/:catalogName/:schemaName/:tableName',
          },
        ],
        element: <Outlet context={schema} />,
        path: routePattern('workspaceSchema'),
      },
    ],
    {
      initialEntries: [
        routePath('workspaceCatalogSchemaTable', {
          catalogName: 'github_v4',
          schemaName: 'api',
          tableName: 'issues',
          workspaceId: 'analytics',
        }),
      ],
    },
  )
  const screen = await render(<RouterProvider router={router} />)

  await expect.element(screen.getByRole('heading', { name: 'github_v4.api.issues' })).toBeVisible()
})
