import { createMemoryRouter, Outlet, RouterProvider } from 'react-router'
import { expect, it } from 'vitest'
import { render } from 'vitest-browser-react'

import type { SchemaResponse } from '@/lib/schema-explorer'
import { routePath, routePattern } from '@/routing/routemap'

import { SchemaTableView } from './schema-table'

const SCHEMA = {
  namespaces: [
    {
      kind: 'schema',
      name: 'github',
      items: [
        {
          columns: [],
          columnsLoaded: false,
          kind: 'table',
          name: 'issues',
          requiredFilters: [],
        },
      ],
    },
    {
      kind: 'catalog',
      name: 'pickl_v4',
      schemas: [
        {
          name: 'public',
          items: [
            {
              columns: [],
              columnsLoaded: false,
              kind: 'table',
              name: 'products',
              requiredFilters: [],
            },
          ],
        },
      ],
    },
  ],
} satisfies SchemaResponse

function tableRouter(path: string) {
  return createMemoryRouter(
    [
      {
        children: [
          {
            element: <SchemaTableView columns={[]} />,
            path: 'catalogs/:catalogName/:schemaName/:tableName',
          },
          {
            element: <SchemaTableView columns={[]} />,
            path: ':schemaName/:tableName',
          },
        ],
        element: <Outlet context={SCHEMA} />,
        path: routePattern('workspaceSchema'),
      },
    ],
    { initialEntries: [path] },
  )
}

it('shows the fully qualified database table name', async () => {
  const router = tableRouter(
    routePath('workspaceSchemaCatalogTable', {
      catalogName: 'pickl_v4',
      schemaName: 'public',
      tableName: 'products',
      workspaceId: 'analytics',
    }),
  )
  const screen = await render(<RouterProvider router={router} />)

  await expect
    .element(screen.getByRole('heading', { name: 'pickl_v4.public.products' }))
    .toBeVisible()
})

it('keeps the existing two-part table heading', async () => {
  const router = tableRouter(
    routePath('workspaceSchemaTable', {
      schemaName: 'github',
      tableName: 'issues',
      workspaceId: 'analytics',
    }),
  )
  const screen = await render(<RouterProvider router={router} />)

  await expect.element(screen.getByRole('heading', { name: 'github.issues' })).toBeVisible()
})
