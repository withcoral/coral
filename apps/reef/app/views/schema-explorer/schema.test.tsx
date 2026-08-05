import { createMemoryRouter, RouterProvider } from 'react-router'
import { expect, it } from 'vitest'
import { render } from 'vitest-browser-react'

import type { SchemaResponse } from '@/lib/schema-explorer'
import { routePath, routePattern } from '@/routing/routemap'

import { SchemaExplorer } from './schema'

const SCHEMA = {
  namespaces: [
    {
      kind: 'schema',
      name: 'github?api',
      items: [
        {
          columns: [],
          columnsLoaded: false,
          kind: 'table',
          name: 'issues/closed',
          requiredFilters: [],
        },
        {
          arguments: [
            { name: 'channel', required: true, values: [] },
            { name: 'cursor', required: false, values: [] },
          ],
          kind: 'tableFunction',
          name: 'messages',
          resultColumns: [],
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
            {
              columns: [],
              columnsLoaded: false,
              kind: 'table',
              name: 'sales',
              requiredFilters: [],
            },
          ],
        },
        {
          name: 'analytics',
          items: [
            {
              columns: [],
              columnsLoaded: false,
              kind: 'table',
              name: 'revenue_by_product',
              requiredFilters: [],
            },
          ],
        },
      ],
    },
  ],
} satisfies SchemaResponse

it('links tables within the active workspace schema route', async () => {
  const workspaceId = 'team alpha'
  const router = createMemoryRouter(
    [
      {
        element: <SchemaExplorer schema={SCHEMA} workspaceId={workspaceId} />,
        path: routePattern('workspaceSchema'),
      },
    ],
    { initialEntries: [routePath('workspaceSchema', { workspaceId })] },
  )
  const screen = await render(<RouterProvider router={router} />)

  await expect.element(screen.getByPlaceholder('Filter schemas and tables')).toBeVisible()
  await expect.element(screen.getByText(/schemas \/.*tables/)).not.toBeInTheDocument()
  await screen.getByRole('button', { name: /github\?api/ }).click()
  await expect.element(screen.getByRole('link', { name: 'issues/closed' })).toHaveAttribute(
    'href',
    routePath('workspaceSchemaTable', {
      schemaName: 'github?api',
      tableName: 'issues/closed',
      workspaceId,
    }),
  )
  await expect
    .element(screen.getByRole('link', { name: 'messages(channel, ...)' }))
    .toHaveAttribute(
      'href',
      routePath('workspaceSchemaTableFunction', {
        functionName: 'messages',
        schemaName: 'github?api',
        workspaceId,
      }),
    )
})

it('renders and links database catalogs through their provider schemas', async () => {
  const workspaceId = 'team alpha'
  const router = createMemoryRouter(
    [
      {
        element: <SchemaExplorer schema={SCHEMA} workspaceId={workspaceId} />,
        path: routePattern('workspaceSchema'),
      },
    ],
    { initialEntries: [routePath('workspaceSchema', { workspaceId })] },
  )
  const screen = await render(<RouterProvider router={router} />)

  await screen.getByRole('button', { name: /pickl_v4/ }).click()
  await expect.element(screen.getByRole('button', { name: /public/ })).toBeVisible()
  await expect.element(screen.getByRole('button', { name: /analytics/ })).toBeVisible()

  await screen.getByRole('button', { name: /public/ }).click()
  await expect.element(screen.getByRole('link', { name: 'products' })).toHaveAttribute(
    'href',
    routePath('workspaceSchemaCatalogTable', {
      catalogName: 'pickl_v4',
      schemaName: 'public',
      tableName: 'products',
      workspaceId,
    }),
  )
})
