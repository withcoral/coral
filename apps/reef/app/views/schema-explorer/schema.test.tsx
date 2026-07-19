import { createMemoryRouter, RouterProvider } from 'react-router'
import { expect, it } from 'vitest'
import { render } from 'vitest-browser-react'

import type { SchemaResponse } from '@/lib/schema-explorer'
import { routePath, routePattern } from '@/routing/routemap'

import { SchemaExplorer } from './schema'

const SCHEMA = {
  connectors: [
    {
      name: 'github?api',
      tables: [
        {
          columns: [],
          columnsLoaded: false,
          name: 'issues/closed',
          requiredFilters: [],
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

  await screen.getByRole('button', { name: /github\?api/ }).click()
  await expect.element(screen.getByRole('link', { name: 'issues/closed' })).toHaveAttribute(
    'href',
    routePath('workspaceSchemaTable', {
      schemaName: 'github?api',
      tableName: 'issues/closed',
      workspaceId,
    }),
  )
})
