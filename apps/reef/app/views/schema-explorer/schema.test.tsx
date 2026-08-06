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
      catalogName: 'github_v4',
      name: 'issues',
      items: [
        {
          columns: [],
          columnsLoaded: false,
          kind: 'table',
          name: 'list_for_repo',
          requiredFilters: [],
        },
        {
          arguments: [],
          kind: 'tableFunction',
          name: 'search',
          resultColumns: [],
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

  await screen.getByRole('button', { name: /github_v4\.issues/ }).click()
  await expect.element(screen.getByRole('link', { name: 'list_for_repo' })).toHaveAttribute(
    'href',
    routePath('workspaceSchemaCatalogTable', {
      catalogName: 'github_v4',
      schemaName: 'issues',
      tableName: 'list_for_repo',
      workspaceId,
    }),
  )
  await expect.element(screen.getByRole('link', { name: 'search()' })).toHaveAttribute(
    'href',
    routePath('workspaceSchemaCatalogTableFunction', {
      catalogName: 'github_v4',
      functionName: 'search',
      schemaName: 'issues',
      workspaceId,
    }),
  )
})
