import { createMemoryRouter, Outlet, RouterProvider } from 'react-router'
import { expect, it } from 'vitest'
import { render } from 'vitest-browser-react'

import type { SchemaResponse } from '@/lib/schema-explorer'
import { routePath, routePattern } from '@/routing/routemap'

import { SchemaTableFunctionView } from './schema-table-function'

const SCHEMA = {
  connectors: [
    {
      items: [
        {
          arguments: [
            { name: 'channel', required: true, values: ['general', 'random'] },
            { name: 'cursor', required: false, values: [] },
          ],
          description: 'Lists messages in a channel.',
          kind: 'tableFunction',
          name: 'messages',
          resultColumns: [
            {
              description: 'Message text.',
              name: 'text',
              nullable: false,
              type: 'Utf8',
            },
          ],
        },
      ],
      name: 'slack',
    },
  ],
} satisfies SchemaResponse

it('shows table-function arguments and result columns from the parent schema response', async () => {
  const router = createMemoryRouter(
    [
      {
        children: [
          {
            element: <SchemaTableFunctionView />,
            path: ':schemaName/functions/:functionName',
          },
        ],
        element: <Outlet context={SCHEMA} />,
        path: routePattern('workspaceSchema'),
      },
    ],
    {
      initialEntries: [
        routePath('workspaceSchemaTableFunction', {
          functionName: 'messages',
          schemaName: 'slack',
          workspaceId: 'analytics',
        }),
      ],
    },
  )
  const screen = await render(<RouterProvider router={router} />)

  await expect
    .element(screen.getByRole('heading', { name: 'slack.messages(channel, ...)' }))
    .toBeVisible()
  await expect.element(screen.getByText('Lists messages in a channel.')).toBeVisible()
  await expect.element(screen.getByLabelText('Required argument')).toHaveTextContent('*')
  await expect.element(screen.getByRole('cell', { name: 'general, random' })).toBeVisible()
  await expect.element(screen.getByRole('cell', { name: 'Message text.' })).toBeVisible()
})
