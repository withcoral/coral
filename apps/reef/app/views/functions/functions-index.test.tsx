import { createMemoryRouter, RouterProvider } from 'react-router'
import { describe, expect, it } from 'vitest'
import { render } from 'vitest-browser-react'

import { FunctionsIndex } from './functions-index'

describe('FunctionsIndex', () => {
  it('renders Desktop-style status and workspace-scoped actions', async () => {
    const router = createMemoryRouter(
      [
        {
          element: (
            <FunctionsIndex
              actionData={undefined}
              editor={null}
              functions={[
                {
                  arguments: [{ dataType: 'Utf8', name: 'owner' }],
                  description: 'Retrieve pull requests',
                  error: null,
                  name: 'retrieve_pull_requests',
                  schema: 'github',
                  status: 'ready',
                },
              ]}
              loadError={null}
              workspaceId="team alpha"
            />
          ),
          path: '/',
        },
      ],
      { initialEntries: ['/'] },
    )
    const screen = await render(<RouterProvider router={router} />)

    await expect
      .element(screen.getByText('github.retrieve_pull_requests(owner: Utf8)'))
      .toBeVisible()
    await expect.element(screen.getByText('Ready')).toBeVisible()
    await expect
      .element(screen.getByRole('link', { name: 'Edit retrieve_pull_requests' }))
      .toHaveAttribute('href', '/workspaces/team%20alpha/functions?edit=retrieve_pull_requests')
  })

  it('highlights SQL while keeping the textarea editable', async () => {
    const router = createMemoryRouter([
      {
        element: (
          <FunctionsIndex
            actionData={undefined}
            editor={{
              artifact: { description: '', name: '', schema: '', sql: 'select 1' },
              loadError: null,
              mode: 'new',
            }}
            functions={[]}
            loadError={null}
            workspaceId="default"
          />
        ),
        path: '/',
      },
    ])
    const screen = await render(<RouterProvider router={router} />)

    const sql = screen.getByRole('textbox', { name: 'SQL' })
    await expect.element(sql).toHaveValue('select 1')
    expect(document.querySelector('.sql-keyword')?.textContent).toBe('select')
    expect(document.querySelector('.sql-number')?.textContent).toBe('1')

    await sql.fill('select count(*) from github.issues where number = 42')
    await expect.element(sql).toHaveValue('select count(*) from github.issues where number = 42')
    expect(document.querySelector('.sql-function')?.textContent).toBe('count')
    expect(document.querySelector('.sql-number')?.textContent).toBe('42')
  })
})
