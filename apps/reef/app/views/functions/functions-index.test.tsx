import { createMemoryRouter, RouterProvider } from 'react-router'
import { describe, expect, it } from 'vitest'
import { render } from 'vitest-browser-react'

import { FunctionsIndex } from './functions-index'

function renderFunctions(props: React.ComponentProps<typeof FunctionsIndex>) {
  const router = createMemoryRouter(
    [{ element: <FunctionsIndex {...props} />, path: '/functions' }],
    { initialEntries: ['/functions'] },
  )
  return render(<RouterProvider router={router} />)
}

describe('FunctionsIndex', () => {
  it('renders available function details and changes the selection', async () => {
    const screen = await renderFunctions({
      functions: [
        {
          arguments: [{ dataType: 'Utf8', name: 'owner' }],
          description: 'Pull requests waiting for review.',
          name: 'review_queue',
          namespace: 'engineering',
          resultColumns: [{ dataType: 'Int64', name: 'number', nullable: false }],
          sources: [],
        },
        {
          arguments: [],
          description: 'Failed deployments requiring investigation.',
          name: 'deployment_failures',
          namespace: 'operations',
          resultColumns: [{ dataType: 'Utf8', name: 'id', nullable: false }],
          sources: [],
        },
        {
          arguments: [],
          description: 'Recently opened incidents.',
          name: 'recent_incidents',
          namespace: 'operations',
          resultColumns: [{ dataType: 'Utf8', name: 'id', nullable: false }],
          sources: [],
        },
      ],
      loadError: null,
    })

    await expect.element(screen.getByText('Functions', { exact: true })).toBeVisible()
    await expect
      .element(screen.getByRole('button', { name: /engineering/ }))
      .toHaveAttribute('aria-expanded', 'false')
    await expect
      .element(screen.getByRole('button', { name: /operations/ }))
      .toHaveAttribute('aria-expanded', 'false')
    await expect
      .element(screen.getByRole('button', { name: 'deployment_failures' }))
      .not.toBeInTheDocument()
    await expect.element(screen.getByRole('heading', { name: 'review_queue' })).toBeVisible()
    await expect.element(screen.getByText('Pull requests waiting for review.')).toBeVisible()
    await expect.element(screen.getByText('owner')).toBeVisible()
    await expect.element(screen.getByText('number')).toBeVisible()

    await screen.getByRole('button', { name: /operations/ }).click()
    await expect
      .element(screen.getByRole('button', { name: /operations/ }))
      .toHaveAttribute('aria-expanded', 'true')
    await expect.element(screen.getByRole('button', { name: 'deployment_failures' })).toBeVisible()

    await screen.getByRole('button', { name: 'recent_incidents' }).click()

    await expect.element(screen.getByText('Recently opened incidents.')).toBeVisible()
    await expect.element(screen.getByRole('heading', { name: 'recent_incidents' })).toBeVisible()

    await screen.getByRole('button', { name: /operations/ }).click()
    await expect
      .element(screen.getByRole('button', { name: 'recent_incidents' }))
      .not.toBeInTheDocument()
  })

  it('renders empty and error states', async () => {
    const empty = await renderFunctions({ functions: [], loadError: null })
    await expect.element(empty.getByText('No functions available.')).toBeVisible()
    empty.unmount()

    const failed = await renderFunctions({ functions: [], loadError: 'Sidecar unavailable' })
    await expect.element(failed.getByText("Couldn't load functions")).toBeVisible()
    await expect.element(failed.getByText('Sidecar unavailable')).toBeVisible()
  })
})
