import { createMemoryRouter, RouterProvider, type ActionFunction } from 'react-router'
import { describe, expect, it, vi } from 'vitest'
import { render } from 'vitest-browser-react'

import { FunctionsIndex } from './functions-index'

const reviewQueue = {
  arguments: [{ dataType: 'Utf8', name: 'owner' }],
  description: 'Pull requests waiting for review.',
  name: 'review_queue',
  namespace: 'engineering',
  resultColumns: [{ dataType: 'Int64', name: 'number', nullable: false }],
  sources: [],
}

function renderFunctions(
  props: React.ComponentProps<typeof FunctionsIndex>,
  action?: ActionFunction,
) {
  const router = createMemoryRouter(
    [{ action, element: <FunctionsIndex {...props} />, path: '/functions' }],
    { initialEntries: ['/functions'] },
  )
  return render(<RouterProvider router={router} />)
}

describe('FunctionsIndex', () => {
  it('renders available function details and changes the selection', async () => {
    const screen = await renderFunctions({
      functions: [
        reviewQueue,
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

  it('keeps the confirmation open when deletion fails', async () => {
    const deleteAction = vi.fn(async () => ({
      message: 'function not found',
      name: 'review_queue',
      status: 'error',
    }))
    const screen = await renderFunctions(
      { functions: [reviewQueue], loadError: null },
      deleteAction,
    )

    await screen.getByRole('button', { name: 'Delete' }).click()
    const confirmation = screen.getByRole('dialog', { name: 'Delete review_queue?' })
    await confirmation.getByRole('button', { name: 'Delete function' }).click()

    await expect.element(confirmation.getByRole('alert')).toHaveTextContent('function not found')
    await expect.element(confirmation).toBeVisible()
    expect(deleteAction).toHaveBeenCalledOnce()
  })

  it('closes the confirmation after deletion succeeds', async () => {
    const deleteAction = vi.fn(async ({ request }: Parameters<ActionFunction>[0]) => {
      const formData = await request.formData()
      expect(formData.get('name')).toBe('review_queue')
      return { name: 'review_queue', status: 'success' }
    })
    const screen = await renderFunctions(
      { functions: [reviewQueue], loadError: null },
      deleteAction,
    )

    await screen.getByRole('button', { name: 'Delete' }).click()
    const confirmation = screen.getByRole('dialog', { name: 'Delete review_queue?' })
    await expect
      .element(confirmation.getByText('Queries that call it will stop working.', { exact: false }))
      .toBeVisible()
    await confirmation.getByRole('button', { name: 'Delete function' }).click()

    await expect.element(confirmation).not.toBeInTheDocument()
    expect(deleteAction).toHaveBeenCalledOnce()
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
