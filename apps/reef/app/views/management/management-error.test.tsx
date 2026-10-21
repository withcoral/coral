/** @vitest-environment happy-dom */

import { screen } from '@testing-library/react'
import { expect, it } from 'vitest'

import { renderRoute } from '@/test-support/route'
import { ManagementErrorBoundary } from './management-error'

// Catches: a request Coral refused, such as a form Reef itself rejects, hiding
// the reason Reef gave behind a page that says Coral is unavailable.
it.each([
  [400, 'Invalid configuration', 'Invalid configuration'],
  [409, 'This sign-in is no longer pending.', 'This sign-in is no longer pending.'],
  [400, '', 'Coral cannot complete this request. Go back to the previous page.'],
  [503, 'Coral is not available.', 'Coral is unavailable. Reload the page to try again.'],
])('shows a %s refusal with the body %j as %j', async (status, body, message) => {
  renderRoute(null, {
    entry: '/refused',
    routes: [
      {
        path: '/refused',
        loader: () => {
          throw new Response(body, { status })
        },
        Component: () => null,
        ErrorBoundary: ManagementErrorBoundary,
      },
    ],
  })
  expect(await screen.findByRole('alert')).toHaveTextContent(message)
})
