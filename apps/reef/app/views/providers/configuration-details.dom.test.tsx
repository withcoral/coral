/** @vitest-environment happy-dom */

import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, test } from 'vitest'

import { OperationDetails } from './configuration-details'

// Catches: the operation search leaving out the reasons an operation cannot run.
test.each([
  ['issues', ['issues.list', 'issues.create: Needs a server.']],
  ['list', ['issues.list', 'repos.list']],
  ['needs a server', ['issues.create: Needs a server.']],
])('the operation search %j lists %j', async (query, shown) => {
  const user = userEvent.setup()
  render(
    <OperationDetails
      operations={[
        { key: 'issues.list' },
        { key: 'issues.create', reasons: ['Needs a server.'] },
        { key: 'repos.list' },
      ]}
    />,
  )
  await user.click(screen.getByText('View operations'))
  await user.type(screen.getByRole('searchbox', { name: 'Find an operation' }), query)
  expect(screen.getAllByRole('listitem').map((item) => item.textContent)).toEqual(shown)
})
