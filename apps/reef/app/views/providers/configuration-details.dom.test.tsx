/** @vitest-environment happy-dom */

import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, test } from 'vitest'

import { ToolDetails } from './configuration-details'

// Catches: the tool search leaving out the reasons a tool cannot run.
test.each([
  ['issues', ['issues.list', 'issues.create: Needs a server.']],
  ['list', ['issues.list', 'repos.list']],
  ['needs a server', ['issues.create: Needs a server.']],
])('the tool search %j lists %j', async (query, shown) => {
  const user = userEvent.setup()
  render(
    <ToolDetails
      tools={[
        { key: 'issues.list' },
        { key: 'issues.create', reasons: ['Needs a server.'] },
        { key: 'repos.list' },
      ]}
    />,
  )
  await user.click(screen.getByText('View tools'))
  await user.type(screen.getByRole('searchbox', { name: 'Find a tool' }), query)
  expect(screen.getAllByRole('listitem').map((item) => item.textContent)).toEqual(shown)
})
