/** @vitest-environment happy-dom */

import { screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, test } from 'vitest'
import { renderRoute } from '@/test-support/route'
import { RestoreWorkspaceButton } from './restore-workspace'

const refused = 'Coral could not restore the workspace. You do not have permission for this action.'
const workspace = { path: '/workspaces/old', etag: 'etag', displayName: 'Old ops' }

function render() {
  renderRoute(
    <RestoreWorkspaceButton
      workspace={workspace}
      form={{ csrf: 'test-csrf', requestId: 'test-request' }}
    />,
    {
      entry: '/workspaces/old/activity',
      routes: [{ path: workspace.path, action: () => ({ error: refused, credential: null }) }],
    },
  )
}

// Catches: a refused restore passing unseen and unannounced.
test('a refused restore from the banner shows beside its button', async () => {
  render()
  await userEvent.click(screen.getByRole('button', { name: 'Restore workspace' }))
  expect(await screen.findByRole('alert')).toHaveTextContent(refused)
})
