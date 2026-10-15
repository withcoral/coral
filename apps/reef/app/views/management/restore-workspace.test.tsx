/** @vitest-environment happy-dom */

import { screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeEach, expect, test, vi } from 'vitest'
import { renderRoute } from '@/test-support/route'
import { RestoreWorkspaceButton } from './restore-workspace'

vi.mock('@/wax/components/toast', () => ({ addToast: vi.fn() }))
const { addToast } = await import('@/wax/components/toast')

const refused = 'Coral could not restore the workspace. You do not have permission for this action.'
const workspace = { path: '/workspaces/old', etag: 'etag', displayName: 'Old ops' }

beforeEach(() => vi.mocked(addToast).mockClear())

function render(compact: boolean) {
  renderRoute(
    <RestoreWorkspaceButton
      workspace={workspace}
      compact={compact}
      form={{ csrf: 'test-csrf', requestId: 'test-request' }}
    />,
    {
      entry: '/workspaces/old/activity',
      routes: [{ path: workspace.path, action: () => ({ error: refused, credential: null }) }],
    },
  )
}

// Catches: a refused restore from the sidebar icon passing unseen and unannounced.
test('a refused restore from the sidebar icon shows a toast each time', async () => {
  render(true)
  const restore = screen.getByRole('button', { name: 'Restore Old ops' })
  await userEvent.click(restore)
  await vi.waitFor(() =>
    expect(addToast).toHaveBeenCalledExactlyOnceWith('error', { title: refused }),
  )
  await vi.waitFor(() => expect(restore).toBeEnabled())
  await userEvent.click(restore)
  await vi.waitFor(() => expect(addToast).toHaveBeenCalledTimes(2))
})

// Catches: the banner's failure leaving its button, or doubling up as a toast.
test('a refused restore from the banner shows beside its button', async () => {
  render(false)
  await userEvent.click(screen.getByRole('button', { name: 'Restore workspace' }))
  expect(await screen.findByRole('alert')).toHaveTextContent(refused)
  expect(addToast).not.toHaveBeenCalled()
})
