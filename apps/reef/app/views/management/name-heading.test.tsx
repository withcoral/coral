/** @vitest-environment happy-dom */

import { act, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, test, vi } from 'vitest'
import { renderRoute } from '@/test-support/route'
import { NameHeading } from './name-heading'

const user = userEvent.setup()

/** A workspace name heading that posts to the workspace route. `etag` is what the loader holds now. */
function render({ pageTitle = false }: { pageTitle?: boolean } = {}) {
  const submissions: Array<{ form: FormData; respond: (result: object) => void }> = []
  let etag = 'etag-1'
  const { router } = renderRoute(
    () => (
      <NameHeading
        name="Tools"
        noun="workspace"
        canRename
        form={{ csrf: 'test-csrf', requestId: 'test-request' }}
        intent="rename"
        etag={etag}
        action="/workspaces/ops"
        pageTitle={pageTitle}
      />
    ),
    {
      // The page sits apart from the route whose action saves the name.
      entry: '/workspaces/ops/activity',
      routes: [
        {
          path: '/workspaces/ops',
          action: async ({ request }) => {
            const form = await request.formData()
            return new Promise((respond) => submissions.push({ form, respond }))
          },
        },
      ],
    },
  )
  return {
    submissions,
    /** Another change lands and the page revalidates with its newer etag. */
    revalidate: async (next: string) => {
      etag = next
      await act(async () => router.navigate('/workspaces/ops/activity', { replace: true }))
    },
  }
}

const input = () => screen.queryByRole('textbox', { name: 'Workspace name' })
const title = () => screen.getByRole('heading', { level: 1, name: 'Tools' })

// Catches: a rename silently overwriting a change made while the editor was open.
test('a rename posts to the workspace route with the etag from when the editor opened', async () => {
  const { submissions, revalidate } = render()
  await user.click(screen.getByRole('button', { name: 'Edit workspace name' }))
  await revalidate('etag-2')
  await user.clear(input()!)
  await user.type(input()!, 'Ops{Enter}')
  await vi.waitFor(() => expect(submissions).toHaveLength(1))
  expect(Object.fromEntries(submissions[0]!.form)).toMatchObject({
    intent: 'rename',
    etag: 'etag-1',
    displayName: 'Ops',
  })
})

// Catches: Escape hiding a rename that is still saving, so it lands unseen.
test('Escape does not close the editor while the rename saves', async () => {
  const { submissions } = render()
  await user.click(screen.getByRole('button', { name: 'Edit workspace name' }))
  await user.type(input()!, ' team{Enter}')
  await vi.waitFor(() => expect(submissions).toHaveLength(1))
  await user.keyboard('{Escape}')
  expect(input()).toHaveAttribute('readonly')
  await act(async () => submissions[0]!.respond({ error: null, credential: null }))
  await vi.waitFor(() => expect(input()).toBeNull())
})

// Catches: a page titled by its name losing its h1 while the editor replaces it.
test('a page title keeps its h1 while the editor is open', async () => {
  render({ pageTitle: true })
  expect(title()).toBeInTheDocument()
  await user.click(screen.getByRole('button', { name: 'Edit workspace name' }))
  expect(input()).toBeInTheDocument()
  expect(title()).toBeInTheDocument()
})
