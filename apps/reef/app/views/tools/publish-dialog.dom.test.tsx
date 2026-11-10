/** @vitest-environment happy-dom */

import { screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { data, redirect } from 'react-router'
import { expect, test, vi } from 'vitest'
import { renderRoute } from '@/test-support/route'

import { PublishDialog, type PublishDialogProps } from './publish-dialog'
import type { PublishReviewResult, PublishReviewView } from './types'

// The bytes keep their exact spelling: the line breaks, and the spaces that end a line.
const SOURCE = 'export default async function remind(name: string) {\n  return name  \n}\n'
const DECLARATION = 'export default function remind(name: string): Promise<string>;\n'

const review = (more: Partial<PublishReviewView> = {}): PublishReviewResult => ({
  refusal: null,
  review: {
    origin: { kind: 'private', path: 'people.remind', agent: null },
    path: 'people.remind',
    description: 'Remind someone',
    source: SOURCE,
    declaration: DECLARATION,
    diagnostics: [],
    diagnosticsTruncated: false,
    previous: null,
    calledTools: ['coral.providers.mailer.send', 'coral.shared.mail.send'],
    sharedTools: [{ path: 'mail.send', revision: 4 }],
    runtimeBuiltPaths: [],
    defaultPolicy: null,
    ...more,
  },
})
const published = (revision: number) => ({
  revision,
  description: 'Remind someone',
  source: 'export default async () => "published"\n',
  declaration: 'export default function (): Promise<string>;\n',
})

const user = userEvent.setup()

/** Opens the dialog over a publish route that answers with `reviews` in turn, then the last. */
function open({
  reviews,
  publish = async () => redirect('/workspaces/demo/tools/shared/people.remind'),
  props = {},
}: {
  reviews: PublishReviewResult[]
  publish?: (form: FormData) => Promise<unknown>
  props?: Partial<PublishDialogProps>
}) {
  const reviewed: URLSearchParams[] = []
  const posted: FormData[] = []
  const onOpenChange = vi.fn()
  renderRoute(
    <PublishDialog
      open
      onOpenChange={onOpenChange}
      workspacePath="/workspaces/demo"
      form={{ csrf: 'test-csrf', requestId: 'test-request' }}
      origin={{ kind: 'private', path: 'people.remind', agent: null }}
      path="people.remind"
      policy="enabled"
      {...props}
    />,
    {
      entry: '/workspaces/demo/tools/private/people.remind',
      routes: [
        {
          path: '/workspaces/demo/shared-tools/publish',
          // As the publish route: a review runs only when the dialog asks.
          shouldRevalidate: () => false,
          loader: ({ request }) => {
            reviewed.push(new URL(request.url).searchParams)
            return reviews[Math.min(reviewed.length, reviews.length) - 1]!
          },
          action: async ({ request }) => {
            const form = await request.formData()
            posted.push(form)
            return publish(form)
          },
        },
      ],
    },
  )
  return { reviewed, posted, onOpenChange }
}

const publishButton = (name: string | RegExp = /^Publish/) => screen.findByRole('button', { name })

// Catches: a publish of anything but the reviewed bytes, a policy other than
// the one the owner chose, a publish against a revision the owner did not
// see, or a dialog that stays open over the shared tool it published.
test('publishes the reviewed content with the chosen policy, then closes', async () => {
  const { reviewed, posted, onOpenChange } = open({
    reviews: [review({ previous: published(2), path: 'mail.remind' })],
    props: { path: 'mail.remind' },
  })
  // The review is of the private tool at the path the dialog opened on.
  await publishButton('Publish revision 3')
  expect(Object.fromEntries(reviewed[0]!)).toEqual({ path: 'mail.remind', tool: 'people.remind' })
  expect(screen.getByText('Remove tokens and IDs that only you should see.')).toBeVisible()
  // The shared tool it calls shows with the revision it has now.
  const called = within(screen.getByRole('list', { name: 'Tools it calls' })).getAllByRole(
    'listitem',
  )
  expect(called.map((item) => item.textContent)).toEqual([
    'coral.providers.mailer.send',
    'coral.shared.mail.sendRevision 4',
  ])

  // The private tool's own policy is the first choice; the owner makes another.
  const policy = screen.getByRole('radiogroup', { name: 'Default policy' })
  expect(within(policy).getByRole('radio', { name: 'Allow' })).toBeChecked()
  await user.click(within(policy).getByRole('radio', { name: 'Block' }))
  await user.click(await publishButton('Publish revision 3'))

  await waitFor(() => expect(onOpenChange).toHaveBeenCalledWith(false))
  expect(posted).toHaveLength(1)
  const form = posted[0]!
  expect(JSON.parse(form.get('reviewed') as string)).toEqual({
    description: 'Remind someone',
    source: SOURCE,
    declaration: DECLARATION,
  })
  expect({
    tool: form.get('tool'),
    agent: form.get('agent'),
    restore: form.get('restore'),
    path: form.get('path'),
    expectedRevision: form.get('expectedRevision'),
    access: form.get('access'),
    csrf: form.get('csrf'),
  }).toEqual({
    tool: 'people.remind',
    agent: null,
    restore: null,
    path: 'mail.remind',
    expectedRevision: '2',
    access: 'disabled',
    csrf: 'test-csrf',
  })
})

// Catches: a refused publish that closes the dialog or hides the reason, or
// a dialog that still offers the stale review, so the next publish would be
// refused again or would replace a revision the owner never saw.
test('after a refused publish, says why and reviews again', async () => {
  const refusal = 'Another owner published this shared tool first. Review the changes again.'
  const { reviewed, posted, onOpenChange } = open({
    reviews: [review(), review({ previous: published(1) })],
    publish: async () => data({ error: refusal, credential: null }, { status: 409 }),
  })
  await user.click(await publishButton('Publish revision 1'))

  expect(await screen.findByText(refusal)).toBeVisible()
  // The second review shows the other owner's revision as the one to replace.
  await publishButton('Publish revision 2')
  expect(screen.getByText(/replaces revision 1 of coral\.shared\.people\.remind/)).toBeVisible()
  expect(reviewed).toHaveLength(2)
  expect(posted[0]!.get('expectedRevision')).toBe('0')
  expect(onOpenChange).not.toHaveBeenCalled()

  // The next publish expects the revision the second review showed.
  await user.click(await publishButton('Publish revision 2'))
  await waitFor(() => expect(posted).toHaveLength(2))
  expect(posted[1]!.get('expectedRevision')).toBe('1')
})

// Catches: a publish at a path the owner typed but Coral never reviewed, or
// a refused path that leaves the owner no way to enter another.
test('reviews another path before it publishes there', async () => {
  const taken = 'Another shared tool uses this path, or a part of it. Enter another path.'
  const { reviewed, posted } = open({
    reviews: [
      review(),
      { review: null, path: 'mail', refusal: taken },
      review({ path: 'mail.remind', previous: published(2) }),
    ],
  })
  const path = await screen.findByRole('textbox', { name: 'Shared tool path' })
  expect(path).toHaveValue('people.remind')
  await publishButton('Publish revision 1')

  await user.clear(path)
  await user.type(path, 'mail')
  // The review on screen is of the first path, so it cannot publish at this one.
  expect(await publishButton()).toBeDisabled()
  await user.click(screen.getByRole('button', { name: 'Review path' }))
  expect(await screen.findByText(taken)).toBeVisible()
  expect(await publishButton()).toBeDisabled()

  await user.type(path, '.remind')
  await user.click(screen.getByRole('button', { name: 'Review path' }))
  await user.click(await publishButton('Publish revision 3'))
  await waitFor(() => expect(posted).toHaveLength(1))
  expect(reviewed.map((query) => query.get('path'))).toEqual([
    'people.remind',
    'mail',
    'mail.remind',
  ])
  expect(posted[0]!.get('path')).toBe('mail.remind')
})

// Catches: a restore that asks for a private tool's review, that lets the
// owner retarget it to another path, or that publishes as a copy of a
// private tool.
test('restores a revision at the path of its shared tool', async () => {
  const origin = { kind: 'restore', revision: 1 } as const
  const { reviewed, posted } = open({
    reviews: [
      review({
        origin,
        path: 'mail.remind',
        previous: published(2),
        defaultPolicy: 'approval_required',
      }),
    ],
    props: { origin, path: 'mail.remind' },
  })
  expect(await screen.findByRole('heading', { name: 'Restore revision 1' })).toBeVisible()
  expect(screen.queryByRole('textbox', { name: 'Shared tool path' })).toBeNull()
  // The revision's own policy comes before the fallback.
  expect(
    within(await screen.findByRole('radiogroup', { name: 'Default policy' })).getByRole('radio', {
      name: 'Ask',
    }),
  ).toBeChecked()
  await user.click(await publishButton('Publish revision 3'))
  await waitFor(() => expect(posted).toHaveLength(1))
  expect(Object.fromEntries(reviewed[0]!)).toEqual({ path: 'mail.remind', restore: '1' })
  expect({ restore: posted[0]!.get('restore'), tool: posted[0]!.get('tool') }).toEqual({
    restore: '1',
    tool: null,
  })
})
