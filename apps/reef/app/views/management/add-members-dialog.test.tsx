/** @vitest-environment happy-dom */
import { act, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, it } from 'vitest'
import { renderRoute } from '@/test-support/route'
import { AddMembersDialog } from './add-members-dialog'
import type { PersonView } from './types'

const person = (id: string, name: string, email: string): PersonView => ({
  user: `users/${id}`,
  name,
  email,
  local: false,
})

// Catches: a chip renamed after a reload to whoever else now shares its email.
// Emails are not unique, so the chip must keep the person you picked.
it('keeps naming the picked person when someone else later shares their email', async () => {
  const ada = person('ada', 'Ada', 'ada@example.com')
  let candidates = [ada]
  const { router } = renderRoute(() => (
    <AddMembersDialog
      candidates={candidates}
      currentUser="users/me"
      disabled={false}
      form={{ csrf: 'test-csrf', requestId: 'test-request' }}
    />
  ))
  const user = userEvent.setup()
  await user.click(screen.getByRole('button', { name: 'Add members' }))
  const dialog = await screen.findByRole('dialog', { name: 'Add members' })
  await user.type(within(dialog).getByRole('combobox', { name: 'People' }), 'ada@{Enter}')
  expect(within(dialog).getByRole('button', { name: 'Remove Ada' })).toBeTruthy()

  candidates = [ada, person('eve', 'Eve', 'ada@example.com')]
  await act(() => router.navigate('/?reloaded'))
  expect(within(dialog).getByRole('button', { name: 'Remove Ada' })).toBeTruthy()
  expect(within(dialog).queryByRole('button', { name: 'Remove Eve' })).toBeNull()
})

// Catches: a picked chip whose person leaves the list submitting its label,
// "Ada (ada@example.com)", in place of the email.
it('submits the email of a picked person who has left the list', async () => {
  const ada = person('ada', 'Ada', 'ada@example.com')
  const ben = person('ben', 'Ben', 'ben@example.com')
  let candidates = [ada, ben]
  const { router } = renderRoute(() => (
    <AddMembersDialog
      candidates={candidates}
      currentUser="users/me"
      disabled={false}
      form={{ csrf: 'test-csrf', requestId: 'test-request' }}
    />
  ))
  const user = userEvent.setup()
  await user.click(screen.getByRole('button', { name: 'Add members' }))
  const dialog = await screen.findByRole('dialog', { name: 'Add members' })
  const people = within(dialog).getByRole('combobox', { name: 'People' })
  await user.type(people, 'ada@{Enter}')

  candidates = [ben]
  await act(() => router.navigate('/?reloaded'))
  await user.type(people, 'ben@{Enter}')
  const emails = [...dialog.querySelectorAll<HTMLInputElement>('input[name=email]')]
  expect(emails.map((e) => e.value)).toEqual(['ada@example.com', 'ben@example.com'])
})
