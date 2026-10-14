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

const renderDialog = (candidates: PersonView[]) => {
  renderRoute(() => (
    <AddMembersDialog
      candidates={candidates}
      currentUser="users/me"
      disabled={false}
      form={{ csrf: 'test-csrf', requestId: 'test-request' }}
    />
  ))
}
const openDialog = async (user: ReturnType<typeof userEvent.setup>) => {
  await user.click(screen.getByRole('button', { name: 'Add members' }))
  const dialog = await screen.findByRole('dialog', { name: 'Add members' })
  return { dialog, people: within(dialog).getByRole('combobox', { name: 'People' }) }
}
const submitted = (dialog: HTMLElement) =>
  [...dialog.querySelectorAll<HTMLInputElement>('input[name=email]')].map((e) => e.value)

// Catches: two people who share an email listed as one, or both chips named
// after the first pick. Both are listed; the email names one pick at a time.
it('lists people who share an email and keeps one of them picked', async () => {
  renderDialog([person('ada', 'Ada', 'ada@example.com'), person('eve', 'Eve', 'ada@example.com')])
  const user = userEvent.setup()
  const { dialog, people } = await openDialog(user)
  await user.type(people, 'Ada{Enter}')
  await user.type(people, 'example')
  const ada = screen.getByRole('option', { name: 'Ada (ada@example.com)' })
  const eve = screen.getByRole('option', { name: 'Eve (ada@example.com)' })
  expect(ada.getAttribute('aria-selected')).toBe('true')
  expect(eve.getAttribute('aria-selected')).toBe('false')

  await user.click(eve)
  expect(within(dialog).getByRole('button', { name: 'Remove Eve' })).toBeTruthy()
  expect(within(dialog).queryByRole('button', { name: 'Remove Ada' })).toBeNull()
  expect(submitted(dialog)).toEqual(['ada@example.com'])
})

// Catches: two people with the same name and email listed as one option,
// because the list told its options apart by their text.
it('lists two people with the same name and email', async () => {
  renderDialog([person('ada', 'Ada', 'ada@example.com'), person('ada2', 'Ada', 'ada@example.com')])
  const user = userEvent.setup()
  const { people } = await openDialog(user)
  await user.type(people, 'example')
  expect(screen.getAllByRole('option', { name: 'Ada (ada@example.com)' })).toHaveLength(2)
})

// Catches: two people with one name listed as one, or picked as two chips you
// cannot tell apart.
it('lists people who share a name and shows their emails on their chips', async () => {
  renderDialog([person('ada', 'Ada', 'ada@example.com'), person('ada2', 'Ada', 'ada@other.com')])
  const user = userEvent.setup()
  const { dialog, people } = await openDialog(user)
  await user.type(people, 'example.com{Enter}')
  expect(within(dialog).getByRole('button', { name: 'Remove Ada' })).toBeTruthy()
  await user.type(people, 'other.com{Enter}')
  expect(within(dialog).getByRole('button', { name: 'Remove Ada (ada@example.com)' })).toBeTruthy()
  expect(within(dialog).getByRole('button', { name: 'Remove Ada (ada@other.com)' })).toBeTruthy()
  expect(submitted(dialog)).toEqual(['ada@example.com', 'ada@other.com'])
})
