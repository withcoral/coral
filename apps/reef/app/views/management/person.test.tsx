/** @vitest-environment happy-dom */
import { render } from '@testing-library/react'
import { expect, test } from 'vitest'
import { Person } from './person'
import type { PersonView } from './types'

const person = (fields: Partial<PersonView>): PersonView => ({
  user: 'users/u',
  name: '',
  email: '',
  local: false,
  ...fields,
})

// Catches: initials made from a placeholder, such as "LU" for the local user.
// Which letters a name gives is the avatar's rule, shown in its story.
test.each<[string, Partial<PersonView>, string, string | null]>([
  ['a name', { name: 'James Summerfield' }, 'James Summerfield', 'JS'],
  ['an email alone', { email: 'james@example.com' }, 'james@example.com', 'ja'],
  ['the local user', { name: 'James Summerfield', local: true }, 'Local user', null],
  ['no name or email', {}, 'Unnamed user', null],
])('a person shows initials only for %s', (_, fields, label, initials) => {
  for (const compact of [false, true]) {
    const { container, unmount } = render(
      <Person person={person(fields)} currentUser="users/other" compact={compact} />,
    )
    expect(container.textContent).toBe(`${initials ?? ''}${label}`)
    unmount()
  }
})
