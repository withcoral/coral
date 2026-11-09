/** @vitest-environment happy-dom */
import { render, screen } from '@testing-library/react'
import { expect, it } from 'vitest'
import { SavedAt } from './saved-at'

it.each([
  ['a UTC time', '2026-11-09T21:30:00Z', '2026-11-09 21:30 UTC'],
  ['an offset time, moved to UTC', '2026-11-09T23:30:00+02:00', '2026-11-09 21:30 UTC'],
  ['an offset that crosses midnight', '2026-11-10T01:15:00+02:00', '2026-11-09 23:15 UTC'],
])('shows %s', (_, at, shown) => {
  render(<SavedAt at={at} />)
  expect(screen.getByText(shown)).toHaveAttribute('datetime', at)
})
