/** @vitest-environment happy-dom */
import { render, screen } from '@testing-library/react'
import { afterEach, expect, it, vi } from 'vitest'
import { Instant } from './instant'

afterEach(() => vi.unstubAllEnvs())

it.each([
  ['2026-09-18T14:02:00.000Z', '18 Sep 2026, 14:02:00 UTC'],
  ['2026-09-08T00:00:09Z', '8 Sep 2026, 00:00:09 UTC'],
  // Sub-second digits stay in the attributes; the visible time keeps whole seconds.
  ['2026-09-22T09:00:05.123456789Z', '22 Sep 2026, 09:00:05 UTC'],
  // An offset instant reads in UTC, which can be another calendar day.
  ['2026-10-01T01:30:00+02:00', '30 Sep 2026, 23:30:00 UTC'],
])('reads %s as %s and keeps the exact instant to inspect', (value, text) => {
  render(<Instant value={value} />)
  const time = screen.getByText(text)
  expect(time.tagName).toBe('TIME')
  expect(time).toHaveAttribute('dateTime', value)
  expect(time).toHaveAttribute('title', value)
})

it.each(['Pacific/Kiritimati', 'America/Los_Angeles'])(
  'reads the same UTC time whatever the host time zone (%s)',
  (zone) => {
    vi.stubEnv('TZ', zone)
    render(<Instant value="2026-09-18T23:59:59Z" />)
    expect(screen.getByText('18 Sep 2026, 23:59:59 UTC')).toBeInTheDocument()
  },
)

it.each([
  ['not-a-time', 'not-a-time'],
  ['', 'Not recorded'],
])('shows %j as stored, without inventing a time', (value, text) => {
  const { container } = render(<Instant value={value} />)
  expect(screen.getByText(text)).toBeInTheDocument()
  expect(container.querySelector('time')).toBeNull()
})
