/** @vitest-environment happy-dom */
import { cleanup, render, screen, within } from '@testing-library/react'
import { afterEach, expect, it } from 'vitest'
import { ApprovalSummary } from './summary'

afterEach(cleanup)

// Catches: a wait just under a unit boundary read as "60 s" or "60 min".
it.each([
  [null, '—'],
  [38.4, '38 s'],
  [59.6, '1 min'],
  [252, '4 min'],
  [3599, '1 h'],
  [5400, '1.5 h'],
  [86_399, '1 d'],
  [129_600, '1.5 d'],
])('shows a median wait of %s seconds as %s', (medianWaitSeconds, shown) => {
  render(<ApprovalSummary approvals={{ approved: 1, declined: 0, medianWaitSeconds }} />)
  const wait = screen.getByRole('group', { name: 'Median wait' })
  expect(within(wait).getByText(shown)).toBeTruthy()
})
