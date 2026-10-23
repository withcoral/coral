/** @vitest-environment happy-dom */
import { cleanup, fireEvent, render, screen } from '@testing-library/react'
import { afterEach, expect, it, vi } from 'vitest'
import { Heading } from './heading'

afterEach(cleanup)

// Catches: a sortable heading that a screen reader cannot tell is sorted, or that does not sort.
it.each([
  [true, 'descending'],
  [false, 'none'],
])('marks a sortable heading active=%s as aria-sort %s and sorts on click', (active, ariaSort) => {
  const onSort = vi.fn()
  render(<Heading sort={{ active, onSort }}>Calls</Heading>)
  expect(screen.getByRole('columnheader').getAttribute('aria-sort')).toBe(ariaSort)
  fireEvent.click(screen.getByRole('button', { name: 'Calls' }))
  expect(onSort).toHaveBeenCalledOnce()
})

// Catches: a plain heading that claims a sort order it does not have.
it('leaves a plain heading without aria-sort or a control', () => {
  render(<Heading>Share</Heading>)
  expect(screen.getByRole('columnheader').hasAttribute('aria-sort')).toBe(false)
  expect(screen.queryByRole('button')).toBeNull()
})
