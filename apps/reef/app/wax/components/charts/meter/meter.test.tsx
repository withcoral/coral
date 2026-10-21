/** @vitest-environment happy-dom */
import { cleanup, render } from '@testing-library/react'
import { afterEach, expect, it } from 'vitest'
import { Meter } from './meter'

afterEach(cleanup)

// Catches: a bar that overflows its track, or a fill drawn for nothing.
it.each([
  ['a share of the maximum', 1, 4, '25%'],
  ['the whole maximum', 4, 4, '100%'],
  ['more than the maximum, capped', 6, 4, '100%'],
  ['nothing for zero', 0, 4, null],
  ['nothing for a zero maximum', 3, 0, null],
])('fills %s', (_, value, max, width) => {
  const { container } = render(<Meter value={value} max={max} />)
  const fill = container.firstElementChild!.firstElementChild as HTMLElement | null
  expect(fill?.style.width ?? null).toBe(width)
})
