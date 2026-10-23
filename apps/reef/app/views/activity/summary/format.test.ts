import { expect, it } from 'vitest'
import { duration, share } from './format'

// Catches: a wait just under a unit boundary read as "60 s" or "60 min".
it.each([
  [38.4, '38 s'],
  [59.6, '1 min'],
  [252, '4 min'],
  [3599, '1 h'],
  [5400, '1.5 h'],
  [86_399, '1 d'],
  [129_600, '1.5 d'],
])('shows a wait of %s seconds as %s', (seconds, shown) => {
  expect(duration(seconds)).toBe(shown)
})

// Catches: a few stopped calls rounded away to 0%, or a partial share rounded up to 100%.
it.each([
  [0, 0, '0%'],
  [0, 10, '0%'],
  [1, 2000, '<0.1%'],
  [2, 1245, '0.2%'],
  [1240, 1245, '99.6%'],
  [1999, 2000, '>99.9%'],
  [10, 10, '100%'],
  [412, 458, '90%'],
])('shows %s of %s as %s', (part, whole, shown) => {
  expect(share(part, whole)).toBe(shown)
})
