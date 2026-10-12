import { expect, it } from 'vitest'
import type { SearchCoverage } from '../types'
import { coverageLine, worstTone } from './coverage'

const counts = { eligible: 3, searched: 3, failed: 0, returned: 0, hasMore: false }
const row = (kind: SearchCoverage['kind'], state: SearchCoverage['state'], change = {}) =>
  ({ kind, state, ...counts, ...change }) satisfies SearchCoverage

// Search Coverage in Reef's words. A row never names a retriever or a stored state.
it.each([
  [
    row('operation', 'results_found', { returned: 12 }),
    ['Operations', '12 found', 'success', '3 of 3 providers searched'],
  ],
  [
    row('saved_function', 'results_found', { returned: 1, hasMore: true }),
    ['Saved functions', '1 found', 'success', '3 of 3 functions searched, more available'],
  ],
  [row('operation', 'empty'), ['Operations', 'none match', 'neutral', '3 of 3 providers searched']],
  [
    row('saved_function', 'empty', { eligible: 0, searched: 0 }),
    ['Saved functions', 'none match', 'neutral', '0 of 0 functions searched'],
  ],
  [row('operation', 'skipped'), ['Operations', 'skipped by the provider filter', 'neutral', null]],
  [
    row('saved_function', 'skipped'),
    ['Saved functions', 'skipped by the provider filter', 'neutral', null],
  ],
  [
    row('operation', 'partial', { searched: 2, failed: 1 }),
    ['Operations', '1 of 3 providers failed', 'warning', 'Results may be missing.'],
  ],
  [
    row('saved_function', 'partial', { searched: 1, failed: 2 }),
    ['Saved functions', '2 of 3 functions failed', 'warning', 'Results may be missing.'],
  ],
  [
    row('operation', 'partial', { searched: 2 }),
    ['Operations', 'some are not indexed yet', 'warning', 'Results may be missing.'],
  ],
  [
    row('operation', 'error', { searched: 0, failed: 3 }),
    ['Operations', 'could not search', 'error', 'Results are missing.'],
  ],
  [
    row('saved_function', 'error'),
    ['Saved functions', 'could not search', 'error', 'Results are missing.'],
  ],
] as const)('states %o as %j', (coverage, [subject, label, tone, detail]) => {
  expect(coverageLine(coverage)).toEqual({ subject, label, tone, detail })
})

it.each([
  [['results_found', 'empty'], 'neutral'],
  [['results_found', 'partial'], 'warning'],
  [['partial', 'error'], 'error'],
  [['skipped', 'error'], 'error'],
] as const)('tints a page with the states %j as %s', (states, tone) => {
  expect(worstTone(states.map((state) => row('operation', state)))).toBe(tone)
})
