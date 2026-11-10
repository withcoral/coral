import { expect, it } from 'vitest'
import type { SearchCoverage } from '../types'
import { coverageLine, worstTone } from './coverage'

const counts = { eligible: 3, searched: 3, failed: 0, returned: 0, hasMore: false }
const row = (kind: SearchCoverage['kind'], state: SearchCoverage['state'], change = {}) =>
  ({ kind, state, ...counts, ...change }) satisfies SearchCoverage

// Search Coverage in Reef's words. A row never names a retriever or a stored state. A count
// says when more matched than Coral returned; only missing results keep a sentence.
it.each([
  [row('provider_tool', 'results_found', { returned: 12 }), ['Tools', '12 found', 'success', null]],
  [
    row('private_tool', 'results_found', { returned: 1, hasMore: true }),
    ['Private tools', '1+ found', 'success', null],
  ],
  [
    row('shared_tool', 'results_found', { returned: 2 }),
    ['Shared tools', '2 found', 'success', null],
  ],
  [row('provider_tool', 'empty'), ['Tools', 'none match', 'neutral', null]],
  [
    row('private_tool', 'empty', { eligible: 0, searched: 0 }),
    ['Private tools', 'none match', 'neutral', null],
  ],
  [row('provider_tool', 'skipped'), ['Tools', 'skipped by the provider filter', 'neutral', null]],
  [
    row('private_tool', 'skipped'),
    ['Private tools', 'skipped by the provider filter', 'neutral', null],
  ],
  [
    row('provider_tool', 'partial', { searched: 2, failed: 1 }),
    ['Tools', '1 of 3 providers failed', 'warning', 'Results may be missing.'],
  ],
  [
    row('private_tool', 'partial', { searched: 1, failed: 2 }),
    ['Private tools', '2 of 3 private tools failed', 'warning', 'Results may be missing.'],
  ],
  [
    row('shared_tool', 'partial', { searched: 2, failed: 1 }),
    ['Shared tools', '1 of 3 shared tools failed', 'warning', 'Results may be missing.'],
  ],
  [
    row('provider_tool', 'partial', { searched: 2 }),
    ['Tools', 'some are not indexed yet', 'warning', 'Results may be missing.'],
  ],
  [
    row('provider_tool', 'error', { searched: 0, failed: 3 }),
    ['Tools', 'could not search', 'error', 'Results are missing.'],
  ],
  [
    row('private_tool', 'error'),
    ['Private tools', 'could not search', 'error', 'Results are missing.'],
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
  expect(worstTone(states.map((state) => row('provider_tool', state)))).toBe(tone)
})
