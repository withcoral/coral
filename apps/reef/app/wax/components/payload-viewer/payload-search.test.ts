import { expect, it } from 'vitest'

import { filterQuery } from '@/utils/filter-query'

import { jsonStructure, type JsonNode } from './json-structure'
import { findMatches, lineUnits, structureUnits } from './payload-search'

const large = JSON.stringify({
  rows: Array.from({ length: 120 }, (_, index) => ({ id: index, name: `row-${index}` })),
  meta: { page: 1 },
  note: 'n'.repeat(400),
})

/** What Find finds in `value`: each match as the retained text it spans, and Raw's own count. */
function found(value: string, query: string, view: 'Structured' | 'Raw') {
  const search = filterQuery(query)
  if (view === 'Raw') {
    const { matches } = findMatches(lineUnits(value), search)
    return { matches: matches.map(({ ranges }) => spanned(value, ranges)), rawOnly: 0 }
  }
  const { units, entries } = structureUnits(jsonStructure(value) as JsonNode)
  const { matches, rawOnly } = findMatches(units, search, { source: value, entries })
  return { matches: matches.map(({ ranges }) => spanned(value, ranges)), rawOnly }
}
const spanned = (value: string, ranges: [number, number][]) =>
  value.slice(ranges[0]![0], ranges.at(-1)![1])
// row-11 and row-110 to row-119: each match spans their shared start.
const eleven = Array.from({ length: 11 }, () => 'row-11')

// Catches: a Find that reads `*`, `^` and `$` unlike every other search, counts one hit twice,
// searches only what is unfolded, or says nothing about what only Raw shows.
it.each([
  ['row-11', 'Structured', large, eleven, 0],
  // Across the whole line, `row*11` also spans row-0 to "id":11, and row-12 to "id":110.
  ['row*11', 'Structured', large, eleven, 2],
  ['^row-11', 'Structured', large, eleven, 0],
  ['row-11$', 'Structured', large, ['row-11'], 0],
  ['nnn$', 'Structured', large, ['nnn'], 0],
  ['^row', 'Raw', large, [], 0],
  ['row-11', 'Raw', large, eleven, 0],
  // Across a key and its value, the colon, or a quoted token: the retained text matches.
  ['"name":"x"', 'Structured', '{"name":"x","other":"name"}', ['"name":"x"'], 0],
  ['e":"', 'Structured', '{"name":"x","other":"name"}', ['e":"'], 0],
  ['"other"', 'Structured', '{"name":"x","other":"name"}', ['"other"'], 0],
  // What reaches another entry, or only punctuation, Raw shows.
  ['1,2', 'Structured', '[1,2]', [], 1],
  ['"a":{"b"', 'Structured', '{"a":{"b":1}}', [], 1],
  [',', 'Structured', '{"a":1,"b":2}', [], 1],
  [':', 'Structured', '{"a":1}', [], 1],
  ['1', 'Structured', '{"a":1,"b":[1,1]}', ['1', '1', '1'], 0],
  // Raw searches each line, whatever ends it.
  ['^"', 'Raw', '{"a":\r\n"x",\r"b":\n"x"}', ['"', '"', '"'], 0],
] as const)('finds %j in %s %#', (query, view, value, matches, rawOnly) => {
  expect(found(value, query, view)).toEqual({ matches, rawOnly })
})
