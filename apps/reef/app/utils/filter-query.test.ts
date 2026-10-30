import { expect, test } from 'vitest'

import { filterQuery, payloadQuery } from './filter-query'

const names = ['GitHub', 'GitHub Enterprise', 'Weather', 'api.v2 (beta)', 'a*b', '^start$']

// Catches: a query character read as a pattern, which would also throw on a
// half-typed one; or case or surrounding space deciding a match.
test.each([
  ['', names],
  ['  ', names],
  ['github', ['GitHub', 'GitHub Enterprise']],
  [' hub ', ['GitHub', 'GitHub Enterprise']],
  ['.v2 (', ['api.v2 (beta)']],
  ['.', ['api.v2 (beta)']],
  ['github (', []],
  ['a*b', ['a*b']],
  ['git*prise', []],
  ['^git', []],
  ['^start$', ['^start$']],
])('%j matches %j', (input, expected) => {
  const query = filterQuery(input)
  expect(names.filter((name) => query.matches(name))).toEqual(expected)
})

// Catches: a query that tests only its first text, or fails on a missing one.
test('a query matches when any of its texts matches', () => {
  const query = filterQuery('ada@')
  expect(query.matches('Ada Lovelace', 'ada@example.com')).toBe(true)
  expect(query.matches(undefined, null, 'Ada Lovelace')).toBe(false)
})

// Catches: a missed repeat, a mark off by the case of its text, or a mark
// for no query.
test.each([
  [
    'e',
    'repos.get',
    [
      [1, 2],
      [7, 8],
    ],
  ],
  [
    'RE',
    'repos.re',
    [
      [0, 2],
      [6, 8],
    ],
  ],
  ['*', 'a*b', [[1, 2]]],
  ['', 'issues.list', []],
  ['ß', 'Straße', [[4, 5]]],
])('%j marks %j at %j', (input, text, ranges) => {
  expect(filterQuery(input).ranges(text)).toEqual(ranges)
})

// Catches: a Find that counts each piece of a `*` query as its own match.
test.each([
  [
    'row*11',
    'row-11 row-110',
    [
      [
        [0, 3],
        [4, 6],
      ],
      [
        [7, 10],
        [11, 13],
      ],
    ],
  ],
  ['ow', 'row row', [[[1, 3]], [[5, 7]]]],
  ['^r', 'row row', [[[0, 1]]]],
  // Wildcards alone, anchored or not, take each text whole, once.
  ['*', 'row', [[[0, 3]]]],
  ['**', 'row', [[[0, 3]]]],
  ['^*', 'row', [[[0, 3]]]],
  ['*$', 'row', [[[0, 3]]]],
  ['^*$', 'row', [[[0, 3]]]],
  ['*', '', []],
  // Anchors alone have no text to find.
  ['^', 'row', []],
  ['$', 'row', []],
  ['^$', '', []],
  ['', 'row', []],
])('%j finds each match of %j as one hit', (input, text, hits) => {
  const query = payloadQuery(input)
  expect(query.hits(text)).toEqual(hits)
  // Catches: a query that says it matches what Find can neither count nor show.
  if (input) expect(query.matches(text)).toBe(hits.length > 0)
})
