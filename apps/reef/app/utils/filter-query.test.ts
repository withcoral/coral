import { expect, test } from 'vitest'

import { filterQuery } from './filter-query'

const names = ['GitHub', 'GitHub Enterprise', 'Weather', 'api.v2 (beta)']

// Catches: `^`, `$` or `*` read as plain text, or read anywhere but where
// they mean something; other regex characters read at all, which would also
// throw on a half-typed pattern; or case deciding a match.
test.each([
  ['', names],
  ['  ', names],
  ['github', ['GitHub', 'GitHub Enterprise']],
  ['^github$', ['GitHub']],
  ['^git', ['GitHub', 'GitHub Enterprise']],
  ['^hub', []],
  ['prise$', ['GitHub Enterprise']],
  ['git*prise', ['GitHub Enterprise']],
  ['^w*r$', ['Weather']],
  ['git^hub', []],
  ['hub$ ', ['GitHub']],
  ['.v2 (', ['api.v2 (beta)']],
  ['.', ['api.v2 (beta)']],
  ['github (', []],
])('%j matches %j', (input, expected) => {
  const query = filterQuery(input)
  expect(names.filter((name) => query.matches(name))).toEqual(expected)
})

// Catches: a query that tests only its first text, or fails on a missing one.
test('a query matches when any of its texts matches', () => {
  const query = filterQuery('^ada@')
  expect(query.matches('Ada Lovelace', 'ada@example.com')).toBe(true)
  expect(query.matches(undefined, null, 'Ada Lovelace')).toBe(false)
})

// Catches: marks on what `*` matched, a missed repeat, or an empty mark.
test.each([
  [
    'e',
    'repos.get',
    [
      [1, 2],
      [7, 8],
    ],
  ],
  ['^re', 'repos.re', [[0, 2]]],
  [
    '^iss*list$',
    'issues.list',
    [
      [0, 3],
      [7, 11],
    ],
  ],
  ['*', 'issues.list', []],
  ['', 'issues.list', []],
  ['ß', 'Straße', [[4, 5]]],
])('%j marks %j at %j', (input, text, ranges) => {
  expect(filterQuery(input).ranges(text)).toEqual(ranges)
})
