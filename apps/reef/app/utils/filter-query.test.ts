import { expect, test } from 'vitest'

import { filterQuery } from './filter-query'

const names = ['GitHub', 'GitHub Enterprise', 'Weather', 'api.v2 (beta)', 'a*b', 'a\\b']

// Catches: `^`, `$` or `*` read as plain text, or read anywhere but where
// they mean something; `\*` or `\\` not read as one literal character, or a
// lone `\` not read as itself; other regex characters read at all, which
// would also throw on a half-typed pattern; or case deciding a match.
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
  ['^a*b$', ['a*b', 'a\\b']],
  ['^a\\*b$', ['a*b']],
  ['^a\\\\b$', ['a\\b']],
  ['^a\\b$', ['a\\b']],
])('%j matches %j', (input, expected) => {
  const query = filterQuery(input)
  expect(names.filter((name) => query.matches(name))).toEqual(expected)
})

// Catches: `*` that stops at a line break.
test('a wildcard spans lines', () => {
  expect(filterQuery('first*second').matches('first\nsecond')).toBe(true)
})

// Catches: a query that backtracks, which grows with each `*` and froze the
// page for 15 seconds at seven of them on a 60-character name.
test('a query with many wildcards fails a long name at once', () => {
  const started = performance.now()
  expect(filterQuery('a*a*a*a*a*a*a*b').matches('a'.repeat(10_000))).toBe(false)
  expect(performance.now() - started).toBeLessThan(100)
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
  ['a\\*', 'a*b', [[0, 2]]],
  ['', 'issues.list', []],
  ['ß', 'Straße', [[4, 5]]],
])('%j marks %j at %j', (input, text, ranges) => {
  expect(filterQuery(input).ranges(text)).toEqual(ranges)
})
