/** @vitest-environment happy-dom */
import { act, renderHook } from '@testing-library/react'
import { expect, it, onTestFinished, vi } from 'vitest'
import { byCount, byText, useSort } from './sort'

interface Row {
  name: string
  calls: number
  team: string
}
const row = (name: string, calls: number, team: string): Row => ({ name, calls, team })
const ROWS = [
  row('Zendesk', 3, 'b'),
  row('github', 9, 'a'),
  row('Attio', 3, 'a'),
  row('Linear', 1, 'b'),
]

const columns = {
  name: byText((r: Row) => r.name),
  calls: byCount((r: Row) => r.calls),
  team: byText((r: Row) => r.team),
}
type Column = keyof typeof columns

function renderSort(initial: Column) {
  const { result } = renderHook(() => useSort({ columns, initial, tiebreak: columns.name.compare }))
  return {
    names: () => result.current.rows(ROWS).map((r) => r.name),
    orders: () => (['name', 'calls', 'team'] as const).map((column) => result.current.of(column)),
    by: (column: Column) => act(() => result.current.by(column)),
  }
}

// Catches: text that splits by case, or counts that start at the smallest.
it.each([
  ['name', ['Attio', 'github', 'Linear', 'Zendesk'], ['ascending', 'none', 'none']],
  ['calls', ['github', 'Attio', 'Zendesk', 'Linear'], ['none', 'descending', 'none']],
] as const)('starts a %s column its own way', (initial, names, orders) => {
  const sort = renderSort(initial)
  expect(sort.names()).toEqual(names)
  expect(sort.orders()).toEqual(orders)
})

// Catches: a heading that sorts one way only, or a column you switch to that keeps the
// direction of the one before.
it('turns the column around, and starts another column its own way', () => {
  const sort = renderSort('name')

  sort.by('name')
  expect(sort.names()).toEqual(['Zendesk', 'Linear', 'github', 'Attio'])
  expect(sort.orders()).toEqual(['descending', 'none', 'none'])

  sort.by('calls')
  expect(sort.names()).toEqual(['github', 'Attio', 'Zendesk', 'Linear'])
  expect(sort.orders()).toEqual(['none', 'descending', 'none'])

  sort.by('name')
  expect(sort.names()).toEqual(['Attio', 'github', 'Linear', 'Zendesk'])
  expect(sort.orders()).toEqual(['ascending', 'none', 'none'])
})

// Catches: ties that turn around with the column, so equal rows swap on every click.
it('keeps equal rows in tiebreak order whichever way the column runs', () => {
  const sort = renderSort('team')
  expect(sort.names()).toEqual(['Attio', 'github', 'Linear', 'Zendesk'])

  sort.by('team')
  expect(sort.names()).toEqual(['Linear', 'Zendesk', 'Attio', 'github'])
})

/** Runs the test as a runtime whose default locale is `locale`, such as a browser set to it. */
function runtimeLocale(locale: string) {
  const compare = String.prototype.localeCompare
  const spy = vi.spyOn(String.prototype, 'localeCompare').mockImplementation(function (
    this: string,
    that,
    locales,
    options,
  ) {
    return compare.call(this, that, locales ?? locale, options)
  })
  onTestFinished(() => spy.mockRestore())
  return spy
}

// Catches: an order that follows the runtime's locale, so the server and a browser in
// another language disagree when the page hydrates. Swedish puts Ö after Z. English gives
// the same order either way, so in that runtime only the check on the locales catches it.
it.each(['en', 'sv'])('sorts text in one order in a runtime set to %s', (locale) => {
  const compared = runtimeLocale(locale)
  const { result } = renderHook(() =>
    useSort({ columns: { name: byText((name: string) => name) }, initial: 'name' }),
  )
  expect(result.current.rows(['Zendesk', 'Öresund', 'Okta'])).toEqual([
    'Okta',
    'Öresund',
    'Zendesk',
  ])
  expect(compared.mock.calls.filter(([, locales]) => locales === undefined)).toEqual([])
})
