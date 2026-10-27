/** @vitest-environment happy-dom */
import { render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, it, vi } from 'vitest'

import { LONG_STRING_LENGTH, PayloadViewer } from './payload-viewer'

const view = (value: string, format: 'json' | 'text' = 'json', label = 'Provider response') => {
  render(<PayloadViewer value={value} format={format} label={label} />)
  return screen.getByRole('region', { name: label })
}
const structure = (region: HTMLElement) => within(region).getByRole('group', { name: 'Structure' })
/** The expanded viewer, where every inspection control is. */
const inspect = async (
  value: string,
  format: 'json' | 'text' = 'json',
  label = 'Provider response',
) => {
  const region = view(value, format, label)
  await userEvent.setup().click(within(region).getByRole('button', { name: `Expand ${label}` }))
  return screen.findByRole('dialog', { name: label })
}

// Catches: an inline payload that repeats the expanded viewer's tools, or loses its actions.
it.each([
  ['JSON', '{"a":1}', 'json'],
  ['text', 'line', 'text'],
] as const)('keeps only Copy, Download and Expand in an inline %s payload', (_, value, format) => {
  const region = view(value, format)
  const actions = within(region).getByRole('group', { name: 'Payload actions' })
  expect(
    within(actions)
      .getAllByRole('button')
      .map((button) => button.getAttribute('aria-label')),
  ).toEqual(['Copy Provider response', 'Download Provider response', 'Expand Provider response'])
  for (const gone of ['searchbox', 'radiogroup', 'status'] as const)
    expect(within(region).queryByRole(gone)).toBeNull()
  expect(within(region).queryByRole('button', { name: 'Wrap' })).toBeNull()
  expect(region.textContent).not.toMatch(/JSON ·|Text ·|\d B\b/)
  // The payload scrolls inside the card, and you reach it with the keyboard.
  expect(within(region).getByRole('group', { name: 'Payload content' })).toHaveAttribute(
    'tabindex',
    '0',
  )
})

// Catches: a renderer that reparses and reserializes JSON, which rounds large integers,
// respells numbers, drops duplicate keys and rewrites escapes.
it('shows a small JSON value fully expanded, with every token as retained', () => {
  const value =
    '{"id":12345678901234567890,"ratio":1.0e3,"id":"dup","text":"a\\u0041\\n","list":[1,true,null],"empty":{}}'
  const region = view(value)
  const tree = structure(region)
  for (const token of [
    '12345678901234567890',
    '1.0e3',
    '"dup"',
    '"a\\u0041\\n"',
    'true',
    'null',
    '{}',
  ])
    expect(tree).toHaveTextContent(token)
  // Both `id` keys stay, in order.
  expect(tree.textContent!.match(/"id": /g)).toHaveLength(2)
  // Nothing starts folded, and each container states its size.
  for (const toggle of within(tree).getAllByRole('button', { expanded: true }))
    expect(toggle).toBeVisible()
  expect(within(tree).queryAllByRole('button', { expanded: false })).toHaveLength(0)
  expect(within(tree).getByRole('button', { name: 'list: 3 items' })).toBeInTheDocument()
  expect(within(tree).getByRole('button', { name: 'Value: 6 properties' })).toBeInTheDocument()
})

const large = JSON.stringify({
  rows: Array.from({ length: 120 }, (_, index) => ({ id: index, name: `row-${index}` })),
  meta: { page: 1 },
  note: 'n'.repeat(400),
})

// Catches: a large payload that opens as one endless document, or folding that drops content.
it('opens large JSON shallow, its long strings folded, and unfolds what you open', async () => {
  const user = userEvent.setup()
  const tree = structure(view(large))
  // The top level shows; the containers in it fold, each with its size.
  expect(within(tree).getByRole('button', { name: 'Value: 3 properties' })).toHaveAttribute(
    'aria-expanded',
    'true',
  )
  const rows = within(tree).getByRole('button', { name: 'rows: 120 items' })
  expect(rows).toHaveAttribute('aria-expanded', 'false')
  expect(within(tree).getByRole('button', { name: 'meta: 1 property' })).toHaveAttribute(
    'aria-expanded',
    'false',
  )
  expect(tree).not.toHaveTextContent('row-0')
  // A long string shows its start and folds the rest, which one press shows.
  // 402 characters with its quotes; the first 120 show.
  const note = within(tree).getByRole('button', { name: '282 more characters: note' })
  expect(note).toHaveAttribute('aria-expanded', 'false')
  expect(note).toHaveTextContent('282 more characters')
  expect(tree).not.toHaveTextContent(`"${'n'.repeat(400)}"`)
  await user.click(note)
  expect(tree).toHaveTextContent(`"${'n'.repeat(400)}"`)
  expect(note).toHaveAccessibleName('Fold: note')
  await user.click(rows)
  expect(rows).toHaveAttribute('aria-expanded', 'true')
  expect(within(tree).getAllByRole('button', { name: /^Item \d+: 2 properties$/ })).toHaveLength(
    120,
  )
})

it('keeps every long string open in a small payload, and lets each fold alone', async () => {
  const user = userEvent.setup()
  const long = 'x'.repeat(LONG_STRING_LENGTH + 1)
  const tree = structure(view(JSON.stringify({ a: long, b: long })))
  const [a, b] = [
    within(tree).getByRole('button', { name: 'Fold: a' }),
    within(tree).getByRole('button', { name: 'Fold: b' }),
  ]
  expect(a).toHaveAttribute('aria-expanded', 'true')
  await user.click(a)
  expect(a).toHaveAttribute('aria-expanded', 'false')
  expect(b).toHaveAttribute('aria-expanded', 'true')
})

// Catches: a Find that searches only what is unfolded, or finds a match it never shows.
it('finds matches in folded content and unfolds each one it moves to', async () => {
  const user = userEvent.setup()
  const region = await inspect(large)
  const tree = structure(region)
  const find = within(region).getByRole('searchbox', { name: 'Find in Provider response' })
  await user.type(find, 'row-11')
  // row-11 and row-110 to row-119.
  expect(within(region).getByRole('status')).toHaveTextContent('1 of 11')
  const current = () => tree.querySelector('[data-current-match]')
  expect(current()).toHaveTextContent('row-11')
  expect(within(tree).getByRole('button', { name: 'rows: 120 items' })).toHaveAttribute(
    'aria-expanded',
    'true',
  )
  await user.keyboard('{Enter}')
  expect(within(region).getByRole('status')).toHaveTextContent('2 of 11')
  expect(current()!.parentElement).toHaveTextContent('"row-110"')
  await user.keyboard('{Shift>}{Enter}{/Shift}')
  await user.keyboard('{Shift>}{Enter}{/Shift}')
  expect(within(region).getByRole('status')).toHaveTextContent('11 of 11')
  await user.click(within(region).getByRole('button', { name: 'Next match' }))
  expect(within(region).getByRole('status')).toHaveTextContent('1 of 11')
  // The folded end of a long string is searched too.
  await user.clear(find)
  await user.type(find, 'nnn$')
  expect(within(region).getByRole('status')).toHaveTextContent('1 of 1')
  expect(within(tree).getByRole('button', { name: 'Fold: note' })).toHaveAttribute(
    'aria-expanded',
    'true',
  )
  await user.clear(find)
  await user.type(find, 'absent')
  expect(within(region).getByRole('status')).toHaveTextContent('No matches')
  expect(within(region).getByRole('button', { name: 'Next match' })).toBeDisabled()
})

it.each([
  ['precision-sensitive numbers', '{"n":12345678901234567890.000,"e":-0E+2}'],
  ['duplicate keys', '{"a":1,"a":2}'],
  ['a scalar', '  "spaced"\n'],
])('shows %s exactly as retained in Raw', async (_, value) => {
  const user = userEvent.setup()
  const region = await inspect(value)
  await user.click(within(region).getByRole('radio', { name: 'Raw' }))
  expect(within(region).queryByRole('group', { name: 'Structure' })).toBeNull()
  expect(within(region).getByRole('group', { name: 'Raw' }).textContent).toBe(value)
})

// Catches: malformed JSON that is reformatted, hidden, or offered a structure it does not have.
it('shows malformed JSON as retained, and says it is not valid JSON', async () => {
  const value = '{"status": 200, "data": [1, 2'
  const region = await inspect(value)
  // Inline too, it shows as retained.
  // The modal hides the page behind it from assistive technology.
  const inline = screen.getByRole('region', { name: 'Provider response', hidden: true })
  expect(within(inline).getByRole('group', { name: 'Raw', hidden: true }).textContent).toBe(value)
  expect(within(region).getByRole('group', { name: 'Raw' }).textContent).toBe(value)
  expect(within(region).queryByRole('radiogroup', { name: 'View' })).toBeNull()
  expect(within(region).getByRole('group', { name: 'Payload tools' })).toHaveTextContent(
    'Not valid JSON',
  )
})

it('shows JSON null as a value, not as nothing', () => {
  const tree = structure(view('null'))
  expect(tree.textContent).toBe('null')
})

// Catches: a log shown as JSON, or a renderer that collapses its whitespace.
it('shows a text log as numbered lines, its whitespace kept, with a wrap toggle', async () => {
  const user = userEvent.setup()
  const value = 'first  line\n\tindented\n\nlast {"not":"json"}'
  const region = await inspect(value, 'text', 'Log message')
  // The modal hides the page behind it from assistive technology.
  const inline = screen.getByRole('region', { name: 'Log message', hidden: true })
  expect(within(inline).getByRole('group', { name: 'Lines', hidden: true }).textContent).toBe(
    `1first  line2\tindented34last {"not":"json"}`,
  )
  const lines = within(region).getByRole('group', { name: 'Lines' })
  expect(lines.textContent).toBe(`1first  line2\tindented34last {"not":"json"}`)
  expect(within(region).queryByRole('radiogroup', { name: 'View' })).toBeNull()
  expect(within(region).queryByRole('group', { name: 'Structure' })).toBeNull()
  const wrap = within(region).getByRole('button', { name: 'Wrap' })
  expect(wrap).toHaveAttribute('aria-pressed', 'true')
  await user.click(wrap)
  expect(wrap).toHaveAttribute('aria-pressed', 'false')
  await user.type(within(region).getByRole('searchbox', { name: 'Find in Log message' }), 'in')
  expect(within(region).getByRole('status')).toHaveTextContent('1 of 2')
})

// Catches: a copy or download of only what is shown, or of the formatted structure.
it('copies and downloads the complete payload as retained', async () => {
  const user = userEvent.setup()
  const writeText = vi.fn(() => Promise.resolve())
  Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
  const blobs: Blob[] = []
  const createObjectURL = vi.spyOn(URL, 'createObjectURL').mockImplementation((blob) => {
    blobs.push(blob as Blob)
    return 'blob:payload'
  })
  const names: string[] = []
  const click = vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(function (
    this: HTMLAnchorElement,
  ) {
    names.push(this.download)
  })
  try {
    const region = view(large)
    await user.click(within(region).getByRole('button', { name: 'Copy Provider response' }))
    expect(writeText).toHaveBeenCalledWith(large)
    await user.click(within(region).getByRole('button', { name: 'Download Provider response' }))
    expect(names).toEqual(['provider-response.json'])
    expect(await blobs[0]!.text()).toBe(large)
    expect(blobs[0]!.type).toBe('application/json')
  } finally {
    createObjectURL.mockRestore()
    click.mockRestore()
  }
})

// Catches: a size measured from the formatted display, or one that counts characters, not bytes.
it.each([
  ['{"a":1}', '7 B'],
  ['"é"', '4 B'],
  [JSON.stringify('x'.repeat(1498)), '1.5 kB'],
])('states the retained size of %s in the expanded viewer', async (value, size) => {
  const tools = within(await inspect(value)).getByRole('group', { name: 'Payload tools' })
  expect(tools).toHaveTextContent(`JSON · ${size}`)
})

it('says when only the start of a value was retained, and never otherwise', () => {
  render(<PayloadViewer value='{"cut' format="json" label="Upstream response data" truncated />)
  expect(
    within(screen.getByRole('region', { name: 'Upstream response data' })).getByText(
      'This is only the start of the value.',
    ),
  ).toBeInTheDocument()
  expect(within(view('{}', 'json', 'Whole')).queryByText(/start of the value/)).toBeNull()
})

// Catches: a Find that reads `*`, `^` and `$` unlike every other search, or counts one hit twice.
it.each([
  ['row*11', 'Structured', '1 of 11'],
  ['^row-11', 'Structured', '1 of 11'],
  ['row-11$', 'Structured', '1 of 1'],
  ['^row', 'Raw', 'No matches'],
  ['row-11', 'Raw', '1 of 11'],
])('finds %j in the %s view as other searches do', async (query, mode, status) => {
  const user = userEvent.setup()
  const region = await inspect(large)
  if (mode === 'Raw') await user.click(within(region).getByRole('radio', { name: 'Raw' }))
  await user.type(
    within(region).getByRole('searchbox', { name: 'Find in Provider response' }),
    query,
  )
  expect(within(region).getByRole('status')).toHaveTextContent(status)
})

// Catches: a view switch that keeps a match index but leaves the match folded away.
it('reveals the current match after you switch views', async () => {
  const user = userEvent.setup()
  const region = await inspect(large)
  await user.click(within(region).getByRole('radio', { name: 'Raw' }))
  await user.type(
    within(region).getByRole('searchbox', { name: 'Find in Provider response' }),
    'row-77',
  )
  await user.click(within(region).getByRole('radio', { name: 'Structured' }))
  const tree = structure(region)
  expect(within(region).getByRole('status')).toHaveTextContent('1 of 1')
  expect(tree.querySelector('[data-current-match]')).toHaveTextContent('row-77')
})

// Catches: JSON nested deep enough to exhaust the stack when its structure is walked.
it('shows very deeply nested JSON exactly as retained, in Raw only', async () => {
  const value = '['.repeat(5000) + ']'.repeat(5000)
  const region = await inspect(value)
  expect(within(region).queryByRole('radiogroup', { name: 'View' })).toBeNull()
  expect(within(region).getByRole('group', { name: 'Raw' }).textContent).toBe(value)
  expect(within(region).getByRole('group', { name: 'Payload tools' })).toHaveTextContent(
    'JSON · 10 kB',
  )
})

it('keeps line terminators exact in Raw and finds per line', async () => {
  const user = userEvent.setup()
  const value = '{"a":\r\n"x",\r"b":\n"x"}'
  const region = await inspect(value)
  await user.click(within(region).getByRole('radio', { name: 'Raw' }))
  expect(within(region).getByRole('group', { name: 'Raw' }).textContent).toBe(value)
  await user.type(
    within(region).getByRole('searchbox', { name: 'Find in Provider response' }),
    '^"',
  )
  expect(within(region).getByRole('status')).toHaveTextContent('1 of 3')
})
