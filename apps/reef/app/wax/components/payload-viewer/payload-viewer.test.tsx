/** @vitest-environment happy-dom */
import { render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, it, vi } from 'vitest'

import { PayloadViewer } from './payload-viewer'

const view = (value: string, format: 'json' | 'text' = 'json', label = 'Provider response') => {
  render(<PayloadViewer value={value} format={format} label={label} />)
  return screen.getByRole('region', { name: label })
}
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

const large = JSON.stringify({
  rows: Array.from({ length: 120 }, (_, index) => ({ id: index, name: `row-${index}` })),
  meta: { page: 1 },
  note: 'n'.repeat(400),
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

// Catches: a page that cannot tell while you inspect a payload, so it moves on under you,
// or that stays held after the expanded viewer closes or goes away.
it('says when its expanded viewer opens and closes, and when it goes away open', async () => {
  const onExpandedChange = vi.fn()
  const { unmount } = render(
    <PayloadViewer
      value='{"a":1}'
      format="json"
      label="Provider response"
      onExpandedChange={onExpandedChange}
    />,
  )
  const user = userEvent.setup()
  const expand = screen.getByRole('button', { name: 'Expand Provider response' })
  expect(onExpandedChange).not.toHaveBeenCalled()
  await user.click(expand)
  await screen.findByRole('dialog', { name: 'Provider response' })
  expect(onExpandedChange.mock.calls).toEqual([[true]])
  await user.keyboard('{Escape}')
  await vi.waitFor(() => expect(onExpandedChange.mock.calls).toEqual([[true], [false]]))
  expect(screen.queryByRole('dialog')).toBeNull()
  expect(expand).toHaveFocus()
  // Open when it unmounts, it says it closed.
  await user.click(expand)
  await screen.findByRole('dialog', { name: 'Provider response' })
  unmount()
  expect(onExpandedChange.mock.calls).toEqual([[true], [false], [true], [false]])
})
