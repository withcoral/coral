import type { Meta, StoryObj } from '@storybook/react-vite'
import { expect, waitFor, within } from 'storybook/test'
import { PayloadViewer } from './payload-viewer'

const largeJson = JSON.stringify({
  items: Array.from({ length: 80 }, (_, index) => ({
    id: index,
    name: `item-${index}`,
    tags: ['a', 'b'],
  })),
  cursor: { next: 'abc' },
  description: 'A long description that folds to its start. '.repeat(10),
})

const meta = {
  title: 'Wax/PayloadViewer',
  component: PayloadViewer,
  args: {
    label: 'Provider response',
    format: 'json',
    value: '{"id":9007199254740993,"ratio":1.0e3,"id":"duplicate","list":[1,true,null],"empty":{}}',
  },
} satisfies Meta<typeof PayloadViewer>
export default meta
type Story = StoryObj<typeof meta>

/** A small value opens whole, every token as retained. */
export const SmallJson: Story = {}
/** A large value opens shallow; its long string folds to its start. */
export const LargeJson: Story = { args: { value: largeJson } }
export const LargeJsonNarrow: Story = {
  args: { value: largeJson },
  parameters: { chromatic: { viewports: [390] } },
}
export const TextLog: Story = {
  args: {
    label: 'Log message',
    format: 'text',
    value: 'Fetched page 1\n  then page 2\n\tindented with a tab\n' + 'A long line. '.repeat(20),
  },
}
/**
 * A huge array shows 200 items at a time, and says how many more there are. Catches: every
 * item rendered at once, or a card that grows with them instead of scrolling to that line.
 */
export const HugeArray: Story = {
  args: {
    value: JSON.stringify(Array.from({ length: 1000 }, (_, index) => ({ id: index }))),
  },
  play: async ({ canvasElement, userEvent }) => {
    const content = within(canvasElement).getByRole('group', { name: 'Payload content' })
    const tree = within(content).getByRole('group', { name: 'Structure' })
    await expect(within(tree).getAllByRole('button', { name: /^Item \d+:/ })).toHaveLength(200)
    const bound = Math.min(320, canvasElement.ownerDocument.defaultView!.innerHeight / 2)
    await expect(content.getBoundingClientRect().height).toBeLessThanOrEqual(bound + 0.5)
    // At the card's end, the line says what follows and shows the next items.
    content.scrollTop = content.scrollHeight
    const next = within(tree).getByRole('button', { name: 'Show next 200: Value' })
    await waitFor(() => {
      const [box, card] = [next, content].map((element) => element.getBoundingClientRect())
      expect(box.top).toBeGreaterThanOrEqual(card.top)
      expect(box.bottom).toBeLessThanOrEqual(card.bottom)
    })
    await expect(next.closest('div')).toHaveTextContent('800 more items')
    await userEvent.click(next)
    await expect(within(tree).getAllByRole('button', { name: /^Item \d+:/ })).toHaveLength(400)
  },
}
export const MalformedJson: Story = { args: { value: '{"status": 200, "data": [1, 2' } }
export const JsonNull: Story = { args: { label: 'Final value', value: 'null' } }
export const Truncated: Story = {
  args: { label: 'Upstream response data', value: '{"message":"Too many', truncated: true },
}

/** A short, narrow phone screen. Chromatic reads `viewports`; Vitest reads the global. */
const narrow = {
  globals: { viewport: { value: 'mobile1', isRotated: false } },
  parameters: { chromatic: { viewports: [320] } },
}
const tallJson = JSON.stringify(
  Object.fromEntries(Array.from({ length: 40 }, (_, index) => [`key${index}`, `value ${index}`])),
)
const tallText = Array.from({ length: 60 }, (_, index) => `log line ${index + 1}`).join('\n')
const tallMalformed = `${Array.from({ length: 60 }, (_, index) => `"line ${index}",`).join('\n')} [`

/**
 * Catches: an inline payload with no working height bound (an invalid max-height computes to
 * none), or actions that take room inside its card. The actions sit in a compact row above
 * the bordered payload, at its right; the payload takes the full width and scrolls in itself.
 */
const bounded: Story['play'] = async ({ canvasElement, globals }) => {
  const region = within(canvasElement).getByRole('region')
  const content = within(region).getByRole('group', { name: 'Payload content' })
  const actions = within(region).getByRole('group', { name: 'Payload actions' })
  const view = canvasElement.ownerDocument.defaultView!
  const bound = Math.min(320, view.innerHeight / 2)
  // A narrow story runs on the short phone screen, where half the screen is the bound.
  if (globals.viewport?.value === 'mobile1') {
    await expect(view.innerWidth).toBeLessThanOrEqual(360)
    await expect(bound).toBeLessThan(320)
  }
  const maxHeight = getComputedStyle(content).maxHeight
  await expect(maxHeight).toMatch(/^[\d.]+px$/)
  await expect(parseFloat(maxHeight)).toBeCloseTo(bound, 0)
  await expect(content.getBoundingClientRect().height).toBeLessThanOrEqual(bound + 0.5)
  await expect(content.scrollHeight).toBeGreaterThan(content.clientHeight)
  // The actions sit above the bordered payload, right-aligned with it, not inside it.
  const [regionBox, actionsBox, contentBox] = [region, actions, content].map((element) =>
    element.getBoundingClientRect(),
  )
  await expect(content.contains(actions)).toBe(false)
  await expect(getComputedStyle(content).borderTopStyle).toBe('solid')
  await expect(actionsBox.bottom).toBeLessThanOrEqual(contentBox.top)
  await expect(Math.abs(contentBox.right - actionsBox.right)).toBeLessThan(4)
  await expect(actionsBox.left).toBeGreaterThan(contentBox.left + contentBox.width / 2)
  await expect(actionsBox.height).toBeLessThan(40)
  // The payload takes the full width.
  await expect(contentBox.width).toBeGreaterThanOrEqual(regionBox.width - 1)
  // It scrolls inside itself; the actions stay where they were.
  content.scrollTop = 160
  await waitFor(() => expect(content.scrollTop).toBeGreaterThan(0))
  await expect(actions.getBoundingClientRect().top).toBe(actionsBox.top)
}
export const BoundedJson: Story = { args: { value: tallJson }, play: bounded }
export const BoundedJsonNarrow: Story = { ...BoundedJson, ...narrow }
export const BoundedRaw: Story = { args: { value: tallMalformed }, play: bounded }
export const BoundedRawNarrow: Story = { ...BoundedRaw, ...narrow }
export const BoundedText: Story = {
  args: { label: 'Log message', format: 'text', value: tallText },
  play: bounded,
}
export const BoundedTextNarrow: Story = { ...BoundedText, ...narrow }

/**
 * Catches: an expanded viewer whose close control leaves the standard Wax header: it sits on
 * the title's row at the top right, the title at the left. Escape and the close control both
 * close it, and focus returns to Expand.
 */
const expandedHeader: Story['play'] = async ({ canvasElement, userEvent, globals }) => {
  const page = within(canvasElement.ownerDocument.body)
  if (globals.viewport?.value === 'mobile1')
    await expect(canvasElement.ownerDocument.defaultView!.innerWidth).toBeLessThanOrEqual(360)
  const expand = within(canvasElement).getByRole('button', { name: 'Expand Provider response' })
  expand.focus()
  await userEvent.keyboard('{Enter}')
  const dialog = await page.findByRole('dialog', { name: 'Provider response' })
  const title = within(dialog).getByRole('heading', { name: 'Provider response' })
  const close = within(dialog).getByRole('button', { name: 'Close' })
  await waitFor(() => expect(close).toBeVisible())
  const [dialogBox, titleBox, closeBox] = [dialog, title, close].map((element) =>
    element.getBoundingClientRect(),
  )
  // One row: the close control's centre falls within the title's line.
  const middle = closeBox.top + closeBox.height / 2
  await expect(middle).toBeGreaterThanOrEqual(titleBox.top)
  await expect(middle).toBeLessThanOrEqual(titleBox.bottom)
  // Title at the left, close control at the right, apart.
  await expect(titleBox.left - dialogBox.left).toBeLessThan(40)
  await expect(dialogBox.right - closeBox.right).toBeLessThan(40)
  await expect(closeBox.left).toBeGreaterThan(titleBox.right)
  // Above the tools.
  const tools = within(dialog).getByRole('group', { name: 'Payload tools' })
  await expect(closeBox.bottom).toBeLessThanOrEqual(tools.getBoundingClientRect().top)
  // Tab and Shift+Tab cycle inside the dialog; focus never reaches the page behind it.
  const focusables = dialog.querySelectorAll('button, input, [tabindex="0"]').length
  for (const key of ['{Tab}', '{Shift>}{Tab}{/Shift}'])
    for (let press = 0; press <= focusables; press++) {
      await userEvent.keyboard(key)
      // Base UI's focus guard at either end hands focus back into the dialog.
      await waitFor(() =>
        expect(dialog).toContainElement(canvasElement.ownerDocument.activeElement as HTMLElement),
      )
    }
  await userEvent.keyboard('{Escape}')
  await waitFor(() => expect(page.queryByRole('dialog')).toBeNull())
  await waitFor(() => expect(expand).toHaveFocus())
  // The close control closes it too, and focus returns the same way.
  await userEvent.click(expand)
  await userEvent.click(
    within(await page.findByRole('dialog', { name: 'Provider response' })).getByRole('button', {
      name: 'Close',
    }),
  )
  await waitFor(() => expect(page.queryByRole('dialog')).toBeNull())
  await waitFor(() => expect(expand).toHaveFocus())
}
export const ExpandedHeader: Story = { args: { value: largeJson }, play: expandedHeader }
export const ExpandedHeaderNarrow: Story = { ...ExpandedHeader, ...narrow }
