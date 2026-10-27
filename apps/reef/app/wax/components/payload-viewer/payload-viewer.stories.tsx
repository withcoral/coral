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
 * none), or actions that take a column down its whole height. The payload scrolls in a body as
 * wide as its card, under a compact action row that stays in view.
 */
const bounded: Story['play'] = async ({ canvasElement, globals }) => {
  const region = within(canvasElement).getByRole('region')
  const content = within(region).getByRole('group', { name: 'Payload content' })
  const actions = within(region).getByRole('group', { name: 'Payload actions' })
  const card = content.parentElement!
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
  // The code takes the card's full width; the actions take only a row at its top right.
  const [cardBox, actionsBox, contentBox] = [card, actions, content].map((element) =>
    element.getBoundingClientRect(),
  )
  await expect(contentBox.width).toBeGreaterThanOrEqual(card.clientWidth - 1)
  await expect(actionsBox.bottom).toBeLessThanOrEqual(contentBox.top + 1)
  await expect(actionsBox.top - cardBox.top).toBeLessThan(12)
  await expect(cardBox.right - actionsBox.right).toBeLessThan(12)
  await expect(actionsBox.height).toBeLessThan(40)
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
