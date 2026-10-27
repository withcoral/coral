import type { Meta, StoryObj } from '@storybook/react-vite'
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
