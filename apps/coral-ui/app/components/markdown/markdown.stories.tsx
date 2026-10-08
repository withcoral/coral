import type { Meta, StoryObj } from '@storybook/react-vite'

import { Markdown } from './markdown'

const meta = {
  component: Markdown,
  tags: ['autodocs'],
  title: 'Components/Markdown',
} satisfies Meta<typeof Markdown>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {
  args: {
    children:
      'Use **source credentials** from your account settings. See [provider docs](https://example.com) for setup details.',
  },
}
