import type { Meta, StoryObj } from '@storybook/react-vite'

import { expect } from 'storybook/test'

import { Brand } from './brand'

const meta = {
  component: Brand,
  title: 'Wax/Brand',
  parameters: { layout: 'centered' },
  args: { compact: false },
} satisfies Meta<typeof Brand>
export default meta
type Story = StoryObj<typeof meta>

export const Lockup: Story = {}

export const Mark: Story = {
  args: { compact: true },
}

export const Labeled: Story = {
  args: { label: 'console' },
  play: async ({ canvas }) => {
    await expect(canvas.getByRole('img', { name: 'Coral console' })).toHaveTextContent('console')
  },
}
