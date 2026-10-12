import type { Meta, StoryObj } from '@storybook/react-vite'

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
