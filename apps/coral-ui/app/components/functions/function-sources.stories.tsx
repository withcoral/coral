import type { Meta, StoryObj } from '@storybook/react-vite'

import { FunctionSources } from './function-sources'

const meta = {
  args: {
    sources: ['github', 'linear', 'slack'],
  },
  component: FunctionSources,
  decorators: [
    (Story) => (
      <div style={{ width: 240 }}>
        <Story />
      </div>
    ),
  ],
  tags: ['autodocs'],
  title: 'Components/Functions/FunctionSources',
} satisfies Meta<typeof FunctionSources>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}

export const Empty: Story = {
  args: {
    sources: [],
  },
}
