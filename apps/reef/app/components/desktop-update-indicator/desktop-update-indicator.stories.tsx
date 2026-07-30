import type { Meta, StoryObj } from '@storybook/react-vite'

import { theme } from '@/wax/theme/theme.css'

import { DesktopUpdateIndicator } from './desktop-update-indicator'

const meta = {
  args: {
    isMinimized: false,
    state: { status: 'available', version: '0.9.0' },
  },
  component: DesktopUpdateIndicator,
  parameters: { layout: 'centered' },
  render: (args) => (
    <div
      style={{
        background: theme.surface.main,
        boxSizing: 'border-box',
        padding: 12,
        width: args.isMinimized ? 58 : 180,
      }}
    >
      <DesktopUpdateIndicator {...args} />
    </div>
  ),
  tags: ['autodocs'],
  title: 'Components/DesktopUpdateIndicator',
} satisfies Meta<typeof DesktopUpdateIndicator>

export default meta
type Story = StoryObj<typeof meta>

export const Available: Story = {}

export const Downloading: Story = {
  args: {
    state: { status: 'downloading', version: '0.9.0' },
  },
}

export const Ready: Story = {
  args: {
    state: { status: 'ready', version: '0.9.0' },
  },
}

export const Minimized: Story = {
  args: {
    isMinimized: true,
    state: { status: 'ready', version: '0.9.0' },
  },
}
