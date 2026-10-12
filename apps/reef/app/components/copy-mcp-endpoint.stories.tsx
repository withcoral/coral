import type { Meta, StoryObj } from '@storybook/react-vite'

import { CopyMcpEndpoint } from './copy-mcp-endpoint'

const meta = {
  component: CopyMcpEndpoint,
  parameters: {
    layout: 'centered',
  },
  title: 'Components/CopyMcpEndpoint',
} satisfies Meta<typeof CopyMcpEndpoint>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {
  args: {
    endpoint: 'https://coral.example.com/workspaces/finance/mcp',
  },
}
