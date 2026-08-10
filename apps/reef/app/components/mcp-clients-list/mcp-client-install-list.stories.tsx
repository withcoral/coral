import type { Meta, StoryObj } from '@storybook/react-vite'

import { McpClientInstallList } from './mcp-client-install-list'

const meta = {
  args: {
    clients: [
      {
        id: 'claude-code',
        installCommand: 'curl -fsSL https://reef.example/mcp/install/claude-code | sh',
        name: 'Claude Code',
      },
      {
        id: 'codex',
        installCommand: 'curl -fsSL https://reef.example/mcp/install/codex | sh',
        name: 'Codex',
      },
    ],
  },
  component: McpClientInstallList,
  parameters: { layout: 'padded' },
  title: 'Components/McpClientInstallList',
} satisfies Meta<typeof McpClientInstallList>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}
