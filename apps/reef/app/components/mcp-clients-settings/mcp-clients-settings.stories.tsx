import type { Meta, StoryObj } from '@storybook/react-vite'
import { fn } from 'storybook/test'

import { McpClientsSettings } from './mcp-clients-settings'

const meta = {
  args: {
    clients: [
      { configuredWorkspace: 'default', id: 'claude-code', name: 'Claude Code' },
      { id: 'codex', name: 'Codex' },
    ],
    onWorkspaceChange: fn(),
    workspaces: [{ name: 'default' }, { name: 'analytics' }],
  },
  component: McpClientsSettings,
  parameters: { layout: 'padded' },
  render: (args) => (
    <div style={{ maxWidth: 960 }}>
      <McpClientsSettings {...args} />
    </div>
  ),
  tags: ['autodocs'],
  title: 'Components/McpClientsSettings',
} satisfies Meta<typeof McpClientsSettings>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}

export const Loading: Story = {
  args: { clients: [], loading: true },
}

export const Empty: Story = {
  args: { clients: [] },
}

export const Error: Story = {
  args: { clients: [], error: 'Unable to read MCP client configurations.' },
}
