import type { Meta, StoryObj } from '@storybook/react-vite'
import { fn } from 'storybook/test'

import { McpClientsList } from './mcp-clients-list'

const meta = {
  args: {
    clients: [
      { configuredWorkspace: 'default', id: 'claude-code', name: 'Claude Code' },
      { id: 'codex', name: 'Codex' },
    ],
    onWorkspaceChange: fn(),
    workspaces: [{ name: 'default' }, { name: 'analytics' }],
  },
  component: McpClientsList,
  parameters: { layout: 'padded' },
  render: (args) => (
    <div style={{ maxWidth: 960 }}>
      <McpClientsList {...args} />
    </div>
  ),
  tags: ['autodocs'],
  title: 'Components/McpClientsList',
} satisfies Meta<typeof McpClientsList>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}

export const Bounded: Story = {
  args: {
    clients: ['Claude Code', 'Claude Desktop', 'Codex', 'Cursor', 'VS Code', 'Zed'].map(
      (name, index) => ({
        configuredWorkspace: index === 0 ? 'default' : undefined,
        id: name.toLowerCase().replaceAll(' ', '-'),
        name,
      }),
    ),
    maxHeight: 200,
    pendingClientIds: ['codex'],
  },
}

export const Error: Story = {
  args: { clients: [], error: 'Unable to read MCP client configurations.' },
}
