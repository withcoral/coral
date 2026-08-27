import type { Meta, StoryObj } from '@storybook/react-vite'

import { McpClientInstallList } from './mcp-client-install-list'

const meta = {
  args: {
    clients: [
      {
        id: 'claude-code',
        installCommand: 'curl -fsSL https://coral-ui.example/mcp/install/claude-code | sh',
        name: 'Claude Code',
      },
      {
        id: 'codex',
        installCommand:
          'npx -y add-mcp@1.11.0 https://coral.example/mcp --global --agent codex --name coral --transport http --yes',
        name: 'Codex',
        workspaceInstallUrl: 'https://coral.example/mcp',
      },
      {
        id: 'claude-desktop',
        name: 'Claude Desktop',
        setupInstructions:
          'Claude Desktop supports remote MCP servers through Settings → Connectors. Add the Coral endpoint there.',
      },
    ],
    workspaces: [{ name: 'team-analytics-insights' }],
  },
  component: McpClientInstallList,
  parameters: { layout: 'padded' },
  title: 'Components/McpClientInstallList',
} satisfies Meta<typeof McpClientInstallList>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}

export const PowerShell: Story = {
  args: {
    clients: [
      {
        id: 'codex',
        installCommand:
          'npx --yes add-mcp@1.11.0 (Get-Command coral).Source --global --agent codex --name coral --args mcp-stdio --yes',
        installCommandLabel: 'Requires PowerShell',
        name: 'Codex',
        workspaceInstallCommand:
          'npx --yes add-mcp@1.11.0 (Get-Command coral).Source --global --agent codex --name coral --args mcp-stdio --yes',
        workspaceInstallShell: 'powershell',
      },
    ],
  },
}
