import type { Meta, StoryObj } from '@storybook/react-vite'

import { McpClientInstall } from './mcp-client-install'

const meta = {
  args: {
    clients: [
      { id: 'claude-code', install: { shell: 'posix', transport: 'stdio' }, name: 'Claude Code' },
      { id: 'codex', install: { shell: 'posix', transport: 'stdio' }, name: 'Codex' },
      { id: 'vscode', install: { shell: 'posix', transport: 'stdio' }, name: 'VS Code' },
    ],
    workspaces: [{ name: 'analytics' }, { name: 'team-analytics-insights' }],
  },
  component: McpClientInstall,
  parameters: { layout: 'padded' },
  title: 'Components/McpClientInstall',
} satisfies Meta<typeof McpClientInstall>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}

/**
 * Nothing to install until a workspace exists to name. This page sits outside
 * the workspace routes, so it renders for a caller who holds no membership yet
 * and for one whose workspace listing failed.
 */
export const WithoutWorkspaces: Story = {
  args: { workspaces: [] },
}

/**
 * A remote deployment, where the endpoint URL carries the workspace. Switch the
 * client picker to Claude Desktop for the one client add-mcp cannot configure
 * over HTTP: it shows its own steps, and the workspace picker goes away.
 *
 * The endpoint in those steps is the bare `/mcp` URL the loader passes through,
 * which the server answers with a 404. Reproduced here as it renders today.
 */
export const Remote: Story = {
  args: {
    clients: [
      {
        id: 'codex',
        install: { transport: 'http', url: 'https://coral.example.com/mcp' },
        name: 'Codex',
      },
      {
        id: 'claude-desktop',
        name: 'Claude Desktop',
        setupInstructions:
          'Claude Desktop supports remote MCP servers through Settings → Connectors. Add the Coral endpoint there. https://coral.example.com/mcp',
      },
    ],
  },
}
