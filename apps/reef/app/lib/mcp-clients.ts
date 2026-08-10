export interface WebMcpClient {
  readonly id: string
  readonly name: string
}

// This manually maintained catalog is consumed by the pinned add-mcp installer
// in this feature branch. Client-specific config formats stay inside add-mcp.
export const webMcpClients: readonly WebMcpClient[] = [
  { id: 'antigravity', name: 'Antigravity' },
  { id: 'cline', name: 'Cline' },
  { id: 'cline-cli', name: 'Cline CLI' },
  { id: 'claude-code', name: 'Claude Code' },
  { id: 'claude-desktop', name: 'Claude Desktop' },
  { id: 'codex', name: 'Codex' },
  { id: 'cursor', name: 'Cursor' },
  { id: 'gemini-cli', name: 'Gemini CLI' },
  { id: 'goose', name: 'Goose' },
  { id: 'github-copilot-cli', name: 'GitHub Copilot CLI' },
  { id: 'mcporter', name: 'Mcporter' },
  { id: 'opencode', name: 'OpenCode' },
  { id: 'vscode', name: 'VS Code' },
  { id: 'windsurf', name: 'Windsurf' },
  { id: 'zed', name: 'Zed' },
]

export function mcpClientById(clientId: string | undefined): WebMcpClient | undefined {
  return webMcpClients.find((client) => client.id === clientId)
}

export function mcpClientInstallPath(clientId: string): string {
  return `/mcp/install/${encodeURIComponent(clientId)}`
}
