import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { loader } from './settings-loader'

describe('settings loader', () => {
  beforeEach(() => vi.stubEnv('CORAL_MCP_MODE', 'local'))
  afterEach(() => vi.unstubAllEnvs())

  it('loads local install commands with workspace support', () => {
    expect(loader(request())).toEqual({
      runtime: 'web',
      usesRemoteMcp: false,
      mcpClients: expect.arrayContaining([
        expect.objectContaining({
          id: 'codex',
          installCommand:
            'npx --yes add-mcp@1.11.0 "$(command -v coral)" --global --agent codex --name coral --args mcp-stdio --yes',
          workspaceInstallCommand:
            'npx --yes add-mcp@1.11.0 "$(command -v coral)" --global --agent codex --name coral --args mcp-stdio --yes',
        }),
      ]),
    })
  })

  it('shows remote setup instructions instead of a failing command for unsupported clients', () => {
    vi.stubEnv('CORAL_MCP_MODE', 'remote')
    vi.stubEnv('CORAL_MCP_URL', 'https://coral.example.com/mcp')

    const result = loader(request())
    expect(result.usesRemoteMcp).toBe(true)
    expect(result.mcpClients).toContainEqual({
      id: 'claude-desktop',
      name: 'Claude Desktop',
      setupInstructions:
        'Claude Desktop supports remote MCP servers through Settings → Connectors. Add the Coral endpoint there. https://coral.example.com/mcp',
    })
  })

  it('uses add-mcp directly for remote clients it supports', () => {
    vi.stubEnv('CORAL_MCP_MODE', 'remote')
    vi.stubEnv('CORAL_MCP_URL', 'https://coral.example.com/mcp')

    expect(loader(request()).mcpClients).toContainEqual({
      id: 'codex',
      installCommand:
        'npx -y add-mcp@1.11.0 https://coral.example.com/mcp --global --agent codex --name coral --transport http --yes',
      name: 'Codex',
    })
  })

  it('loads PowerShell commands for Windows requests', () => {
    expect(
      loader({
        request: new Request('https://reef.example/settings', {
          headers: { 'sec-ch-ua-platform': '"Windows"' },
        }),
      } as Parameters<typeof loader>[0]),
    ).toEqual({
      runtime: 'web',
      usesRemoteMcp: false,
      mcpClients: expect.arrayContaining([
        expect.objectContaining({
          id: 'codex',
          installCommand:
            'npx --yes add-mcp@1.11.0 (Get-Command coral).Source --global --agent codex --name coral --args mcp-stdio --yes',
          workspaceInstallShell: 'powershell',
        }),
      ]),
    })
  })
})

function request(headers?: HeadersInit): Parameters<typeof loader>[0] {
  return { request: new Request('https://reef.example/settings', { headers }) } as Parameters<
    typeof loader
  >[0]
}
