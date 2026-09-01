import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { loader } from './settings-loader'

describe('settings loader', () => {
  beforeEach(() => vi.stubEnv('CORAL_MCP_MODE', 'local'))
  afterEach(() => vi.unstubAllEnvs())

  it('loads local stdio installs', () => {
    expect(loader(request())).toEqual({
      runtime: 'web',
      mcpClients: expect.arrayContaining([
        { id: 'codex', install: { shell: 'posix', transport: 'stdio' }, name: 'Codex' },
      ]),
    })
  })

  it('shows remote setup instructions instead of a failing command for unsupported clients', () => {
    vi.stubEnv('CORAL_MCP_MODE', 'remote')
    vi.stubEnv('CORAL_MCP_URL', 'https://coral.example.com/mcp')

    const result = loader(request())
    expect(result.mcpClients).toContainEqual({
      id: 'claude-desktop',
      name: 'Claude Desktop',
      setupInstructions:
        'Claude Desktop supports remote MCP servers through Settings → Connectors. Add the Coral endpoint there. https://coral.example.com/mcp',
    })
  })

  it('scopes remote installs through the endpoint URL', () => {
    vi.stubEnv('CORAL_MCP_MODE', 'remote')
    vi.stubEnv('CORAL_MCP_URL', 'https://coral.example.com/mcp')

    expect(loader(request()).mcpClients).toContainEqual({
      id: 'codex',
      install: { transport: 'http', url: 'https://coral.example.com/mcp' },
      name: 'Codex',
    })
  })

  it('loads PowerShell installs for Windows requests', () => {
    expect(
      loader({
        request: new Request('https://reef.example/settings', {
          headers: { 'sec-ch-ua-platform': '"Windows"' },
        }),
      } as Parameters<typeof loader>[0]),
    ).toEqual({
      runtime: 'web',
      mcpClients: expect.arrayContaining([
        { id: 'codex', install: { shell: 'powershell', transport: 'stdio' }, name: 'Codex' },
      ]),
    })
  })
})

function request(headers?: HeadersInit): Parameters<typeof loader>[0] {
  return { request: new Request('https://reef.example/settings', { headers }) } as Parameters<
    typeof loader
  >[0]
}
