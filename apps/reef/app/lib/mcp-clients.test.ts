import { describe, expect, it } from 'vitest'

import { mcpClientById, mcpClientInstallPath, webMcpClients } from './mcp-clients'

describe('web MCP clients', () => {
  it('exposes only unique, script-addressable client IDs', () => {
    expect(webMcpClients).not.toHaveLength(0)
    expect(new Set(webMcpClients.map((client) => client.id)).size).toBe(webMcpClients.length)

    for (const client of webMcpClients) {
      expect(mcpClientById(client.id)).toEqual(client)
      expect(mcpClientInstallPath(client.id)).toBe(`/mcp/install/${client.id}`)
    }
  })

  it('does not resolve unknown clients into an install route', () => {
    expect(mcpClientById('not-a-client')).toBeUndefined()
  })
})
