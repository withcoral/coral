import { describe, expect, it } from 'vitest'

import { mcpConnectionFromEnv } from './mcp-connection'

describe('MCP connection configuration', () => {
  it('defaults to the local stdio installer', () => {
    expect(mcpConnectionFromEnv({})).toEqual({ mode: 'local' })
  })

  it('uses a configured remote HTTPS endpoint', () => {
    expect(
      mcpConnectionFromEnv({
        CORAL_MCP_MODE: 'remote',
        CORAL_MCP_URL: 'https://coral.example.com/mcp',
      }),
    ).toEqual({ mode: 'remote', url: 'https://coral.example.com/mcp' })
  })

  it('rejects invalid and credential-bearing remote endpoints', () => {
    expect(() => mcpConnectionFromEnv({ CORAL_MCP_MODE: 'remote' })).toThrow(
      'CORAL_MCP_URL must be set',
    )
    expect(() =>
      mcpConnectionFromEnv({ CORAL_MCP_MODE: 'remote', CORAL_MCP_URL: 'http://coral.test/mcp' }),
    ).toThrow('absolute HTTPS URL')
    expect(() =>
      mcpConnectionFromEnv({
        CORAL_MCP_MODE: 'remote',
        CORAL_MCP_URL: 'https://token@coral.test/mcp',
      }),
    ).toThrow('without credentials')
  })

  it('rejects blank and unknown modes', () => {
    expect(() => mcpConnectionFromEnv({ CORAL_MCP_MODE: ' ' })).toThrow(
      'CORAL_MCP_MODE must be "local" or "remote"',
    )
    expect(() => mcpConnectionFromEnv({ CORAL_MCP_MODE: 'stdio' })).toThrow(
      'CORAL_MCP_MODE must be "local" or "remote"',
    )
  })
})
