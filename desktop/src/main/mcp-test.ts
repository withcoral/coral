import type { McpClientId, McpTestResult } from '../shared/types'
import { mcpClients } from './mcp-config'

function clientById(clientId: McpClientId) {
  const client = mcpClients().find((candidate) => candidate.id === clientId)
  if (!client) throw new Error(`Unknown MCP client: ${clientId}`)
  return client
}

export async function openMcpConnectionTest(clientId: McpClientId): Promise<McpTestResult> {
  const client = clientById(clientId)
  throw new Error(`Connection tests are not available for ${client.name} yet.`)
}
