import { shell } from 'electron'
import type { McpClientId, McpTestResult } from '../shared/types'
import { mcpClients } from './mcp-config'

const CLAUDE_TEST_PROMPT = [
  'Test my Coral MCP connection.',
  '',
  'Use the Coral MCP server, not web search or built-in connectors. Run a Coral catalog discovery, for example by using the Coral catalog tools or this SQL:',
  '',
  'SELECT schema_name, table_name FROM coral.tables ORDER BY schema_name, table_name LIMIT 10',
  '',
  'Tell me whether Coral is connected, then show the first schemas/tables you found. If the Coral tools are not available, say that the Coral MCP server is not connected.',
].join('\n')

function clientById(clientId: McpClientId) {
  const client = mcpClients().find((candidate) => candidate.id === clientId)
  if (!client) throw new Error(`Unknown MCP client: ${clientId}`)
  return client
}

function claudeTestUrl(): string {
  const url = new URL('claude://claude.ai/new')
  url.searchParams.set('q', CLAUDE_TEST_PROMPT)
  return url.toString()
}

export async function openMcpConnectionTest(clientId: McpClientId): Promise<McpTestResult> {
  const client = clientById(clientId)
  if (client.id !== 'claude-desktop') {
    throw new Error('Connection tests are currently available for Claude Desktop only.')
  }

  const launchUrl = claudeTestUrl()
  await shell.openExternal(launchUrl)
  return {
    client,
    launchUrl,
    message: 'Opened Claude with a Coral MCP test prompt. Review it and send it in Claude.',
  }
}
