import { shell } from 'electron'
import { agents, upsertServer, type AgentType, type McpServerConfig } from 'add-mcp'
import type { McpClientDescriptor, McpClientId, McpConfigureResult } from '../shared/types'
import { externalCoralPath } from './sidecar'

// Display order for the Connect page and the app menu.
const MCP_CLIENT_IDS: readonly McpClientId[] = [
  'claude-code',
  'codex',
  'claude-desktop',
  'vscode',
  'cursor',
]

const MCP_SERVER_NAME = 'coral'

const TEST_PROMPT =
  'Use the coral MCP tools to explore my data: list the available tables, then run an example SQL query on one of them and summarize the result.'

// Deep links are constructed here from fixed templates so the renderer can
// never route an arbitrary URL through shell.openExternal.
const TEST_DEEP_LINKS: Partial<Record<McpClientId, (prompt: string) => string>> = {
  'claude-desktop': (prompt) => `claude://claude.ai/new?q=${encodeURIComponent(prompt)}`,
  codex: (prompt) => `codex://threads/new?prompt=${encodeURIComponent(prompt)}`,
}

function agentTypeForClient(clientId: McpClientId): AgentType {
  return clientId
}

function descriptorForClient(clientId: McpClientId): McpClientDescriptor {
  const agent = agents[agentTypeForClient(clientId)]
  return {
    id: clientId,
    name: agent.displayName,
    configPath: agent.configPath,
    testable: clientId in TEST_DEEP_LINKS,
  }
}

export function mcpClients(): McpClientDescriptor[] {
  return MCP_CLIENT_IDS.map(descriptorForClient)
}

function findClient(clientId: McpClientId): McpClientDescriptor {
  if (!MCP_CLIENT_IDS.includes(clientId)) throw new Error(`Unknown MCP client: ${clientId}`)
  return descriptorForClient(clientId)
}

export async function configureMcpClient(clientId: McpClientId): Promise<McpConfigureResult> {
  const client = findClient(clientId)
  const coralPath = await externalCoralPath()
  const serverConfig: McpServerConfig = {
    command: coralPath,
    args: ['mcp-stdio'],
  }

  const result = upsertServer(agentTypeForClient(client.id), MCP_SERVER_NAME, serverConfig, {
    local: false,
  })
  if (!result.success) {
    throw new Error(result.error ?? `Failed to configure ${client.name} MCP`)
  }

  const configPath = result.path || client.configPath
  return {
    client: { ...client, configPath },
    configPath,
  }
}

export async function mcpAddCommand(): Promise<string> {
  const coralPath = await externalCoralPath()
  return `npx add-mcp "${coralPath} mcp-stdio" --name ${MCP_SERVER_NAME} -g`
}

export async function testMcpClient(clientId: McpClientId): Promise<void> {
  const client = findClient(clientId)
  const deepLink = TEST_DEEP_LINKS[client.id]
  if (!deepLink) throw new Error(`${client.name} does not support a connection test`)
  await shell.openExternal(deepLink(TEST_PROMPT))
}
