import { agents, upsertServer, type AgentType, type McpServerConfig } from 'add-mcp'
import type {
  McpClientDescriptor,
  McpClientId,
  McpConfigureResult,
  McpLaunchConfig,
} from '../shared/types'
import { externalCoralPath } from './sidecar'

const MCP_CLIENT_IDS: readonly McpClientId[] = ['codex', 'claude-code']

function agentTypeForClient(clientId: McpClientId): AgentType {
  return clientId
}

function descriptorForClient(clientId: McpClientId): McpClientDescriptor {
  const agent = agents[agentTypeForClient(clientId)]
  return {
    id: clientId,
    name: agent.displayName,
    configPath: agent.configPath,
  }
}

export function mcpClients(): McpClientDescriptor[] {
  return MCP_CLIENT_IDS.map(descriptorForClient)
}

function findClient(clientId: McpClientId): McpClientDescriptor {
  if (!MCP_CLIENT_IDS.includes(clientId)) throw new Error(`Unknown MCP client: ${clientId}`)
  return descriptorForClient(clientId)
}

export async function getMcpLaunchConfig(): Promise<McpLaunchConfig> {
  return {
    args: ['mcp-stdio'],
    command: await externalCoralPath(),
  }
}

export async function configureMcpClient(clientId: McpClientId): Promise<McpConfigureResult> {
  const client = findClient(clientId)
  const serverConfig: McpServerConfig = await getMcpLaunchConfig()

  const result = upsertServer(agentTypeForClient(client.id), 'coral', serverConfig, {
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
