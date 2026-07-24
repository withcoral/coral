import {
  agents,
  getAgentTypes,
  listInstalledServers,
  removeServer,
  upsertServer,
  type AgentServers,
  type AgentType,
  type InstalledServer,
  type McpServerConfig,
} from 'add-mcp'
import { existsSync } from 'node:fs'
import { isDeepStrictEqual } from 'node:util'
import type { McpClientDescriptor, McpLaunchConfig } from '../shared/types'
import { externalCoralPath } from './sidecar'

const DEFAULT_WORKSPACE = 'default'

type CoralEntry =
  | { kind: 'absent' }
  | { kind: 'collision' }
  | { kind: 'local'; workspace: string }

function descriptor(
  client: Pick<AgentServers, 'agentType' | 'displayName'>,
  configuredWorkspace?: string,
): McpClientDescriptor {
  return {
    ...(configuredWorkspace ? { configuredWorkspace } : {}),
    id: client.agentType,
    name: client.displayName,
  }
}

function supportsStdio(clientId: AgentType): boolean {
  return agents[clientId].supportedTransports.includes('stdio')
}

async function stdioClients(): Promise<AgentServers[]> {
  const supported = getAgentTypes().filter(supportsStdio)
  // Explicit ids keep not-yet-installed clients available for preconfiguration.
  return listInstalledServers({ agents: supported, global: true })
}

async function requireStdioClient(clientId: unknown): Promise<AgentServers> {
  const client =
    typeof clientId === 'string'
      ? (await stdioClients()).find(({ agentType }) => agentType === clientId)
      : undefined
  if (!client) throw new Error('Unknown or unsupported MCP client.')
  return client
}

function requireWorkspaceArgument(value: unknown): string {
  if (typeof value !== 'string' || value.length === 0) {
    throw new Error('A Coral workspace is required.')
  }
  return value
}

function stringArray(value: unknown): string[] | undefined {
  return Array.isArray(value) && value.every((item) => typeof item === 'string') ? value : undefined
}

function localInvocation(
  config: Record<string, unknown>,
): { args: string[]; command: string } | undefined {
  const commandArray = stringArray(config.command)
  const command =
    commandArray?.[0] ??
    (typeof config.command === 'string' ? config.command : undefined) ??
    (typeof config.cmd === 'string' ? config.cmd : undefined)
  const args = commandArray ? commandArray.slice(1) : stringArray(config.args)
  return command && args ? { args, command } : undefined
}

function workspaceFromCanonicalArgs(args: readonly string[]): string | undefined {
  if (args.length === 1 && args[0] === 'mcp-stdio') return DEFAULT_WORKSPACE
  if (args.length !== 2 || args[0] !== 'mcp-stdio') return undefined

  const workspaceArg = args[1]
  if (!workspaceArg.startsWith('--workspace=')) return undefined
  return workspaceArg.slice('--workspace='.length) || undefined
}

function persistedShape(value: unknown): unknown {
  // Client config files omit undefined transform fields (notably Codex's env).
  const json = JSON.stringify(value)
  return json === undefined ? undefined : JSON.parse(json)
}

function isCanonicalCoralConfig(server: InstalledServer, invocation: McpServerConfig): boolean {
  const expected = agents[server.agentType].transformConfig('coral', invocation, {
    local: false,
  })
  return isDeepStrictEqual(persistedShape(server.config), persistedShape(expected))
}

function coralEntry(servers: readonly InstalledServer[]): CoralEntry {
  const server = servers.find(({ serverName }) => serverName === 'coral')
  if (!server) return { kind: 'absent' }

  const invocation = localInvocation(server.config)
  const executable = invocation?.command.split(/[\\/]/).at(-1)
  if (!invocation || !/^coral(?:\.exe)?$/i.test(executable ?? '')) {
    return { kind: 'collision' }
  }

  const workspace = workspaceFromCanonicalArgs(invocation.args)
  if (!workspace || !isCanonicalCoralConfig(server, invocation)) {
    return { kind: 'collision' }
  }
  return { kind: 'local', workspace }
}

export async function mcpClients(): Promise<McpClientDescriptor[]> {
  return (await stdioClients())
    .sort(
      (left, right) =>
        Number(right.detected) - Number(left.detected) ||
        left.displayName.localeCompare(right.displayName) ||
        left.agentType.localeCompare(right.agentType),
    )
    .map((client) => {
      const entry = coralEntry(client.servers)
      return descriptor(client, entry.kind === 'local' ? entry.workspace : undefined)
    })
}

async function mcpLaunchConfig(workspaceName?: string): Promise<McpLaunchConfig> {
  return {
    args: workspaceName ? ['mcp-stdio', `--workspace=${workspaceName}`] : ['mcp-stdio'],
    command: await externalCoralPath(),
  }
}

export async function getMcpLaunchConfig(): Promise<McpLaunchConfig> {
  return mcpLaunchConfig()
}

function requireManageableCoralEntry(client: AgentServers): void {
  if (!client.detected && existsSync(client.configPath)) {
    throw new Error(
      `${client.displayName} was not detected, but its global MCP config already exists; Coral will not overwrite it.`,
    )
  }

  if (coralEntry(client.servers).kind === 'collision') {
    throw new Error(
      `${client.displayName} already has an incompatible global MCP server named "coral".`,
    )
  }
}

export async function configureMcpClient(clientId: unknown, workspaceName: unknown): Promise<void> {
  const workspace = requireWorkspaceArgument(workspaceName)
  const client = await requireStdioClient(clientId)
  requireManageableCoralEntry(client)

  const serverConfig: McpServerConfig = await mcpLaunchConfig(workspace)
  const result = upsertServer(client.agentType, 'coral', serverConfig, { local: false })
  if (!result.success) {
    throw new Error(
      `Failed to configure ${client.displayName}: ${result.error ?? 'could not write the Coral entry'}`,
    )
  }
}

export async function removeMcpClient(clientId: unknown): Promise<void> {
  const client = await requireStdioClient(clientId)
  requireManageableCoralEntry(client)

  const result = removeServer(client.agentType, 'coral', { local: false })
  if (!result.success) {
    throw new Error(
      `Failed to remove Coral from ${client.displayName}: ${result.error ?? 'could not remove the Coral entry'}`,
    )
  }
}
