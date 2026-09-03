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
import { isDeepStrictEqual } from 'node:util'
import type { McpClientDescriptor, McpLaunchConfig } from '../shared/types'
import { desktopCoralStateDir, externalCoralPath } from './sidecar'

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

function desktopCoralEnv(): Record<string, string> {
  return { CORAL_CONFIG_DIR: desktopCoralStateDir() }
}

/**
 * Whether this entry is one Desktop wrote, and so one it may rewrite.
 *
 * Entries written before Desktop pointed clients at its own state carry no
 * `CORAL_CONFIG_DIR`, and are recognised here so the app can still show and
 * upgrade them. Rejecting them would strand a user with an entry Desktop
 * refuses to touch and no way to fix it but hand-editing the client's config.
 */
function isCanonicalCoralConfig(server: InstalledServer, invocation: McpServerConfig): boolean {
  const persisted = persistedShape(server.config)
  const candidates: McpServerConfig[] = [
    { ...invocation, env: desktopCoralEnv() },
    // The shape Desktop wrote before it pointed clients at its own state.
    invocation,
  ]
  return candidates.some((candidate) =>
    isDeepStrictEqual(
      persisted,
      persistedShape(agents[server.agentType].transformConfig('coral', candidate, { local: false })),
    ),
  )
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

/**
 * The invocation Desktop writes into a client's config.
 *
 * `CORAL_CONFIG_DIR` is what makes the server the client launches the same
 * Coral this app shows. Without it the binary resolves its own default
 * directory, and the agent reads a different set of sources and workspaces
 * while its queries never reach the Traces view.
 */
async function mcpLaunchConfig(workspaceName?: string): Promise<McpLaunchConfig> {
  return {
    args: workspaceName ? ['mcp-stdio', `--workspace=${workspaceName}`] : ['mcp-stdio'],
    command: await externalCoralPath(),
    env: desktopCoralEnv(),
  }
}

export async function getMcpLaunchConfig(): Promise<McpLaunchConfig> {
  return mcpLaunchConfig()
}

function requireManageableCoralEntry(client: AgentServers): void {
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
