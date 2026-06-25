import { mkdir, readFile, writeFile } from 'node:fs/promises'
import { dirname, join } from 'node:path'
import { app } from 'electron'
import type { McpClientDescriptor, McpClientId, McpConfigureResult } from '../shared/types'
import { externalCoralPath } from './sidecar'

type JsonObject = Record<string, unknown>

function homePath(...parts: string[]): string {
  return join(app.getPath('home'), ...parts)
}

function appDataPath(...parts: string[]): string {
  if (process.platform === 'darwin') return homePath('Library', 'Application Support', ...parts)
  if (process.platform === 'win32') return join(process.env.APPDATA ?? app.getPath('appData'), ...parts)
  return homePath('.config', ...parts)
}

export function mcpClients(): McpClientDescriptor[] {
  return [
    {
      id: 'codex',
      name: 'Codex',
      configPath: homePath('.codex', 'config.toml'),
    },
    {
      id: 'claude-desktop',
      name: 'Claude Desktop',
      configPath: appDataPath('Claude', 'claude_desktop_config.json'),
    },
    {
      id: 'cursor',
      name: 'Cursor',
      configPath: homePath('.cursor', 'mcp.json'),
    },
    {
      id: 'vscode',
      name: 'VS Code',
      configPath: appDataPath('Code', 'User', 'mcp.json'),
    },
    {
      id: 'opencode',
      name: 'OpenCode',
      configPath: homePath('.config', 'opencode', 'opencode.json'),
    },
  ]
}

function findClient(clientId: McpClientId): McpClientDescriptor {
  const client = mcpClients().find((candidate) => candidate.id === clientId)
  if (!client) throw new Error(`Unknown MCP client: ${clientId}`)
  return client
}

async function readText(path: string): Promise<string> {
  try {
    return await readFile(path, 'utf8')
  } catch {
    return ''
  }
}

async function writeText(path: string, text: string): Promise<void> {
  await mkdir(dirname(path), { recursive: true })
  await writeFile(path, text)
}

async function readJson(path: string): Promise<JsonObject> {
  const raw = await readText(path)
  if (!raw.trim()) return {}
  const parsed = JSON.parse(raw) as unknown
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new Error(`${path} does not contain a JSON object`)
  }
  return parsed as JsonObject
}

async function writeJson(path: string, value: JsonObject): Promise<void> {
  await writeText(path, `${JSON.stringify(value, null, 2)}\n`)
}

function asObject(value: unknown): JsonObject {
  return value && typeof value === 'object' && !Array.isArray(value) ? (value as JsonObject) : {}
}

function tomlString(value: string): string {
  return JSON.stringify(value)
}

function replaceTomlTable(raw: string, header: string, body: string): string {
  const lines = raw.split(/\r?\n/)
  const start = lines.findIndex((line) => line.trim() === header)
  if (start === -1) {
    const prefix = raw.trimEnd()
    return `${prefix}${prefix ? '\n\n' : ''}${body}\n`
  }

  let end = start + 1
  while (end < lines.length && !lines[end]?.trimStart().startsWith('[')) end += 1
  const next = [...lines.slice(0, start), body.trimEnd(), ...lines.slice(end)]
  return `${next.join('\n').trimEnd()}\n`
}

async function configureCodex(path: string, coralPath: string): Promise<void> {
  const body = [
    '[mcp_servers.coral]',
    `command = ${tomlString(coralPath)}`,
    'args = ["mcp-stdio"]',
  ].join('\n')
  await writeText(path, replaceTomlTable(await readText(path), '[mcp_servers.coral]', body))
}

async function configureClaudeOrCursor(path: string, coralPath: string, includeType: boolean): Promise<void> {
  const config = await readJson(path)
  const mcpServers = asObject(config.mcpServers)
  mcpServers.coral = {
    ...(includeType ? { type: 'stdio' } : {}),
    command: coralPath,
    args: ['mcp-stdio'],
  }
  config.mcpServers = mcpServers
  await writeJson(path, config)
}

async function configureVsCode(path: string, coralPath: string): Promise<void> {
  const config = await readJson(path)
  const servers = asObject(config.servers)
  servers.coral = {
    type: 'stdio',
    command: coralPath,
    args: ['mcp-stdio'],
  }
  config.servers = servers
  await writeJson(path, config)
}

async function configureOpenCode(path: string, coralPath: string): Promise<void> {
  const config = await readJson(path)
  const mcp = asObject(config.mcp)
  mcp.coral = {
    type: 'local',
    command: [coralPath, 'mcp-stdio'],
    enabled: true,
  }
  config.$schema = typeof config.$schema === 'string' ? config.$schema : 'https://opencode.ai/config.json'
  config.mcp = mcp
  await writeJson(path, config)
}

export async function configureMcpClient(clientId: McpClientId): Promise<McpConfigureResult> {
  const client = findClient(clientId)
  const coralPath = await externalCoralPath()

  switch (client.id) {
    case 'codex':
      await configureCodex(client.configPath, coralPath)
      break
    case 'claude-desktop':
      await configureClaudeOrCursor(client.configPath, coralPath, false)
      break
    case 'cursor':
      await configureClaudeOrCursor(client.configPath, coralPath, true)
      break
    case 'vscode':
      await configureVsCode(client.configPath, coralPath)
      break
    case 'opencode':
      await configureOpenCode(client.configPath, coralPath)
      break
  }

  return { client, configPath: client.configPath }
}
