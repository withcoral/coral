import { beforeEach, describe, expect, it, vi } from 'vitest'

const USER_DATA = '/home/coral/.config/Coral'
const CONFIG_DIR = `${USER_DATA}/coral`
const CORAL_BIN = '/Applications/Coral.app/Contents/Resources/coral/coral'

const mocks = vi.hoisted(() => ({
  listInstalledServers: vi.fn(),
  upsertServer: vi.fn(),
}))

vi.mock('electron', () => ({ app: { isPackaged: true, getPath: () => USER_DATA } }))
vi.mock('./coral-config', () => ({
  desktopCoralConfigDir: (userDataDir: string, directory = 'coral') => `${userDataDir}/${directory}`,
  ensureDesktopCoralConfig: vi.fn(),
}))
vi.mock('./sidecar', async () => {
  const actual = await vi.importActual<typeof import('./sidecar')>('./sidecar')
  return { ...actual, externalCoralPath: async () => CORAL_BIN }
})
vi.mock('add-mcp', async () => {
  const actual = await vi.importActual<typeof import('add-mcp')>('add-mcp')
  return {
    ...actual,
    listInstalledServers: mocks.listInstalledServers,
    upsertServer: mocks.upsertServer,
  }
})

import { agents } from 'add-mcp'

import { configureMcpClient, getMcpLaunchConfig, mcpClients } from './mcp-config'

/** One installed client whose global `coral` entry holds `config`. */
function installed(config: unknown) {
  mocks.listInstalledServers.mockResolvedValue([
    {
      agentType: 'claude-code',
      displayName: 'Claude Code',
      detected: true,
      servers: [{ agentType: 'claude-code', serverName: 'coral', config }],
    },
  ])
}

function claudeCodeConfig(env?: Record<string, string>) {
  return agents['claude-code'].transformConfig(
    'coral',
    { command: CORAL_BIN, args: ['mcp-stdio', '--workspace=default'], ...(env ? { env } : {}) },
    { local: false },
  )
}

describe('desktop MCP client configuration', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mocks.upsertServer.mockReturnValue({ success: true })
  })

  it('points a configured client at the state directory Desktop runs against', async () => {
    mocks.listInstalledServers.mockResolvedValue([
      { agentType: 'claude-code', displayName: 'Claude Code', detected: true, servers: [] },
    ])

    await configureMcpClient('claude-code', 'default')

    expect(mocks.upsertServer).toHaveBeenCalledWith(
      'claude-code',
      'coral',
      {
        args: ['mcp-stdio', '--workspace=default'],
        command: CORAL_BIN,
        env: { CORAL_CONFIG_DIR: CONFIG_DIR },
      },
      { local: false },
    )
  })

  it('reports the workspace of an entry it wrote', async () => {
    installed(claudeCodeConfig({ CORAL_CONFIG_DIR: CONFIG_DIR }))

    await expect(mcpClients()).resolves.toEqual([
      { configuredWorkspace: 'default', id: 'claude-code', name: 'Claude Code' },
    ])
  })

  // An entry written before Desktop pointed clients at its own state must stay
  // manageable, or the app refuses to touch the entry it wrote itself.
  it('still manages an entry written without the state directory', async () => {
    installed(claudeCodeConfig())

    await expect(mcpClients()).resolves.toEqual([
      { configuredWorkspace: 'default', id: 'claude-code', name: 'Claude Code' },
    ])

    await configureMcpClient('claude-code', 'default')
    expect(mocks.upsertServer).toHaveBeenCalledWith(
      'claude-code',
      'coral',
      expect.objectContaining({ env: { CORAL_CONFIG_DIR: CONFIG_DIR } }),
      { local: false },
    )
  })

  it('leaves an entry pointed at another state directory alone', async () => {
    installed(claudeCodeConfig({ CORAL_CONFIG_DIR: '/somewhere/else' }))

    await expect(mcpClients()).resolves.toEqual([{ id: 'claude-code', name: 'Claude Code' }])
    await expect(configureMcpClient('claude-code', 'default')).rejects.toThrow(
      'already has an incompatible global MCP server named "coral"',
    )
  })

  it('carries the state directory in the launch config it hands out', async () => {
    await expect(getMcpLaunchConfig()).resolves.toEqual({
      args: ['mcp-stdio'],
      command: CORAL_BIN,
      env: { CORAL_CONFIG_DIR: CONFIG_DIR },
    })
  })
})
