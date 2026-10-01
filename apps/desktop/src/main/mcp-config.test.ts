import { beforeEach, describe, expect, it, vi } from 'vitest'

const mocks = vi.hoisted(() => ({
  getPath: vi.fn(),
  ensureDesktopCoralConfig: vi.fn(),
  externalCoralPath: vi.fn(),
  getAgentTypes: vi.fn(),
  listInstalledServers: vi.fn(),
  removeServer: vi.fn(),
  upsertServer: vi.fn(),
}))

vi.mock('electron', () => ({
  app: {
    isPackaged: true,
    getPath: mocks.getPath,
  },
}))

vi.mock('add-mcp', async (importOriginal) => {
  const actual = await importOriginal<typeof import('add-mcp')>()
  return {
    ...actual,
    getAgentTypes: mocks.getAgentTypes,
    listInstalledServers: mocks.listInstalledServers,
    removeServer: mocks.removeServer,
    upsertServer: mocks.upsertServer,
  }
})

vi.mock('./coral-config', () => ({
  desktopRuntimeCoralConfigOptions: vi.fn(() => ({})),
  ensureDesktopCoralConfig: mocks.ensureDesktopCoralConfig,
}))

vi.mock('./sidecar', () => ({
  externalCoralPath: mocks.externalCoralPath,
}))

const { configureMcpClient, mcpClients } = await import('./mcp-config')
const { agents } = await import('add-mcp')

describe('desktop MCP client configuration', () => {
  const userData = '/Users/simon/Library/Application Support/@withcoral/desktop'
  const configDir = `${userData}/coral`
  const coralCommand = '/Applications/Coral.app/Contents/Resources/coral/coral'

  beforeEach(() => {
    vi.clearAllMocks()
    mocks.getPath.mockReturnValue(userData)
    mocks.ensureDesktopCoralConfig.mockResolvedValue(configDir)
    mocks.externalCoralPath.mockResolvedValue(coralCommand)
    mocks.getAgentTypes.mockReturnValue(['claude-code'])
    mocks.listInstalledServers.mockResolvedValue([
      {
        agentType: 'claude-code',
        detected: true,
        displayName: 'Claude Code',
        servers: [],
      },
    ])
    mocks.upsertServer.mockReturnValue({ success: true })
  })

  it('installs Coral stdio with Desktop’s config directory in the agent environment', async () => {
    await configureMcpClient('claude-code', 'default')

    expect(mocks.upsertServer).toHaveBeenCalledWith(
      'claude-code',
      'coral',
      {
        args: ['mcp-stdio', '--workspace=default'],
        command: coralCommand,
        env: { CORAL_CONFIG_DIR: configDir },
      },
      { local: false },
    )
  })

  it('keeps Desktop’s config directory when add-mcp transforms the Claude Code config', () => {
    expect(
      agents['claude-code'].transformConfig(
        'coral',
        {
          args: ['mcp-stdio', '--workspace=default'],
          command: coralCommand,
          env: { CORAL_CONFIG_DIR: configDir },
        },
        { local: false },
      ),
    ).toMatchObject({ env: { CORAL_CONFIG_DIR: configDir } })
  })

  it('recognizes an installed Coral server that includes Desktop’s config directory', async () => {
    mocks.listInstalledServers.mockResolvedValue([
      {
        agentType: 'claude-code',
        detected: true,
        displayName: 'Claude Code',
        servers: [
          {
            agentType: 'claude-code',
            serverName: 'coral',
            config: {
              args: ['mcp-stdio', '--workspace=default'],
              command: coralCommand,
              env: { CORAL_CONFIG_DIR: configDir },
            },
          },
        ],
      },
    ])

    await expect(mcpClients()).resolves.toEqual([
      { configuredWorkspace: 'default', id: 'claude-code', name: 'Claude Code' },
    ])
  })

  it('treats an old Coral server without Desktop’s config directory as updateable', async () => {
    mocks.listInstalledServers.mockResolvedValue([
      {
        agentType: 'claude-code',
        detected: true,
        displayName: 'Claude Code',
        servers: [
          {
            agentType: 'claude-code',
            serverName: 'coral',
            config: {
              args: ['mcp-stdio', '--workspace=default'],
              command: coralCommand,
            },
          },
        ],
      },
    ])

    await expect(mcpClients()).resolves.toEqual([{ id: 'claude-code', name: 'Claude Code' }])
    await expect(configureMcpClient('claude-code', 'default')).resolves.toBeUndefined()
    expect(mocks.upsertServer).toHaveBeenCalledWith(
      'claude-code',
      'coral',
      {
        args: ['mcp-stdio', '--workspace=default'],
        command: coralCommand,
        env: { CORAL_CONFIG_DIR: configDir },
      },
      { local: false },
    )
  })
})
