export type McpClientId = 'codex' | 'claude-code'

export interface McpClientDescriptor {
  id: McpClientId
  name: string
  configPath: string
}

export interface McpConfigureResult {
  client: McpClientDescriptor
  configPath: string
}

export interface McpLaunchConfig {
  args: string[]
  command: string
}

export interface CoralDesktopApi {
  configureMcp(clientId: McpClientId): Promise<McpConfigureResult>
  getMcpLaunchConfig(): Promise<McpLaunchConfig>
  listMcpClients(): Promise<McpClientDescriptor[]>
}
