export type McpClientId = string

export interface McpClientDescriptor {
  id: McpClientId
  name: string
  configuredWorkspace?: string
}

export interface McpLaunchConfig {
  args: string[]
  command: string
}

export interface CoralDesktopApi {
  configureMcp(clientId: McpClientId, workspaceName: string): Promise<void>
  getMcpLaunchConfig(): Promise<McpLaunchConfig>
  listMcpClients(): Promise<McpClientDescriptor[]>
  removeMcp(clientId: McpClientId): Promise<void>
}
