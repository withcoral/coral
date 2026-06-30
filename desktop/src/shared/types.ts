export type McpClientId = 'codex' | 'claude-code'

export interface SidecarInfo {
  url: string
  packaged: boolean
}

export interface McpClientDescriptor {
  id: McpClientId
  name: string
  configPath: string
}

export interface McpConfigureResult {
  client: McpClientDescriptor
  configPath: string
}

export interface McpTestResult {
  client: McpClientDescriptor
  launchUrl: string
  message: string
}

export interface CoralDesktopApi {
  awaitInitialization(): Promise<SidecarInfo>
  listMcpClients(): Promise<McpClientDescriptor[]>
  configureMcp(clientId: McpClientId): Promise<McpConfigureResult>
  testMcp(clientId: McpClientId): Promise<McpTestResult>
}
