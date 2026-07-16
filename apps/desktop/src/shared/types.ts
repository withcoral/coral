export type McpClientId = 'claude-code' | 'claude-desktop' | 'codex' | 'cursor' | 'vscode'

export interface SidecarInfo {
  /**
   * Base URL the renderer should point gRPC-web clients at. In packaged builds
   * this is the same-origin proxy path (coral-app://app/__coral__); in dev it is
   * the sidecar's loopback HTTP origin. It is NOT necessarily the sidecar's real
   * network endpoint — use it only as a client baseUrl.
   */
  grpcBaseUrl: string
  packaged: boolean
}

export interface McpClientDescriptor {
  id: McpClientId
  name: string
  configPath: string
  /** Whether the client's desktop app supports a "test connection" deep link. */
  testable: boolean
}

export interface McpConfigureResult {
  client: McpClientDescriptor
  configPath: string
}

export interface CoralDesktopApi {
  awaitInitialization(): Promise<SidecarInfo>
  listMcpClients(): Promise<McpClientDescriptor[]>
  configureMcp(clientId: McpClientId): Promise<McpConfigureResult>
  /** Shell command that installs the Coral MCP server into any add-mcp agent. */
  mcpAddCommand(): Promise<string>
  /** Open the client's desktop app with a prefilled prompt exercising Coral MCP. */
  testMcpClient(clientId: McpClientId): Promise<void>
}
