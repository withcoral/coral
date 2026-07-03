export type McpClientId = 'codex' | 'claude-code'

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
}

export interface McpConfigureResult {
  client: McpClientDescriptor
  configPath: string
}

export interface CoralDesktopApi {
  awaitInitialization(): Promise<SidecarInfo>
  listMcpClients(): Promise<McpClientDescriptor[]>
  configureMcp(clientId: McpClientId): Promise<McpConfigureResult>
}
