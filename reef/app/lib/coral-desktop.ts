export type McpClientId = 'codex' | 'claude-code'

export interface SidecarInfo {
  packaged: boolean
  url: string
}

export interface McpClientDescriptor {
  configPath: string
  id: McpClientId
  name: string
}

export interface McpConfigureResult {
  client: McpClientDescriptor
  configPath: string
}

export interface CoralDesktopApi {
  awaitInitialization(): Promise<SidecarInfo>
  configureMcp(clientId: McpClientId): Promise<McpConfigureResult>
  listMcpClients(): Promise<McpClientDescriptor[]>
}

declare global {
  interface Window {
    coralDesktop?: CoralDesktopApi
  }
}

export function coralDesktopApi(): CoralDesktopApi | null {
  if (typeof window === 'undefined') return null
  return window.coralDesktop ?? null
}

export function desktopErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}
