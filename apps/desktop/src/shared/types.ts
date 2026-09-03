export type McpClientId = string

export interface McpClientDescriptor {
  id: McpClientId
  name: string
  configuredWorkspace?: string
}

export interface McpLaunchConfig {
  args: string[]
  command: string
  /** Always carries CORAL_CONFIG_DIR: the state directory this app runs against. */
  env: Record<string, string>
}

export type DesktopUpdateState =
  | { status: 'unsupported' }
  | { status: 'idle' }
  | { status: 'available'; version: string }
  | { status: 'downloading'; version: string }
  | { status: 'ready'; version: string }

export type DesktopUpdateStateListener = (state: DesktopUpdateState) => void

export interface CoralDesktopApi {
  configureMcp(clientId: McpClientId, workspaceName: string): Promise<void>
  downloadUpdate(): Promise<void>
  getMcpLaunchConfig(): Promise<McpLaunchConfig>
  getUpdateState(): Promise<DesktopUpdateState>
  // Asks Coral to quit and install a staged update. Resolves once the request
  // is accepted, which is before the sidecar stops and Squirrel takes over, so
  // it never reports whether the install itself succeeded.
  installUpdate(): Promise<void>
  listMcpClients(): Promise<McpClientDescriptor[]>
  onUpdateStateChange(listener: DesktopUpdateStateListener): () => void
  removeMcp(clientId: McpClientId): Promise<void>
}
