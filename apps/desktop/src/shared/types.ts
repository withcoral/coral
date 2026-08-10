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
  // Quits Coral and installs the staged update; resolves only if nothing is
  // staged to install.
  installUpdate(): Promise<void>
  listMcpClients(): Promise<McpClientDescriptor[]>
  onUpdateStateChange(listener: DesktopUpdateStateListener): () => void
  removeMcp(clientId: McpClientId): Promise<void>
}
