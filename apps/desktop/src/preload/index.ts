import { contextBridge, ipcRenderer } from 'electron'
import type { CoralDesktopApi, McpClientId } from '../shared/types'

const api: CoralDesktopApi = {
  configureMcp: (clientId: McpClientId) => ipcRenderer.invoke('coral:configure-mcp', clientId),
  getMcpLaunchConfig: () => ipcRenderer.invoke('coral:get-mcp-launch-config'),
  listMcpClients: () => ipcRenderer.invoke('coral:list-mcp-clients'),
}

contextBridge.exposeInMainWorld('coralDesktop', api)
