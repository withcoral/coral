import { contextBridge, ipcRenderer } from 'electron'
import type { CoralDesktopApi, McpClientId } from '../shared/types'

const api: CoralDesktopApi = {
  configureMcp: (clientId: McpClientId, workspaceName: string) =>
    ipcRenderer.invoke('coral:configure-mcp', clientId, workspaceName),
  getMcpLaunchConfig: () => ipcRenderer.invoke('coral:get-mcp-launch-config'),
  listMcpClients: () => ipcRenderer.invoke('coral:list-mcp-clients'),
  removeMcp: (clientId: McpClientId) => ipcRenderer.invoke('coral:remove-mcp', clientId),
}

contextBridge.exposeInMainWorld('coralDesktop', api)
