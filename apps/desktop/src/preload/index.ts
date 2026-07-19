import { contextBridge, ipcRenderer } from 'electron'
import type { CoralDesktopApi, McpClientId } from '../shared/types'

const api: CoralDesktopApi = {
  listMcpClients: () => ipcRenderer.invoke('coral:list-mcp-clients'),
  configureMcp: (clientId: McpClientId) => ipcRenderer.invoke('coral:configure-mcp', clientId),
}

contextBridge.exposeInMainWorld('coralDesktop', api)
