import { contextBridge, ipcRenderer } from 'electron'
import type { CoralDesktopApi, McpClientId } from '../shared/types'

const api: CoralDesktopApi = {
  listMcpClients: () => ipcRenderer.invoke('coral:list-mcp-clients'),
  configureMcp: (clientId: McpClientId) => ipcRenderer.invoke('coral:configure-mcp', clientId),
}

window.addEventListener(
  'DOMContentLoaded',
  () => {
    document.documentElement.dataset.coralDesktopPlatform = process.platform
  },
  { once: true },
)

contextBridge.exposeInMainWorld('coralDesktop', api)
