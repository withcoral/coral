import { contextBridge, ipcRenderer } from 'electron'
import type {
  CoralDesktopApi,
  DesktopUpdateState,
  DesktopUpdateStateListener,
  McpClientId,
} from '../shared/types'

const api: CoralDesktopApi = {
  configureMcp: (clientId: McpClientId, workspaceName: string) =>
    ipcRenderer.invoke('coral:configure-mcp', clientId, workspaceName),
  getMcpLaunchConfig: () => ipcRenderer.invoke('coral:get-mcp-launch-config'),
  getUpdateState: () => ipcRenderer.invoke('coral:get-update-state'),
  listMcpClients: () => ipcRenderer.invoke('coral:list-mcp-clients'),
  onUpdateStateChange: (listener: DesktopUpdateStateListener) => {
    const handler = (_event: Electron.IpcRendererEvent, state: DesktopUpdateState) => {
      listener(state)
    }
    ipcRenderer.on('coral:update-state-changed', handler)
    return () => {
      ipcRenderer.removeListener('coral:update-state-changed', handler)
    }
  },
  removeMcp: (clientId: McpClientId) => ipcRenderer.invoke('coral:remove-mcp', clientId),
}

contextBridge.exposeInMainWorld('coralDesktop', api)
