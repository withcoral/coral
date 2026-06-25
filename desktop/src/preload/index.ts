import { contextBridge, ipcRenderer } from 'electron'
import type { CoralDesktopApi, McpClientId } from '../shared/types'

const api: CoralDesktopApi = {
  awaitInitialization: () => ipcRenderer.invoke('coral:await-initialization'),
  installCli: () => ipcRenderer.invoke('coral:install-cli'),
  listMcpClients: () => ipcRenderer.invoke('coral:list-mcp-clients'),
  configureMcp: (clientId: McpClientId) => ipcRenderer.invoke('coral:configure-mcp', clientId),
  testMcp: (clientId: McpClientId) => ipcRenderer.invoke('coral:test-mcp', clientId),
  onShowOnboarding: (callback: () => void) => {
    const listener = () => callback()
    ipcRenderer.on('coral:show-onboarding', listener)
    return () => ipcRenderer.removeListener('coral:show-onboarding', listener)
  },
}

contextBridge.exposeInMainWorld('coralDesktop', api)
