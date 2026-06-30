import { app, BrowserWindow, Menu, dialog, ipcMain, shell } from 'electron'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import type { McpClientId, SidecarInfo } from '../shared/types'
import { configureMcpClient, mcpClients } from './mcp-config'
import { startReefRendererServer, type ReefRendererServer } from './reef-renderer'
import { startCoralSidecar, type CoralSidecar } from './sidecar'

let mainWindow: BrowserWindow | null = null
let sidecar: CoralSidecar | null = null
let sidecarPromise: Promise<CoralSidecar> | null = null
let reefRenderer: ReefRendererServer | null = null
let reefRendererPromise: Promise<ReefRendererServer> | null = null
let quitting = false

function currentDir(): string {
  return dirname(fileURLToPath(import.meta.url))
}

function rendererUrl(): string | null {
  return process.env.ELECTRON_RENDERER_URL ?? null
}

function ensureReefRenderer(): Promise<ReefRendererServer> {
  if (!reefRendererPromise) {
    reefRendererPromise = startReefRendererServer().then((started) => {
      reefRenderer = started
      return started
    })
  }
  return reefRendererPromise
}

async function rendererEntryUrl(): Promise<string> {
  const devUrl = rendererUrl()
  if (devUrl) return devUrl
  return (await ensureReefRenderer()).url
}

function escapeHtml(value: string): string {
  return value.replace(/[&<>"']/g, (char) => {
    switch (char) {
      case '&':
        return '&amp;'
      case '<':
        return '&lt;'
      case '>':
        return '&gt;'
      case '"':
        return '&quot;'
      default:
        return '&#39;'
    }
  })
}

function desktopIconPath(): string {
  return app.isPackaged
    ? join(process.resourcesPath, 'icons', 'icon.png')
    : join(currentDir(), '..', '..', 'resources', 'icons', 'icon.png')
}

function createMainWindow(): BrowserWindow {
  const preloadPath = join(currentDir(), '..', 'preload', 'index.cjs')

  const window = new BrowserWindow({
    width: 1280,
    height: 860,
    minWidth: 720,
    minHeight: 520,
    title: 'Coral',
    icon: desktopIconPath(),
    autoHideMenuBar: process.platform !== 'darwin',
    webPreferences: {
      preload: preloadPath,
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: false,
    },
  })

  window.webContents.on('did-fail-load', (_event, code, description, url) => {
    console.error(`[coral-renderer] failed to load ${url}: ${code} ${description}`)
  })

  window.webContents.setWindowOpenHandler(({ url }) => {
    if (url.startsWith('http://') || url.startsWith('https://')) {
      void shell.openExternal(url)
    }
    return { action: 'deny' }
  })

  void rendererEntryUrl()
    .then((url) => window.loadURL(url))
    .catch((error: unknown) => {
      const message = error instanceof Error ? error.message : String(error)
      const escapedMessage = escapeHtml(message)
      console.error(`[coral-renderer] failed to start Reef renderer: ${message}`)
      void window.loadURL(
        `data:text/html;charset=utf-8,${encodeURIComponent(`<main style="font-family: system-ui; padding: 24px;"><h1>Coral failed to start</h1><p>${escapedMessage}</p></main>`)}`,
      )
    })

  window.on('closed', () => {
    if (mainWindow === window) mainWindow = null
  })

  return window
}

function ensureSidecar(): Promise<CoralSidecar> {
  if (!sidecarPromise) {
    sidecarPromise = startCoralSidecar().then((started) => {
      sidecar = started
      return started
    })
  }
  return sidecarPromise
}

async function stopServices(): Promise<void> {
  const activeSidecar = sidecar
  const pendingSidecar = sidecar ? null : sidecarPromise
  const activeRenderer = reefRenderer
  const pendingRenderer = reefRenderer ? null : reefRendererPromise

  sidecar = null
  sidecarPromise = null
  reefRenderer = null
  reefRendererPromise = null

  await Promise.allSettled([
    activeSidecar?.stop(),
    pendingSidecar?.then((started) => started.stop()).catch(() => undefined),
    activeRenderer?.stop(),
    pendingRenderer?.then((started) => started.stop()).catch(() => undefined),
  ])
}

function registerIpcHandlers() {
  ipcMain.handle('coral:await-initialization', async (): Promise<SidecarInfo> => {
    const started = await ensureSidecar()
    return { url: started.url, packaged: started.packaged }
  })
  ipcMain.handle('coral:list-mcp-clients', () => mcpClients())
  ipcMain.handle('coral:configure-mcp', (_event, clientId: McpClientId) => configureMcpClient(clientId))
}

function installMenu() {
  const mcpSubmenu: Electron.MenuItemConstructorOptions[] = mcpClients().map((client) => ({
    label: client.name,
    click: async () => {
      try {
        const result = await configureMcpClient(client.id)
        await dialog.showMessageBox({
          type: 'info',
          message: `${result.client.name} MCP configured`,
          detail: result.configPath,
        })
      } catch (error) {
        await dialog.showErrorBox('MCP configuration failed', error instanceof Error ? error.message : String(error))
      }
    },
  }))

  const template: Electron.MenuItemConstructorOptions[] = [
    {
      label: 'Coral',
      submenu: [
        {
          label: 'Configure MCP',
          submenu: mcpSubmenu,
        },
        { type: 'separator' },
        { role: 'quit' },
      ],
    },
    {
      label: 'Edit',
      submenu: [
        { role: 'undo' },
        { role: 'redo' },
        { type: 'separator' },
        { role: 'cut' },
        { role: 'copy' },
        { role: 'paste' },
      ],
    },
    {
      label: 'View',
      submenu: [
        { role: 'reload' },
        { role: 'toggleDevTools' },
        { type: 'separator' },
        { role: 'resetZoom' },
        { role: 'zoomIn' },
        { role: 'zoomOut' },
      ],
    },
  ]
  Menu.setApplicationMenu(Menu.buildFromTemplate(template))
}

const gotLock = app.requestSingleInstanceLock()
if (!gotLock) {
  app.quit()
} else {
  app.on('second-instance', () => {
    if (!mainWindow) return
    if (mainWindow.isMinimized()) mainWindow.restore()
    mainWindow.focus()
  })
}

app.whenReady().then(() => {
  if (process.platform === 'darwin' && app.dock) app.dock.setIcon(desktopIconPath())
  registerIpcHandlers()
  installMenu()
  sidecarPromise = ensureSidecar()
  mainWindow = createMainWindow()
})

app.on('activate', () => {
  if (BrowserWindow.getAllWindows().length === 0) mainWindow = createMainWindow()
})

app.on('before-quit', (event) => {
  if (quitting || (!sidecar && !sidecarPromise && !reefRenderer && !reefRendererPromise)) return
  event.preventDefault()
  void stopServices().finally(() => {
    quitting = true
    app.quit()
  })
})
