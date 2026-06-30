import { app, BrowserWindow, Menu, dialog, ipcMain, nativeTheme, shell } from 'electron'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import type { McpClientId, SidecarInfo } from '../shared/types'
import { configureMcpClient, mcpClients } from './mcp-config'
import { startAppRendererServer, type AppRendererServer } from './app-renderer'
import { startCoralSidecar, type CoralSidecar } from './sidecar'

let mainWindow: BrowserWindow | null = null
let sidecar: CoralSidecar | null = null
let sidecarPromise: Promise<CoralSidecar> | null = null
let appRenderer: AppRendererServer | null = null
let appRendererPromise: Promise<AppRendererServer> | null = null
let quitting = false

function currentDir(): string {
  return dirname(fileURLToPath(import.meta.url))
}

function rendererUrl(): string | null {
  return process.env.ELECTRON_RENDERER_URL ?? null
}

function ensureAppRenderer(): Promise<AppRendererServer> {
  if (!appRendererPromise) {
    appRendererPromise = startAppRendererServer().then((started) => {
      appRenderer = started
      return started
    })
  }
  return appRendererPromise
}

async function rendererEntryUrl(): Promise<string> {
  const devUrl = rendererUrl()
  if (devUrl) return devUrl
  return (await ensureAppRenderer()).url
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

type IconAppearance = 'light' | 'dark'
type IconFormat = 'icns' | 'ico' | 'mac-png' | 'png'

function desktopIconPath(appearance: IconAppearance = 'light', format: IconFormat = 'png'): string {
  const variant = appearance === 'dark' ? '-dark' : ''
  const iconName = format === 'mac-png' ? `icon${variant}-mac.png` : `icon${variant}.${format}`
  return app.isPackaged
    ? join(process.resourcesPath, 'icons', iconName)
    : join(currentDir(), '..', '..', 'resources', 'icons', iconName)
}

function currentIconAppearance(): IconAppearance {
  return nativeTheme.shouldUseDarkColors ? 'dark' : 'light'
}

function currentWindowIconPath(): string {
  if (process.platform === 'darwin') return desktopIconPath(currentIconAppearance(), 'mac-png')
  if (process.platform === 'win32') return desktopIconPath(currentIconAppearance(), 'ico')
  return desktopIconPath(currentIconAppearance(), 'png')
}

function currentDockIconPath(): string {
  return desktopIconPath(currentIconAppearance(), 'mac-png')
}

function updatePlatformIcon() {
  if (process.platform === 'darwin' && app.dock) {
    app.dock.setIcon(currentDockIconPath())
  } else {
    mainWindow?.setIcon(currentWindowIconPath())
  }
}

function createMainWindow(): BrowserWindow {
  const preloadPath = join(currentDir(), '..', 'preload', 'index.cjs')

  const window = new BrowserWindow({
    width: 1280,
    height: 860,
    minWidth: 720,
    minHeight: 520,
    title: 'Coral',
    icon: currentWindowIconPath(),
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
      console.error(`[coral-renderer] failed to start app renderer: ${message}`)
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
  const activeRenderer = appRenderer
  const pendingRenderer = appRenderer ? null : appRendererPromise

  sidecar = null
  sidecarPromise = null
  appRenderer = null
  appRendererPromise = null

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
  updatePlatformIcon()
  nativeTheme.on('updated', updatePlatformIcon)
  registerIpcHandlers()
  installMenu()
  sidecarPromise = ensureSidecar()
  mainWindow = createMainWindow()
})

app.on('activate', () => {
  if (BrowserWindow.getAllWindows().length === 0) mainWindow = createMainWindow()
})

app.on('before-quit', (event) => {
  if (quitting || (!sidecar && !sidecarPromise && !appRenderer && !appRendererPromise)) return
  event.preventDefault()
  void stopServices().finally(() => {
    quitting = true
    app.quit()
  })
})
