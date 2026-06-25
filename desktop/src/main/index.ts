import { app, BrowserWindow, Menu, dialog, ipcMain, shell } from 'electron'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import type { McpClientId, SidecarInfo } from '../shared/types'
import { installCliAlias } from './cli-alias'
import { configureMcpClient, mcpClients } from './mcp-config'
import { startCoralSidecar, type CoralSidecar } from './sidecar'

let mainWindow: BrowserWindow | null = null
let sidecar: CoralSidecar | null = null
let sidecarPromise: Promise<CoralSidecar> | null = null

function currentDir(): string {
  return dirname(fileURLToPath(import.meta.url))
}

function rendererUrl(): string | null {
  return process.env.ELECTRON_RENDERER_URL ?? null
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

  const devUrl = rendererUrl()
  if (devUrl) {
    void window.loadURL(devUrl)
  } else {
    void window.loadFile(join(currentDir(), '..', 'renderer', 'index.html'))
  }

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

function registerIpcHandlers() {
  ipcMain.handle('coral:await-initialization', async (): Promise<SidecarInfo> => {
    const started = await ensureSidecar()
    return { url: started.url, packaged: started.packaged }
  })
  ipcMain.handle('coral:install-cli', () => installCliAlias())
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
          label: 'Show Onboarding',
          click: () => mainWindow?.webContents.send('coral:show-onboarding'),
        },
        { type: 'separator' },
        {
          label: 'Install CLI Command',
          click: async () => {
            try {
              const result = await installCliAlias()
              await dialog.showMessageBox({
                type: 'info',
                message: result.installKind === 'alias' ? 'Coral CLI alias installed' : 'Coral CLI command installed',
                detail:
                  result.installKind === 'alias'
                    ? `${result.commandPath} -> ${result.targetPath}\n\nUpdated ${result.shellConfigPath}. Open a new terminal or source the file.`
                    : result.commandPath,
              })
            } catch (error) {
              await dialog.showErrorBox('CLI install failed', error instanceof Error ? error.message : String(error))
            }
          },
        },
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
  if (!sidecar) return
  event.preventDefault()
  const active = sidecar
  sidecar = null
  void active.stop().finally(() => app.quit())
})
