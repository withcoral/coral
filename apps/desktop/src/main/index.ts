import { app, BrowserWindow, Menu, dialog, ipcMain, nativeTheme, shell } from 'electron'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import type { McpClientId } from '../shared/types'
import { configureMcpClient, getMcpLaunchConfig, mcpClients } from './mcp-config'
import {
  APP_ENTRY_URL,
  APP_ORIGIN,
  registerAppProtocol,
  registerAppSchemePrivileges,
} from './app-renderer'
import { killAllTrackedChildren, startCoralSidecar, type CoralSidecar } from './sidecar'
import { checkForDesktopUpdates, desktopUpdatesSupported, installAutoUpdater } from './auto-update'

const SHUTDOWN_TIMEOUT_MS = 6000

let mainWindow: BrowserWindow | null = null
let sidecar: CoralSidecar | null = null
let sidecarPromise: Promise<CoralSidecar> | null = null
let quitting = false
let stopping = false

function currentDir(): string {
  return dirname(fileURLToPath(import.meta.url))
}

function rendererUrl(): string | null {
  // Only honor the dev override in unpackaged builds so a stray env var cannot
  // repoint the UI of a shipped app.
  if (app.isPackaged) return null
  return process.env.ELECTRON_RENDERER_URL ?? null
}

async function rendererEntryUrl(): Promise<string> {
  const devRendererUrl = rendererUrl()
  if (!devRendererUrl) return APP_ENTRY_URL

  // Dev uses the Vite server (HMR), whose React Router loaders call the Electron
  // sidecar through CORAL_ENDPOINT. Wait for the sidecar before the first
  // document request so initial SSR does not race a still-building CLI.
  await ensureSidecar()
  return devRendererUrl
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
  let trustedRendererOrigin: string | null = null
  let trustedErrorUrl: string | null = null

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
      sandbox: true,
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

  window.webContents.on('will-navigate', (event, url) => {
    if (isTrustedNavigation(url, trustedRendererOrigin, trustedErrorUrl)) return
    event.preventDefault()
    if (url.startsWith('http://') || url.startsWith('https://')) {
      void shell.openExternal(url)
    }
  })

  window.webContents.on('will-redirect', (event, url) => {
    if (isTrustedNavigation(url, trustedRendererOrigin, trustedErrorUrl)) return
    event.preventDefault()
  })

  void rendererEntryUrl()
    .then((url) => {
      trustedRendererOrigin = urlOrigin(url)
      trustedErrorUrl = null
      return window.loadURL(url)
    })
    .catch((error: unknown) => {
      const message = error instanceof Error ? error.message : String(error)
      const escapedMessage = escapeHtml(message)
      const errorUrl = `data:text/html;charset=utf-8,${encodeURIComponent(`<main style="font-family: system-ui; padding: 24px;"><h1>Coral failed to start</h1><p>${escapedMessage}</p></main>`)}`
      trustedRendererOrigin = null
      trustedErrorUrl = errorUrl
      console.error(`[coral-renderer] failed to start app renderer: ${message}`)
      void window.loadURL(errorUrl)
    })

  window.on('closed', () => {
    if (mainWindow === window) mainWindow = null
  })

  return window
}

function urlOrigin(value: string): string | null {
  // Node parses a non-special scheme's origin as the opaque "null", which would
  // make app:// indistinguishable from data:/blob:. Map our scheme to a real
  // origin, and treat every opaque origin as untrusted.
  if (value === APP_ORIGIN || value.startsWith(`${APP_ORIGIN}/`)) return APP_ORIGIN
  try {
    const { origin } = new URL(value)
    return origin === 'null' ? null : origin
  } catch {
    return null
  }
}

function isTrustedNavigation(
  targetUrl: string,
  trustedRendererOrigin: string | null,
  trustedErrorUrl: string | null,
): boolean {
  if (trustedErrorUrl && targetUrl === trustedErrorUrl) return true
  if (!trustedRendererOrigin) return false
  return urlOrigin(targetUrl) === trustedRendererOrigin
}

function ensureSidecar(): Promise<CoralSidecar> {
  // Don't spawn a fresh child once teardown has begun — it would outlive quit.
  if (stopping || quitting) {
    return Promise.reject(new Error('Coral is shutting down.'))
  }
  if (sidecarPromise) return sidecarPromise
  const promise = startCoralSidecar().then((started) => {
    sidecar = started
    // If the sidecar dies after startup, clear the cached state so the next
    // request respawns it instead of handing back a dead process.
    started.child.once('exit', (code, signal) => {
      if (sidecar === started) sidecar = null
      if (sidecarPromise === promise) sidecarPromise = null
      if (!stopping && !quitting) {
        console.error(`[coral-sidecar] exited unexpectedly (code=${code}, signal=${signal})`)
      }
    })
    return started
  })
  // Drop the cached promise on failure so a later attempt can retry.
  promise.catch(() => {
    if (sidecarPromise === promise) sidecarPromise = null
  })
  sidecarPromise = promise
  return promise
}

function delay(ms: number): Promise<void> {
  return new Promise((resolveDelay) => setTimeout(resolveDelay, ms))
}

async function stopServices(): Promise<void> {
  const activeSidecar = sidecar
  const pendingSidecar = sidecar ? null : sidecarPromise

  sidecar = null
  sidecarPromise = null

  const teardown = Promise.allSettled([
    activeSidecar?.stop(),
    pendingSidecar?.then((started) => started.stop()).catch(() => undefined),
  ])

  // Never let a stuck child — or a sidecar still mid-startup, which has no
  // timeout in dev — block quit forever.
  await Promise.race([teardown, delay(SHUTDOWN_TIMEOUT_MS)])

  // Backstop: force-kill anything still alive, including a child spawned but not
  // yet exposed via its (still-pending) start promise. No-op once graceful stop
  // has already reaped them.
  killAllTrackedChildren()
}

function registerIpcHandlers() {
  ipcMain.handle('coral:list-mcp-clients', () => mcpClients())
  ipcMain.handle('coral:configure-mcp', (_event, clientId: McpClientId) =>
    configureMcpClient(clientId),
  )
  ipcMain.handle('coral:get-mcp-launch-config', () => getMcpLaunchConfig())
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
        await dialog.showMessageBox({
          type: 'error',
          message: 'MCP configuration failed',
          detail: error instanceof Error ? error.message : String(error),
        })
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
        ...(desktopUpdatesSupported()
          ? ([
              {
                label: 'Check for Updates...',
                click: () => {
                  void checkForDesktopUpdates({ interactive: true })
                },
              },
            ] satisfies Electron.MenuItemConstructorOptions[])
          : []),
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
        // Reload/DevTools are dev-only affordances; keep them out of shipped builds.
        ...(app.isPackaged
          ? []
          : ([
              { role: 'reload' },
              { role: 'toggleDevTools' },
              { type: 'separator' },
            ] satisfies Electron.MenuItemConstructorOptions[])),
        { role: 'resetZoom' },
        { role: 'zoomIn' },
        { role: 'zoomOut' },
      ],
    },
  ]
  Menu.setApplicationMenu(Menu.buildFromTemplate(template))
}

// Must run before the app `ready` event.
registerAppSchemePrivileges()

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
  installAutoUpdater()
  registerAppProtocol(() => ensureSidecar().then((started) => started.url))
  void ensureSidecar().catch((error: unknown) => {
    const message = error instanceof Error ? error.message : String(error)
    console.error(`[coral-sidecar] failed to start during boot: ${message}`)
  })
  mainWindow = createMainWindow()
})

app.on('activate', () => {
  if (BrowserWindow.getAllWindows().length === 0) mainWindow = createMainWindow()
})

app.on('before-quit', (event) => {
  if (quitting) return
  // Teardown already in flight (e.g. a second Cmd-Q): block the quit until it
  // finishes so the spawned sidecar child is never orphaned.
  if (stopping) {
    event.preventDefault()
    return
  }
  if (!sidecar && !sidecarPromise) return

  stopping = true
  event.preventDefault()
  void stopServices().finally(() => {
    quitting = true
    app.quit()
  })
})
