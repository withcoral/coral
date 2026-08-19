import { app, BrowserWindow, Menu, ipcMain, nativeTheme, shell } from 'electron'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import type { DesktopUpdateState } from '../shared/types'
import {
  configureMcpClient,
  getMcpLaunchConfig,
  mcpClients,
  removeMcpClient,
} from './mcp-config'
import {
  APP_ENTRY_URL,
  APP_ORIGIN,
  registerAppProtocol,
  registerAppSchemePrivileges,
} from './app-renderer'
import { killAllTrackedChildren, startCoralSidecar, type CoralSidecar } from './sidecar'
import {
  checkForDesktopUpdates,
  clearPendingDesktopUpdateIntent,
  desktopUpdatesSupported,
  downloadDesktopUpdate,
  getDesktopUpdateState,
  installAutoUpdater,
  onDesktopUpdateStateChange,
  quitAndInstallDesktopUpdate,
  shouldExitForPendingDesktopUpdate,
} from './auto-update'
import { createShutdownCoordinator } from './shutdown'

const SHUTDOWN_TIMEOUT_MS = 6000

// Baked in by electron.vite.config.ts; the guard covers other build paths.
declare const __CORAL_DESKTOP_COMMIT__: string
const buildCommit = typeof __CORAL_DESKTOP_COMMIT__ === 'undefined' ? '' : __CORAL_DESKTOP_COMMIT__

let mainWindow: BrowserWindow | null = null
let sidecar: CoralSidecar | null = null
let sidecarPromise: Promise<CoralSidecar> | null = null

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
    // A hidden bar means the only route to About and Quit is the Alt key.
    autoHideMenuBar: false,
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
  if (shutdownCoordinator.isShuttingDown()) {
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
      if (!shutdownCoordinator.isShuttingDown()) {
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

const shutdownCoordinator = createShutdownCoordinator({
  stopServices,
  installReadyUpdate: quitAndInstallDesktopUpdate,
  quit: () => app.quit(),
})

function registerIpcHandlers() {
  ipcMain.handle('coral:list-mcp-clients', () => mcpClients())
  ipcMain.handle('coral:configure-mcp', (_event, clientId: unknown, workspaceName: unknown) =>
    configureMcpClient(clientId, workspaceName),
  )
  ipcMain.handle('coral:remove-mcp', (_event, clientId: unknown) => removeMcpClient(clientId))
  ipcMain.handle('coral:get-mcp-launch-config', () => getMcpLaunchConfig())
  ipcMain.handle('coral:get-update-state', () => getDesktopUpdateState())
  ipcMain.handle('coral:download-update', () => downloadDesktopUpdate())
  ipcMain.handle('coral:install-update', () => installReadyUpdateNow())
  onDesktopUpdateStateChange(publishDesktopUpdateState)
}

// Quit rather than calling quitAndInstall() directly: the shutdown coordinator
// stops the sidecar first and only then hands the staged update to Squirrel.
function installReadyUpdateNow(): void {
  if (getDesktopUpdateState().status !== 'ready') return
  app.quit()
}

function publishDesktopUpdateState(state: DesktopUpdateState): void {
  const window = mainWindow
  if (!window) return

  try {
    if (window.isDestroyed() || window.webContents.isDestroyed()) return
    window.webContents.send('coral:update-state-changed', state)
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    console.error(`[coral-updater] failed to publish update state: ${message}`)
  }
}

function installAboutPanel() {
  app.setAboutPanelOptions({
    applicationName: 'Coral',
    applicationVersion: app.getVersion(),
    // The panel always parenthesizes a build number, falling back to
    // CFBundleVersion — which release-please keeps equal to the app version.
    version: buildCommit,
    // macOS draws the bundle icon and ignores iconPath.
    ...(process.platform === 'darwin' ? {} : { iconPath: currentWindowIconPath() }),
  })
}

// `role: 'about'` would label itself from app.name, the package name in dev.
const ABOUT_ITEM: Electron.MenuItemConstructorOptions = {
  label: 'About Coral',
  click: () => app.showAboutPanel(),
}

function updateItems(): Electron.MenuItemConstructorOptions[] {
  if (!desktopUpdatesSupported()) return []
  return [
    {
      label: 'Check for Updates...',
      click: () => {
        void checkForDesktopUpdates({ interactive: true })
      },
    },
  ]
}

// macOS puts About, updates, and Quit in an application menu named after the
// app. Windows and Linux have no such menu: a top-level "Coral" entry next to
// Edit and View is where nobody looks for either item, so those platforms get
// the File/Help pair they do expect.
function leadingMenus(): Electron.MenuItemConstructorOptions[] {
  if (process.platform !== 'darwin') return [{ label: 'File', submenu: [{ role: 'quit' }] }]
  const updates = updateItems()
  return [
    {
      label: 'Coral',
      submenu: [
        ABOUT_ITEM,
        { type: 'separator' },
        ...(updates.length > 0
          ? ([...updates, { type: 'separator' }] satisfies Electron.MenuItemConstructorOptions[])
          : []),
        { role: 'quit' },
      ],
    },
  ]
}

function trailingMenus(): Electron.MenuItemConstructorOptions[] {
  if (process.platform === 'darwin') return []
  return [{ label: 'Help', submenu: [...updateItems(), ABOUT_ITEM] }]
}

function installMenu() {
  const template: Electron.MenuItemConstructorOptions[] = [
    ...leadingMenus(),
    {
      label: 'Edit',
      submenu: [
        { role: 'undo' },
        { role: 'redo' },
        { type: 'separator' },
        { role: 'cut' },
        { role: 'copy' },
        { role: 'paste' },
        { type: 'separator' },
        { role: 'selectAll' },
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
    ...trailingMenus(),
  ]
  Menu.setApplicationMenu(Menu.buildFromTemplate(template))
}

function startApplication(): void {
  // Windows keys toasts, taskbar grouping, and jump lists off this id, and
  // silently drops a notification from a process that never set one. Must run
  // before the app is ready. It is inert on macOS and Linux.
  app.setAppUserModelId('com.withcoral.desktop')

  const gotLock = app.requestSingleInstanceLock()
  if (!gotLock) {
    app.quit()
    return
  }

  // The marker may have appeared while this process waited for the lock. An
  // old binary that won the race must release it without clearing the marker.
  if (shouldExitForPendingDesktopUpdate()) {
    console.info('[coral-updater] update installation is still in progress; exiting')
    app.releaseSingleInstanceLock()
    app.exit(0)
    return
  }

  // Keep the hand-off marker visible until the updated binary owns the lock.
  clearPendingDesktopUpdateIntent()

  // Must run before the app `ready` event.
  registerAppSchemePrivileges()

  app.on('second-instance', () => {
    if (shutdownCoordinator.isShuttingDown()) return
    if (!mainWindow) return
    if (mainWindow.isMinimized()) mainWindow.restore()
    mainWindow.focus()
  })

  app.on('before-quit', shutdownCoordinator.beforeQuit)

  app.whenReady().then(() => {
    if (shutdownCoordinator.isShuttingDown()) return

    updatePlatformIcon()
    nativeTheme.on('updated', updatePlatformIcon)
    registerIpcHandlers()
    installAboutPanel()
    installMenu()
    installAutoUpdater({
      allowUpdateQuit: shutdownCoordinator.allowQuit,
      onInstallFailure: shutdownCoordinator.quitAfterUpdateFailure,
    })
    registerAppProtocol(() => ensureSidecar().then((started) => started.url))
    void ensureSidecar().catch((error: unknown) => {
      const message = error instanceof Error ? error.message : String(error)
      console.error(`[coral-sidecar] failed to start during boot: ${message}`)
    })
    mainWindow = createMainWindow()
  })

  app.on('activate', () => {
    if (shutdownCoordinator.isShuttingDown()) return
    if (BrowserWindow.getAllWindows().length === 0) mainWindow = createMainWindow()
  })
}

// Evaluate the hand-off marker before taking the single-instance lock. A
// rapidly reopened old binary exits independently instead of waking the
// instance that is still handing its update to ShipIt.
if (shouldExitForPendingDesktopUpdate()) {
  console.info('[coral-updater] update installation is still in progress; exiting')
  app.exit(0)
} else {
  startApplication()
}
