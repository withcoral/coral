import { app, BrowserWindow, dialog, Menu, powerMonitor, shell } from 'electron'
import { isAbsolute, join, resolve } from 'node:path'

import { APP_ID } from '../shared/app-id'
import { autostartLoginItem } from './autostart'
import {
  BACKGROUND_ARG,
  createBackground,
  readToldStillRunning,
  writeToldStillRunning,
  type Background,
  type LoginItem,
  type MenuCheckbox,
} from './background'
import { createDockPresence } from './dock'
import { failureMessage, failurePage, failureTitle, startingPage } from './local-pages'
import { logSink } from './logs'
import { spawnLauncher, type Launcher, type OutputSink } from './process'
import { utilityLauncher } from './reef'
import { runtimeLauncher, withLoopbackProxyBypass, type ReefEnv } from './runtime'
import { createSupervisor, SupervisorError, type Failure, type SupervisorState } from './supervisor'
import { installTray } from './tray'
import { startUpdates, updatesSupported } from './updater'
import type { UpdateMenuItem, Updates } from './updates'
import { createMainWindow, installContentGuards } from './window'

const PACKAGED_READINESS_TIMEOUT_MS = 30_000
// A development launch compiles Reef's protobufs and routes on first request.
const DEV_READINESS_TIMEOUT_MS = 120_000

// Bootstrap: the only place that reads the process environment.
const packaged = app.isPackaged
const baseEnv = withLoopbackProxyBypass(process.env)
const devRuntime = packaged ? undefined : process.env.CORAL_DESKTOP_DEV_RUNTIME
// The AppImage runtime names the image file that an update replaces.
const appImage = process.env.APPIMAGE
// The login item starts Desktop with this argument, and with no window.
const startedInBackground = process.argv.includes(BACKGROUND_ARG)
const updatable = updatesSupported({ packaged, resourcesPath: process.resourcesPath, appImage })

// The data folder follows the app id, never the display name, and a
// development build keeps its own. Set it before anything reads it: the
// single-instance lock lives there too.
app.setPath('userData', join(app.getPath('appData'), packaged ? APP_ID : `${APP_ID}.dev`))
app.setAppUserModelId(APP_ID)

if (!app.requestSingleInstanceLock()) app.quit()
else main()

function main() {
  const dataDir = app.getPath('userData')
  const logsDir = join(dataDir, 'logs')
  const echo = (prefix: string): OutputSink | undefined =>
    packaged ? undefined : (chunk) => process.stdout.write(`[${prefix}] ${chunk}`)
  const output = (name: string) => logSink(logsDir, name, echo(name))
  const desktopLog = output('desktop')
  const log = (message: string) => desktopLog(`${new Date().toISOString()} ${message}\n`)
  const showLogs = () => void shell.openPath(logsDir)
  const desktopFile = join(dataDir, 'desktop.json')

  const supervisor = createSupervisor({
    dataDir,
    // AppImage resources are read-only, so the runtime runs in the data folder.
    runtime: runtimeLauncher(runtimeBinary(), { cwd: dataDir, baseEnv, output: output('coral') }),
    reef: reefLauncher(dataDir, output('reef')),
    readinessTimeoutMs: packaged ? PACKAGED_READINESS_TIMEOUT_MS : DEV_READINESS_TIMEOUT_MS,
    checkUpdates: updatable,
  })

  let state: SupervisorState = { status: 'stopped' }
  let window: BrowserWindow | null = null

  // Each page appears once it has drawn, so the window never shows blank.
  const show = () => {
    const current = window
    if (!current) return
    const page =
      state.status === 'running'
        ? state.endpoints.reef
        : state.status === 'failed'
          ? failurePage(state.failure, state.restarting, logsDir)
          : state.status === 'starting'
            ? startingPage()
            : null
    // A later state's page aborts this load; that is expected, not a failure.
    if (page)
      void current
        .loadURL(page)
        .catch((error: unknown) => log(`window: ${String(error)}`))
        .finally(() => window === current && current.show())
  }
  // On macOS, Coral is in the Dock and the app switcher only while it has a
  // window. With no window, the menu bar item is where the person finds it,
  // so a Coral that has no such item stays in the Dock.
  let menuBarItem = false
  const syncDock = createDockPresence({
    dock: process.platform === 'darwin' ? app.dock : undefined,
    wanted: () => window !== null || !menuBarItem,
    log,
  })
  const openWindow = () => {
    const opened = createMainWindow(
      join(iconsDir(), process.platform === 'win32' ? 'icon.ico' : 'icon.png'),
    )
    window = opened
    syncDock()
    const forget = () => {
      if (window === opened) window = null
    }
    opened.on('closed', forget)
    // A load still pending when the window closes settles while Electron
    // tears down the web contents, and show() then crashes the app (seen on
    // macOS, on Quit from the failure dialog). Electron emits `destroyed`
    // before it runs that settlement, so show() finds the window forgotten.
    opened.webContents.once('destroyed', forget)
    show()
  }

  supervisor.on('state', (next) => {
    state = next
    log(
      next.status === 'failed'
        ? `${next.status}: ${failureMessage(next.failure)}${next.restarting ? ' Restarting.' : ''}`
        : next.status,
    )
    show()
    if (next.status !== 'failed' || next.restarting) return
    if (next.failure.kind === 'port-in-use') void offerNewPort(next.failure)
    else void offerRestart(next.failure)
  })

  // Set once Quit begins. Nothing starts Coral after that, so Quit's stop
  // leaves no process behind.
  let quitting = false
  // Set by every Quit that Coral offers and that must stop it. On macOS, a quit
  // that arrives without it came from the Dock or the app switcher.
  let quitRequested = false
  const quitCompletely = () => {
    quitRequested = true
    app.quit()
  }
  // Set when macOS logs the person out, restarts or shuts down. This relies
  // on macOS posting that notice before it asks the apps to quit.
  let sessionEnding = false
  const restart = () =>
    void supervisor
      .stop()
      .then(() => (quitting ? undefined : supervisor.start()))
      .catch(logStartFailure)

  // Coral restarts by itself after a crash, within limits; past them, or when
  // it cannot start at all, the user decides.
  let offering = false
  async function offerRestart(failure: Failure) {
    if (offering) return
    offering = true
    try {
      for (;;) {
        const { response } = await dialog.showMessageBox({
          type: 'error',
          message: failureTitle(failure),
          detail: failureMessage(failure),
          buttons: ['Restart', 'Show logs', 'Quit'],
          defaultId: 0,
          cancelId: 2,
        })
        if (response === 1) {
          showLogs()
          continue
        }
        // The user may have restarted from the menu meanwhile.
        if (response === 0 && state.status === 'failed') restart()
        if (response === 2) quitCompletely()
        return
      }
    } finally {
      offering = false
    }
  }

  // A persisted port anchors URLs that agent clients and links have stored, so
  // moving it is the user's decision.
  async function offerNewPort(failure: Extract<Failure, { kind: 'port-in-use' }>) {
    const { response } = await dialog.showMessageBox({
      type: 'warning',
      message: `Port ${failure.port} is already in use`,
      detail:
        failure.listener === 'mcp'
          ? "Another app is using it. Coral can start on a different port, but you'll need to reconnect your apps to the new MCP endpoint."
          : "Another app is using it. Coral can start on a different port, but links to earlier runs won't open anymore.",
      buttons: ['Use Different Port', 'Quit'],
      defaultId: 0,
      cancelId: 1,
    })
    if (response === 0 && !quitting)
      void supervisor.start({ replacePort: failure.listener }).catch(logStartFailure)
    else quitCompletely()
  }

  // Launching Coral again is the way back to the window, and the only one on
  // a desktop that shows no tray.
  const openOrFocusWindow = () => {
    if (!window) return openWindow()
    if (window.isMinimized()) window.restore()
    window.focus()
  }
  app.on('second-instance', (_event, argv) => {
    // A login item that fires while Coral runs asks for no window.
    if (!argv.includes(BACKGROUND_ARG)) openOrFocusWindow()
  })
  // On macOS, activating a Coral that has no window opens one. The Dock icon
  // does that for a Coral that has no menu bar item.
  app.on('activate', () => {
    if (!window) openWindow()
  })
  // Coral serves agent clients only while Desktop runs, so it outlives its
  // last window (ADR 0014). Electron emits this only when the windows close
  // by themselves, not during Quit.
  let background: Background | undefined
  const lastWindowClosed = () => {
    if (quitting) return
    if (!background) return quitCompletely()
    // The Dock icon stays while the notice is open. The person may open the
    // window again meanwhile.
    void background.lastWindowClosed().then(syncDock)
  }
  app.on('window-all-closed', lastWindowClosed)
  // On macOS, Quit in the Coral menu only closes the windows, so Command+Q
  // does not stop Coral for the person's agents. Quit in the Dock and in the
  // app switcher does the same. Quit Coral in the menu bar item, and Quit
  // Completely in the Coral menu and the Dock, stop Coral.
  const quitFromMenu = () => {
    const windows = BrowserWindow.getAllWindows()
    if (windows.length === 0) return lastWindowClosed()
    for (const open of windows) open.close()
  }

  // Quit waits for both processes to stop, so none outlives the app.
  let stopped = false
  app.on('before-quit', (event) => {
    if (stopped) return
    event.preventDefault()
    if (quitting) return
    if (process.platform === 'darwin' && !quitRequested && !sessionEnding) return quitFromMenu()
    quitting = true
    void supervisor.stop().finally(() => {
      stopped = true
      app.quit()
    })
  })
  for (const signal of ['SIGINT', 'SIGTERM'] as const) process.on(signal, quitCompletely)

  // The installer replaces the app only after both processes stop. If the
  // hand-off fails, Coral starts again.
  const stopForUpdate = async () => {
    quitting = true
    await supervisor.stop()
    stopped = true
  }
  const resumeAfterUpdate = () => {
    stopped = false
    quitting = false
    void supervisor.start().catch(logStartFailure)
  }

  void app.whenReady().then(async () => {
    // Sessions exist only once the app is ready, and before any web contents.
    let updates: Updates | undefined
    installContentGuards(
      () => (state.status === 'running' ? state.endpoints.reef : null),
      log,
      updatable
        ? {
            runningVersion: app.getVersion(),
            offer: (version) => void updates?.offerRelease(version),
          }
        : undefined,
    )
    // powerMonitor works only once the app is ready.
    powerMonitor.on('shutdown', () => (sessionEnding = true))
    // macOS passes a login item no arguments, and reports the login launch.
    const hidden =
      startedInBackground ||
      (process.platform === 'darwin' && app.getLoginItemSettings().wasOpenedAtLogin)
    background = createBackground({
      told: await readToldStillRunning(desktopFile),
      saveTold: () => writeToldStillRunning(desktopFile),
      tellStillRunning,
      loginItem: loginItem(),
      log,
      onChange: () => menu(),
    })
    const { settings: backgroundSettings } = background
    const drawTray = installTray(iconsDir(), { open: openOrFocusWindow, quit: quitCompletely }, log)
    menuBarItem = drawTray !== undefined
    const menu = () => {
      const settings = backgroundSettings()
      const version = updates?.ready()
      installMenu({
        restart,
        showLogs,
        quit: quitFromMenu,
        quitCompletely,
        settings,
        update: updates?.menuItem(),
      })
      drawTray?.({
        settings,
        update: updates && version ? { version, item: updates.menuItem() } : undefined,
      })
    }
    if (updatable)
      updates = startUpdates({
        appImage,
        log,
        stopCoral: stopForUpdate,
        resumeCoral: resumeAfterUpdate,
        onMenuChange: menu,
      })
    menu()
    // A second launch or the Dock may have opened the window already.
    if (!hidden && !window) openWindow()
    else syncDock()
    void supervisor.start().catch(logStartFailure)
  })
}

// The window shows the failure, and the state listener logs it.
function logStartFailure(error: unknown) {
  if (error instanceof SupervisorError) return
  console.error(`[coral-desktop] ${String(error)}`)
}

function runtimeBinary(): string {
  const binary = process.platform === 'win32' ? 'coral.exe' : 'coral'
  const command = packaged ? join(process.resourcesPath, 'coral', binary) : devRuntime
  if (!command)
    throw new Error('Start development Desktop with `npm run dev`; it builds the runtime first')
  return command
}

function reefLauncher(cwd: string, output: OutputSink): Launcher<ReefEnv> {
  if (packaged)
    return utilityLauncher(join(process.resourcesPath, 'reef', 'server.mjs'), {
      cwd,
      baseEnv: { ...baseEnv, NODE_ENV: 'production' },
      output,
    })
  // Development runs Reef's Vite server, with hot reload, as `./dev` does.
  const reef = resolve(import.meta.dirname, '..', '..', '..', 'reef')
  return (env) =>
    spawnLauncher('npm', {
      args: [
        'run',
        'dev',
        '--prefix',
        reef,
        '--',
        '--host',
        env.HOST,
        '--port',
        env.PORT,
        '--strictPort',
      ],
      cwd,
      baseEnv: { ...baseEnv, NODE_ENV: 'development' },
      output,
      processGroup: true,
    })(env)
}

function iconsDir(): string {
  return packaged
    ? join(process.resourcesPath, 'icons')
    : resolve(import.meta.dirname, '..', '..', 'resources', 'icons')
}

// Shown once, the first time the last window closes.
async function tellStillRunning(): Promise<void> {
  const isMac = process.platform === 'darwin'
  await dialog.showMessageBox({
    type: 'info',
    message: 'Coral is still running',
    detail: isMac
      ? 'It stays in the menu bar so your apps can connect. To quit, use the Coral icon in the menu bar.'
      : 'It keeps running so your apps can connect. To quit, open Coral and choose Quit from the File menu.',
    buttons: ['OK'],
  })
}

/** This package's login item, or undefined when it cannot register one. */
function loginItem(): LoginItem | undefined {
  // A development build would register Electron itself.
  if (!packaged) return undefined
  // Electron's login item API does not cover Linux. Only an AppImage names a
  // file that an autostart entry can start.
  if (process.platform === 'linux')
    return appImage && isAbsolute(appImage)
      ? autostartLoginItem(join(app.getPath('appData'), 'autostart'), appImage)
      : undefined
  // Windows matches a login item by its arguments too.
  const args = process.platform === 'win32' ? [BACKGROUND_ARG] : []
  return {
    enabled: () => app.getLoginItemSettings({ args }).openAtLogin,
    set: (enabled) => app.setLoginItemSettings({ openAtLogin: enabled, args }),
  }
}

function installMenu(actions: {
  restart: () => void
  showLogs: () => void
  /** Quit in the Coral menu on macOS. Elsewhere, Quit always stops Coral. */
  quit: () => void
  /** Quit Completely in the Coral menu and the Dock menu on macOS. */
  quitCompletely: () => void
  settings: MenuCheckbox[]
  // Absent when this package cannot update itself.
  update: UpdateMenuItem | undefined
}) {
  const isMac = process.platform === 'darwin'
  const restart: Electron.MenuItemConstructorOptions = {
    label: 'Restart Coral',
    click: actions.restart,
  }
  // On macOS, Quit Coral only closes the windows. This one stops Coral.
  const quitCompletely: Electron.MenuItemConstructorOptions = {
    label: 'Quit Completely',
    click: actions.quitCompletely,
  }
  const { update: item } = actions
  const update: Electron.MenuItemConstructorOptions[] = item
    ? [{ label: item.label, enabled: item.enabled, click: () => void item.click() }]
    : []
  const checkboxes = actions.settings.map((setting): Electron.MenuItemConstructorOptions => ({
    type: 'checkbox',
    ...setting,
  }))
  // `role: 'about'` would label itself from app.name, the package name in dev.
  const about: Electron.MenuItemConstructorOptions = {
    label: 'About Coral',
    click: () => app.showAboutPanel(),
  }
  const template: Electron.MenuItemConstructorOptions[] = [
    isMac
      ? {
          label: 'Coral',
          submenu: [
            about,
            ...update,
            { type: 'separator' },
            ...checkboxes,
            { type: 'separator' },
            restart,
            quitCompletely,
            { type: 'separator' },
            { label: 'Quit Coral', accelerator: 'Command+Q', click: actions.quit },
          ],
        }
      : {
          label: 'File',
          submenu: [
            ...checkboxes,
            { type: 'separator' },
            restart,
            { type: 'separator' },
            { role: 'quit' },
          ],
        },
    // macOS routes copy and paste shortcuts through these menu roles.
    { role: 'editMenu' },
    {
      label: 'View',
      submenu: [
        ...(packaged
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
    // Close Window carries the platform's shortcut. Electron's own Window menu
    // leaves it out on macOS, where it expects a File menu to hold it.
    isMac
      ? {
          role: 'window',
          submenu: [
            { role: 'minimize' },
            { role: 'zoom' },
            { role: 'close' },
            { type: 'separator' },
            { role: 'front' },
          ],
        }
      : { role: 'windowMenu' },
    {
      role: 'help',
      submenu: [
        { label: 'Show logs', click: actions.showLogs },
        ...(isMac ? [] : [...update, about]),
      ],
    },
  ]
  Menu.setApplicationMenu(Menu.buildFromTemplate(template))
  // The Dock's own Quit only closes the windows, as Command+Q does, so the
  // Dock menu carries the Quit that stops Coral.
  if (isMac) app.dock?.setMenu(Menu.buildFromTemplate([...update, restart, quitCompletely]))
}
