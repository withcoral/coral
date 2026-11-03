import {
  app,
  BrowserWindow,
  autoUpdater as nativeAutoUpdater,
  dialog,
  Notification,
  shell,
} from 'electron'
import {
  AppImageUpdater,
  MacUpdater,
  NsisUpdater,
  type AppUpdater,
  type Logger,
} from 'electron-updater'
import { existsSync } from 'node:fs'
import { isAbsolute, join } from 'node:path'

import { releaseNotesUrl } from './content-policy'
import { createUpdates, type UpdatePrompts, type Updates } from './updates'

const FIRST_CHECK_DELAY_MS = 10_000
// A session that stays open for days still hears about new releases.
const CHECK_INTERVAL_MS = 4 * 60 * 60 * 1000

/**
 * Whether this package can update itself. Only a release package carries
 * `app-update.yml`, the v1 feed that electron-builder writes for its publish
 * configuration. On Linux only the AppImage replaces itself, and it names its
 * own file in `APPIMAGE`.
 */
export function updatesSupported(options: {
  packaged: boolean
  resourcesPath: string
  appImage: string | undefined
}): boolean {
  if (!options.packaged || !existsSync(join(options.resourcesPath, 'app-update.yml'))) return false
  return process.platform !== 'linux' || validAppImage(options.appImage) !== null
}

/**
 * Connects the update flow to electron-updater, native dialogs and the
 * application menu. `stopCoral` stops both processes before the installer
 * runs; `resumeCoral` starts them again when the hand-off fails.
 */
export function startUpdates(options: {
  appImage: string | undefined
  log: (message: string) => void
  stopCoral: () => Promise<void>
  resumeCoral: () => void
  onMenuChange: () => void
}): Updates {
  const updater = platformUpdater(options.appImage)
  const log = (message: unknown) => options.log(`updates: ${String(message)}`)
  const logger: Logger = { info: log, warn: log, error: log }
  updater.logger = logger
  // The flow downloads what a check finds. A downloaded update installs when
  // the app quits, after Desktop has stopped the runtime and Reef, unless the
  // person chose Restart to Update first.
  updater.autoDownload = false
  updater.autoInstallOnAppQuit = true

  const updates = createUpdates({
    updater,
    currentVersion: app.getVersion(),
    prompts,
    install: async () => {
      await options.stopCoral()
      try {
        await handOff(updater)
      } catch (error) {
        options.resumeCoral()
        throw error
      }
    },
    onChange: () => {
      showReady(updates.ready() !== null)
      options.onMenuChange()
    },
    progress: showProgress,
  })
  updater.on('download-progress', ({ percent }) => showProgress(percent / 100))

  setTimeout(() => void updates.check({ interactive: false }), FIRST_CHECK_DELAY_MS)
  setInterval(() => void updates.check({ interactive: false }), CHECK_INTERVAL_MS)
  return updates
}

// Marks Coral's icon while an update waits to install: a badge on the Dock,
// or a count of 1 on Linux launchers that show one. Windows has no badge; its
// taskbar overlay needs an icon of its own.
function showReady(ready: boolean) {
  if (process.platform === 'darwin') app.dock?.setBadge(ready ? '•' : '')
  else if (process.platform === 'linux') app.setBadgeCount(ready ? 1 : 0)
}

// Shows a download's progress on Coral's Dock or taskbar icon. Electron draws
// it there from any window, and removes it for a negative value.
function showProgress(fraction: number | null) {
  for (const window of BrowserWindow.getAllWindows()) window.setProgressBar(fraction ?? -1)
}

// Names each platform's updater class rather than taking electron-updater's
// `autoUpdater`, which picks a Linux updater from the package type on disk.
// Desktop ships only the AppImage there.
function platformUpdater(appImage: string | undefined): AppUpdater {
  if (process.platform === 'darwin') return new MacUpdater()
  if (process.platform === 'win32') {
    const updater = new NsisUpdater()
    // The installer carries the whole app; there is no web installer.
    updater.disableWebInstaller = true
    return updater
  }
  const updater = new AppImageUpdater()
  // The new image must not start while this one holds the single-instance
  // lock, so Desktop relaunches it after it exits.
  updater.autoRunAppAfterInstall = false
  let installed: string | null = null
  // Emitted when the new image lands beside the old one under another name.
  updater.on('appimage-filename-updated', (path: string) => (installed = validAppImage(path)))
  nativeAutoUpdater.once('before-quit-for-update', () => {
    const image = installed ?? validAppImage(appImage)
    // No arguments: AppRun adds its own again.
    if (image) app.relaunch({ execPath: image, args: [] })
  })
  return updater
}

/**
 * Hands the downloaded update to the installer, which quits the app. It
 * settles only on failure: electron-updater reports a hand-off that did not
 * start as an `error` event, not as a thrown error.
 */
function handOff(updater: AppUpdater): Promise<never> {
  return new Promise((_resolve, reject) => {
    updater.once('error', reject)
    try {
      // Windows installs silently and starts the new version itself.
      if (process.platform === 'win32') updater.quitAndInstall(true, true)
      else updater.quitAndInstall()
    } catch (error) {
      reject(error)
    }
  })
}

// electron-updater requires an absolute path without NUL bytes, and checks it
// only after the download.
function validAppImage(path: string | undefined | null): string | null {
  return path && isAbsolute(path) && !path.includes('\0') ? path : null
}

const STEP_FAILURES = {
  check: 'Coral could not check for updates',
  download: 'Coral could not download the update',
  install: 'Coral could not install the update',
} as const

const prompts: UpdatePrompts = {
  upToDate: async (version) => {
    await dialog.showMessageBox({
      type: 'info',
      message: 'Coral is up to date',
      detail: `You have Coral ${version}, the latest version.`,
    })
  },
  notReady: async (version) => {
    const { response } = await dialog.showMessageBox({
      type: 'info',
      message: `Coral ${version} is not ready to install yet.`,
      detail: 'Try again later.',
      buttons: ['Release Notes', 'OK'],
      defaultId: 1,
      cancelId: 1,
    })
    if (response === 0) openReleaseNotes(version)
  },
  offerInstall: async (version) => {
    const { response } = await dialog.showMessageBox({
      type: 'info',
      message: `Coral ${version} is ready`,
      detail: 'Restart Coral to update now.',
      buttons: ['Restart to Update', 'Release Notes', 'Later'],
      defaultId: 0,
      cancelId: 2,
    })
    if (response === 1) openReleaseNotes(version)
    return response === 0
  },
  failed: async (step, error) => {
    await dialog.showMessageBox({
      type: 'error',
      message: STEP_FAILURES[step],
      detail: `${error instanceof Error ? error.message : String(error)}\n\nTry again later.`,
    })
  },
  notifyReady: (_version, restart) => {
    if (!Notification.isSupported()) return
    const notification = new Notification({
      title: 'Update ready',
      body: 'Restart Coral to update now.',
    })
    notification.on('click', restart)
    notification.show()
  },
}

function openReleaseNotes(version: string) {
  void shell.openExternal(releaseNotesUrl(version))
}
