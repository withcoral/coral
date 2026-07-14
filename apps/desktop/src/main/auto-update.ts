import { Notification, app, dialog } from 'electron'
import { createRequire } from 'node:module'
import type { AppUpdater, UpdateCheckResult } from 'electron-updater'

const require = createRequire(import.meta.url)
const { autoUpdater } = require('electron-updater') as { autoUpdater: AppUpdater }

const STARTUP_UPDATE_CHECK_DELAY_MS = 5000
// Long-running desktop sessions would otherwise only see new releases after a
// restart; re-check periodically so a release ships to open apps too. The
// downloaded update still installs on quit.
const PERIODIC_UPDATE_CHECK_INTERVAL_MS = 4 * 60 * 60 * 1000

let installed = false
let notifiedVersion: string | null = null

// Only macOS release builds publish update metadata (latest-mac.yml); a
// packaged Windows/Linux app would poll for feeds that do not exist.
export function desktopUpdatesSupported(): boolean {
  return app.isPackaged && process.platform === 'darwin'
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.message
  return String(error)
}

function logUpdateError(context: string, error: unknown): void {
  console.error(`[coral-updater] ${context}: ${errorMessage(error)}`)
}

function installUpdateListeners(): void {
  autoUpdater.on('checking-for-update', () => {
    console.info('[coral-updater] checking for updates')
  })
  autoUpdater.on('update-available', (info) => {
    console.info(`[coral-updater] update available: ${info.version}`)
  })
  autoUpdater.on('update-not-available', (info) => {
    console.info(`[coral-updater] no update available; latest is ${info.version}`)
  })
  autoUpdater.on('download-progress', (progress) => {
    console.info(`[coral-updater] download ${progress.percent.toFixed(1)}%`)
  })
  // Notify from this single listener instead of checkForUpdatesAndNotify():
  // that helper attaches a new notification continuation per call, so
  // overlapping or repeated checks notify more than once for one update.
  autoUpdater.on('update-downloaded', (info) => {
    console.info(`[coral-updater] update downloaded: ${info.version}`)
    if (notifiedVersion === info.version) return
    notifiedVersion = info.version
    new Notification({
      title: 'Coral update ready',
      body: `Coral ${info.version} will install when you quit the app.`,
    }).show()
  })
  autoUpdater.on('error', (error) => {
    logUpdateError('update check failed', error)
  })
}

async function showManualResult(result: UpdateCheckResult | null): Promise<void> {
  if (!result) {
    await dialog.showMessageBox({
      type: 'info',
      message: 'Update checks are unavailable for this build',
      detail: 'Coral can check for desktop updates only from a packaged macOS release build.',
    })
    return
  }

  if (!result.isUpdateAvailable) {
    await dialog.showMessageBox({
      type: 'info',
      message: 'Coral is up to date',
      detail: `You are running Coral ${app.getVersion()}.`,
    })
    return
  }

  await dialog.showMessageBox({
    type: 'info',
    message: `Coral ${result.updateInfo.version} is downloading`,
    detail: 'You will be notified when the update is ready. It will install after Coral quits.',
  })
}

export function installAutoUpdater(): void {
  if (!desktopUpdatesSupported() || installed) return

  installed = true
  autoUpdater.autoDownload = true
  autoUpdater.autoInstallOnAppQuit = true
  installUpdateListeners()

  setTimeout(() => {
    void checkForDesktopUpdates({ interactive: false })
  }, STARTUP_UPDATE_CHECK_DELAY_MS)
  // checkForUpdates() dedupes concurrent checks (it returns the in-flight
  // promise), so a plain interval cannot stack downloads.
  setInterval(() => {
    void checkForDesktopUpdates({ interactive: false })
  }, PERIODIC_UPDATE_CHECK_INTERVAL_MS)
}

export async function checkForDesktopUpdates({ interactive }: { interactive: boolean }): Promise<void> {
  if (!desktopUpdatesSupported()) {
    if (interactive) await showManualResult(null)
    return
  }

  try {
    const result = await autoUpdater.checkForUpdates()
    if (interactive) await showManualResult(result)
  } catch (error) {
    logUpdateError('update check failed', error)
    if (interactive) {
      await dialog.showMessageBox({
        type: 'error',
        message: 'Update check failed',
        detail: errorMessage(error),
      })
    }
  }
}
