import { dialog, app } from 'electron'
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

function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.message
  return String(error)
}

function logUpdateError(context: string, error: unknown): void {
  console.error(`[coral-updater] ${context}: ${errorMessage(error)}`)
}

function installUpdateLogging(): void {
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
  autoUpdater.on('update-downloaded', (info) => {
    console.info(`[coral-updater] update downloaded: ${info.version}`)
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
      detail: 'Coral can check for desktop updates only from a packaged release build.',
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
  if (!app.isPackaged || installed) return

  installed = true
  autoUpdater.autoDownload = true
  autoUpdater.autoInstallOnAppQuit = true
  installUpdateLogging()

  setTimeout(() => {
    void checkForDesktopUpdates({ interactive: false })
  }, STARTUP_UPDATE_CHECK_DELAY_MS)
  // electron-updater dedupes overlapping checks, so a plain interval is safe.
  setInterval(() => {
    void checkForDesktopUpdates({ interactive: false })
  }, PERIODIC_UPDATE_CHECK_INTERVAL_MS)
}

export async function checkForDesktopUpdates({ interactive }: { interactive: boolean }): Promise<void> {
  if (!app.isPackaged) {
    if (interactive) await showManualResult(null)
    return
  }

  try {
    const result = await autoUpdater.checkForUpdatesAndNotify()
    if (interactive) await showManualResult(result)
  } catch (error) {
    logUpdateError('manual update check failed', error)
    if (interactive) {
      await dialog.showMessageBox({
        type: 'error',
        message: 'Update check failed',
        detail: errorMessage(error),
      })
    }
  }
}
