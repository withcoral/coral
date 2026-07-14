// Electron-free updater logic so it can be exercised under vitest with a
// fake updater; auto-update.ts wires in electron-updater, dialogs, and
// notifications. Structural types keep this module importable without the
// electron runtime.

export const STARTUP_UPDATE_CHECK_DELAY_MS = 5000
// Long-running desktop sessions would otherwise only see new releases after a
// restart; re-check periodically so a release ships to open apps too. The
// downloaded update still installs on quit.
export const PERIODIC_UPDATE_CHECK_INTERVAL_MS = 4 * 60 * 60 * 1000

interface UpdateInfoLike {
  version: string
}

export interface UpdateCheckResultLike {
  isUpdateAvailable: boolean
  updateInfo: UpdateInfoLike
}

interface DownloadProgressLike {
  percent: number
}

export interface UpdaterLike {
  autoDownload: boolean
  autoInstallOnAppQuit: boolean
  on(event: 'checking-for-update', listener: () => void): unknown
  on(event: 'update-available', listener: (info: UpdateInfoLike) => void): unknown
  on(event: 'update-not-available', listener: (info: UpdateInfoLike) => void): unknown
  on(event: 'download-progress', listener: (progress: DownloadProgressLike) => void): unknown
  on(event: 'update-downloaded', listener: (info: UpdateInfoLike) => void): unknown
  on(event: 'error', listener: (error: Error) => void): unknown
  checkForUpdates(): Promise<UpdateCheckResultLike | null>
}

export interface DesktopUpdaterDeps {
  updater: UpdaterLike
  appVersion: () => string
  showInfoDialog: (message: string, detail: string) => Promise<void>
  showErrorDialog: (message: string, detail: string) => Promise<void>
  showNotification: (title: string, body: string) => void
}

export interface DesktopUpdater {
  install: () => void
  check: (options: { interactive: boolean }) => Promise<void>
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.message
  return String(error)
}

export function createDesktopUpdater(deps: DesktopUpdaterDeps): DesktopUpdater {
  const { updater } = deps
  let installed = false
  let notifiedVersion: string | null = null

  function installListeners(): void {
    updater.on('checking-for-update', () => {
      console.info('[coral-updater] checking for updates')
    })
    updater.on('update-available', (info) => {
      console.info(`[coral-updater] update available: ${info.version}`)
    })
    updater.on('update-not-available', (info) => {
      console.info(`[coral-updater] no update available; latest is ${info.version}`)
    })
    updater.on('download-progress', (progress) => {
      console.info(`[coral-updater] download ${progress.percent.toFixed(1)}%`)
    })
    // Notify from this single listener instead of checkForUpdatesAndNotify():
    // that helper attaches a new notification continuation per call, so
    // overlapping or repeated checks notify more than once for one update.
    updater.on('update-downloaded', (info) => {
      console.info(`[coral-updater] update downloaded: ${info.version}`)
      if (notifiedVersion === info.version) return
      notifiedVersion = info.version
      deps.showNotification(
        'Coral update ready',
        `Coral ${info.version} will install when you quit the app.`,
      )
    })
    updater.on('error', (error) => {
      console.error(`[coral-updater] update check failed: ${errorMessage(error)}`)
    })
  }

  async function showManualResult(result: UpdateCheckResultLike | null): Promise<void> {
    if (!result) {
      await deps.showInfoDialog(
        'Update checks are unavailable for this build',
        'Coral can check for desktop updates only from a packaged macOS release build.',
      )
      return
    }

    if (!result.isUpdateAvailable) {
      await deps.showInfoDialog(
        'Coral is up to date',
        `You are running Coral ${deps.appVersion()}.`,
      )
      return
    }

    await deps.showInfoDialog(
      `Coral ${result.updateInfo.version} is downloading`,
      'You will be notified when the update is ready. It will install after Coral quits.',
    )
  }

  function install(): void {
    if (installed) return

    installed = true
    updater.autoDownload = true
    updater.autoInstallOnAppQuit = true
    installListeners()

    setTimeout(() => {
      void check({ interactive: false })
    }, STARTUP_UPDATE_CHECK_DELAY_MS)
    // checkForUpdates() dedupes concurrent checks (it returns the in-flight
    // promise), so a plain interval cannot stack downloads.
    setInterval(() => {
      void check({ interactive: false })
    }, PERIODIC_UPDATE_CHECK_INTERVAL_MS)
  }

  async function check({ interactive }: { interactive: boolean }): Promise<void> {
    try {
      const result = await updater.checkForUpdates()
      if (interactive) await showManualResult(result)
    } catch (error) {
      console.error(`[coral-updater] update check failed: ${errorMessage(error)}`)
      if (interactive) {
        await deps.showErrorDialog('Update check failed', errorMessage(error))
      }
    }
  }

  return { check, install }
}
