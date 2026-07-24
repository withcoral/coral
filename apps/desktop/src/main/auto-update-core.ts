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
  downloadPromise?: Promise<unknown> | null
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
  // The version whose download promise completed successfully. On macOS,
  // electron-updater emits `update-downloaded` before Squirrel has fetched the
  // archive from its local proxy; the promise resolves after that handoff.
  // Only the latter is safe to describe as ready for install-on-quit.
  let readyVersion: string | null = null
  const trackedDownloads = new WeakMap<Promise<unknown>, Promise<DownloadOutcome>>()

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
    updater.on('update-downloaded', (info) => {
      console.info(`[coral-updater] update archive downloaded: ${info.version}`)
    })
    updater.on('error', (error) => {
      console.error(`[coral-updater] updater error: ${errorMessage(error)}`)
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

    // If this version already finished downloading in a prior check, its ready
    // notification has already fired and will not fire again (dedupe), so
    // promising a future notification here would be a lie.
    if (readyVersion === result.updateInfo.version) {
      await deps.showInfoDialog(
        `Coral ${result.updateInfo.version} is ready`,
        'The update will install when you quit Coral.',
      )
      return
    }

    await deps.showInfoDialog(
      `Coral ${result.updateInfo.version} is downloading`,
      'You will be notified when the update is ready. It will install after Coral quits.',
    )
  }

  function trackDownload(result: UpdateCheckResultLike): Promise<DownloadOutcome> | null {
    const downloadPromise = result.downloadPromise
    if (!downloadPromise) return null

    const existing = trackedDownloads.get(downloadPromise)
    if (existing) return existing

    // electron-updater returns the same in-flight download promise to
    // overlapping checks. Track it once so every caller observes failures but
    // one completed version produces one notification.
    const version = result.updateInfo.version
    const outcome = downloadPromise.then<DownloadOutcome, DownloadOutcome>(
      () => {
        if (readyVersion !== version) {
          readyVersion = version
          try {
            deps.showNotification(
              'Coral update ready',
              `Coral ${version} will install when you quit the app.`,
            )
          } catch (error) {
            console.error(`[coral-updater] update notification failed: ${errorMessage(error)}`)
          }
        }
        return { ok: true }
      },
      (error: unknown) => {
        console.error(`[coral-updater] update download failed: ${errorMessage(error)}`)
        return { ok: false, error }
      },
    )
    trackedDownloads.set(downloadPromise, outcome)
    return outcome
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
    // The check and download layers each reuse their own in-flight promise;
    // trackDownload() also observes a shared download only once.
    setInterval(() => {
      void check({ interactive: false })
    }, PERIODIC_UPDATE_CHECK_INTERVAL_MS)
  }

  async function check({ interactive }: { interactive: boolean }): Promise<void> {
    let result: UpdateCheckResultLike | null
    try {
      result = await updater.checkForUpdates()
    } catch (error) {
      console.error(`[coral-updater] update check failed: ${errorMessage(error)}`)
      if (interactive) {
        await deps.showErrorDialog('Update check failed', errorMessage(error))
      }
      return
    }

    // Attach to the auto-download before opening an interactive dialog. The
    // download starts inside checkForUpdates() and can fail while that dialog
    // is still open; attaching now prevents an unhandled rejection.
    const download = result?.isUpdateAvailable ? trackDownload(result) : null
    if (interactive) await showManualResult(result)
    if (!download) return

    const outcome = await download
    if (!outcome.ok && interactive) {
      await deps.showErrorDialog('Update download failed', errorMessage(outcome.error))
    }
  }

  return { check, install }
}

type DownloadOutcome = { ok: true } | { ok: false; error: unknown }
