// Electron-free updater logic so it can be exercised under vitest with a
// fake updater; auto-update.ts wires in electron-updater, dialogs, and
// notifications. Structural types keep this module importable without the
// electron runtime.

export const STARTUP_UPDATE_CHECK_DELAY_MS = 5000
// Long-running desktop sessions would otherwise only see new releases after a
// restart; re-check periodically so a release ships to open apps too. The
// downloaded update is handed to the installer explicitly after app teardown.
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
  quitAndInstall(): void
}

export interface DesktopUpdaterDeps {
  updater: UpdaterLike
  appVersion: () => string
  showInfoDialog: (message: string, detail: string) => Promise<void>
  showErrorDialog: (message: string, detail: string) => Promise<void>
  showNotification: (title: string, body: string) => void
  recordUpdateIntent: (targetVersion: string) => void
  clearUpdateIntent: () => void
  onInstallFailure: (error: Error) => void
}

export interface DesktopUpdater {
  install: () => void
  check: (options: { interactive: boolean }) => Promise<void>
  quitAndInstall: () => boolean
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.message
  return String(error)
}

export function createDesktopUpdater(deps: DesktopUpdaterDeps): DesktopUpdater {
  const { updater } = deps
  let installed = false
  let installing = false
  // With automatic install disabled on macOS, the promise resolves once the
  // complete archive is available through electron-updater's local proxy.
  // Explicit quitAndInstall() starts the subsequent Squirrel hand-off.
  let readyVersion: string | null = null
  let activeCheck:
    | {
        result: Promise<CheckOutcome>
        completion: Promise<void>
      }
    | null = null

  function clearInstallIntent(): void {
    try {
      deps.clearUpdateIntent()
    } catch (error) {
      console.error(`[coral-updater] failed to clear update intent: ${errorMessage(error)}`)
    }
  }

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
      if (!installing) return

      installing = false
      readyVersion = null
      clearInstallIntent()
      try {
        deps.onInstallFailure(error)
      } catch (failureError) {
        console.error(
          `[coral-updater] install failure handler failed: ${errorMessage(failureError)}`,
        )
      }
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

    const version = result.updateInfo.version
    return downloadPromise.then<DownloadOutcome, DownloadOutcome>(
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
  }

  function install(): void {
    if (installed) return

    installed = true
    updater.autoDownload = true
    updater.autoInstallOnAppQuit = false
    installListeners()

    setTimeout(() => {
      void check({ interactive: false })
    }, STARTUP_UPDATE_CHECK_DELAY_MS)
    setInterval(() => {
      void check({ interactive: false })
    }, PERIODIC_UPDATE_CHECK_INTERVAL_MS)
  }

  async function performCheck(): Promise<CheckOutcome> {
    let result: UpdateCheckResultLike | null
    try {
      result = await updater.checkForUpdates()
    } catch (error) {
      console.error(`[coral-updater] update check failed: ${errorMessage(error)}`)
      return { ok: false, error }
    }

    // Attach before exposing the result. The download starts inside
    // checkForUpdates() and can fail while an interactive dialog is open;
    // attaching now prevents an unhandled rejection.
    const download = result?.isUpdateAvailable ? trackDownload(result) : null
    return { ok: true, result, download }
  }

  async function observeCheck(
    resultPromise: Promise<CheckOutcome>,
    { interactive }: { interactive: boolean },
  ): Promise<void> {
    const result = await resultPromise
    if (!result.ok) {
      if (interactive) {
        await deps.showErrorDialog('Update check failed', errorMessage(result.error))
      }
      return
    }

    if (interactive) await showManualResult(result.result)
    if (!result.download) return

    const download = await result.download
    if (!download.ok && interactive) {
      await deps.showErrorDialog('Update download failed', errorMessage(download.error))
    }
  }

  function check({ interactive }: { interactive: boolean }): Promise<void> {
    // MacUpdater owns one local proxy for the staged ZIP. Serialize the whole
    // check and download, then stop checking once ready, so no later operation
    // can replace that proxy while Squirrel fetches from it.
    if (readyVersion) {
      return interactive
        ? deps.showInfoDialog(
            `Coral ${readyVersion} is ready`,
            'The update will install when you quit Coral.',
          )
        : Promise.resolve()
    }

    if (activeCheck) {
      return interactive
        ? observeCheck(activeCheck.result, { interactive: true })
        : activeCheck.completion
    }

    const result = performCheck()
    const completion = observeCheck(result, { interactive }).finally(() => {
      if (activeCheck?.completion === completion) activeCheck = null
    })
    activeCheck = { result, completion }
    return completion
  }

  function quitAndInstall(): boolean {
    if (installing) return true
    if (!readyVersion) return false

    try {
      deps.recordUpdateIntent(readyVersion)
    } catch (error) {
      console.error(`[coral-updater] failed to record update intent: ${errorMessage(error)}`)
      return false
    }

    installing = true
    try {
      updater.quitAndInstall()
      return true
    } catch (error) {
      installing = false
      clearInstallIntent()
      console.error(`[coral-updater] explicit install failed: ${errorMessage(error)}`)
      return false
    }
  }

  return { check, install, quitAndInstall }
}

type DownloadOutcome = { ok: true } | { ok: false; error: unknown }
type CheckOutcome =
  | { ok: false; error: unknown }
  | {
      ok: true
      result: UpdateCheckResultLike | null
      download: Promise<DownloadOutcome> | null
    }
