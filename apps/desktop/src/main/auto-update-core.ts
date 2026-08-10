// Electron-free updater logic so it can be exercised under vitest with a
// fake updater; auto-update.ts wires in electron-updater, dialogs, and
// notifications. Structural types keep this module importable without the
// electron runtime.

import type { DesktopUpdateState, DesktopUpdateStateListener } from '../shared/types'

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
  downloadUpdate(): Promise<unknown>
  quitAndInstall(): void
}

export interface DesktopUpdaterDeps {
  updater: UpdaterLike
  appVersion: () => string
  showInfoDialog: (message: string, detail: string) => Promise<void>
  showErrorDialog: (message: string, detail: string) => Promise<void>
  showConfirmDialog: (message: string, detail: string, confirmLabel: string) => Promise<boolean>
  showNotification: (title: string, body: string) => void
  recordUpdateIntent: (targetVersion: string) => void
  clearUpdateIntent: () => void
  onInstallFailure: (error: Error) => void
}

export interface DesktopUpdater {
  install: () => void
  check: (options: { interactive: boolean }) => Promise<void>
  download: () => Promise<DownloadOutcome>
  getUpdateState: () => DesktopUpdateState
  onUpdateStateChange: (listener: DesktopUpdateStateListener) => () => void
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
  let updateState: DesktopUpdateState = { status: 'idle' }
  const updateStateListeners = new Set<DesktopUpdateStateListener>()
  // With automatic install disabled on macOS, the promise resolves once the
  // complete archive is available through electron-updater's local proxy.
  // Explicit quitAndInstall() starts the subsequent Squirrel hand-off.
  let readyVersion: string | null = null
  let activeDownload: Promise<DownloadOutcome> | null = null
  let activeCheck:
    | {
        result: Promise<CheckOutcome>
        completion: Promise<void>
      }
    | null = null

  function updateStateVersion(state: DesktopUpdateState): string | null {
    return 'version' in state ? state.version : null
  }

  function setUpdateState(nextState: DesktopUpdateState): void {
    if (
      updateState.status === nextState.status &&
      updateStateVersion(updateState) === updateStateVersion(nextState)
    ) {
      return
    }

    updateState = nextState
    for (const listener of updateStateListeners) {
      try {
        listener(updateState)
      } catch (error) {
        console.error(`[coral-updater] update state listener failed: ${errorMessage(error)}`)
      }
    }
  }

  function getUpdateState(): DesktopUpdateState {
    return updateState
  }

  function onUpdateStateChange(listener: DesktopUpdateStateListener): () => void {
    updateStateListeners.add(listener)
    return () => {
      updateStateListeners.delete(listener)
    }
  }

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
      const failedVersion = readyVersion
      readyVersion = null
      if (failedVersion) {
        setUpdateState({ status: 'available', version: failedVersion })
      }
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

  // Resolves to whether the user asked for the download to start now.
  async function showManualResult(result: UpdateCheckResultLike | null): Promise<boolean> {
    if (!result) {
      await deps.showInfoDialog(
        'Update checks are unavailable for this build',
        'Coral can check for desktop updates only from a packaged macOS release build.',
      )
      return false
    }

    if (!result.isUpdateAvailable) {
      await deps.showInfoDialog(
        'Coral is up to date',
        `You are running Coral ${deps.appVersion()}.`,
      )
      return false
    }

    // If this version already finished downloading in a prior check, its ready
    // notification has already fired and will not fire again (dedupe), so
    // promising a future notification here would be a lie.
    if (readyVersion === result.updateInfo.version) {
      await deps.showInfoDialog(
        `Coral ${result.updateInfo.version} is ready`,
        'The update will install when you quit Coral.',
      )
      return false
    }

    return deps.showConfirmDialog(
      `Coral ${result.updateInfo.version} is available`,
      'Download it now? The update installs after Coral quits.',
      'Download',
    )
  }

  function startDownload(version: string): Promise<DownloadOutcome> {
    setUpdateState({ status: 'downloading', version })
    return updater.downloadUpdate().then<DownloadOutcome, DownloadOutcome>(
      () => {
        if (readyVersion !== version) {
          readyVersion = version
          setUpdateState({ status: 'ready', version })
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
        setUpdateState({ status: 'available', version })
        console.error(`[coral-updater] update download failed: ${errorMessage(error)}`)
        return { ok: false, error }
      },
    )
  }

  function download(): Promise<DownloadOutcome> {
    if (activeDownload) return activeDownload

    // Anything other than `available` — already staged, nothing found, or a
    // click against state the renderer has not caught up with — is a no-op
    // rather than a failure worth surfacing.
    if (updateState.status !== 'available') return Promise.resolve(NOTHING_TO_DOWNLOAD)

    const started = startDownload(updateState.version).finally(() => {
      if (activeDownload === started) activeDownload = null
    })
    activeDownload = started
    return started
  }

  function install(): void {
    if (installed) return

    installed = true
    updater.autoDownload = false
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

    if (result?.isUpdateAvailable) {
      setUpdateState({ status: 'available', version: result.updateInfo.version })
    } else {
      setUpdateState({ status: 'idle' })
    }

    return { ok: true, result }
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

    if (!interactive) return
    if (!(await showManualResult(result.result))) return

    const outcome = await download()
    if (!outcome.ok) {
      await deps.showErrorDialog('Update download failed', errorMessage(outcome.error))
    }
  }

  function check({ interactive }: { interactive: boolean }): Promise<void> {
    // MacUpdater owns one local proxy for the staged ZIP. Serialize checks
    // against each other and against a download, then stop checking once ready,
    // so no later operation can replace that proxy while Squirrel fetches it.
    if (readyVersion) {
      return interactive
        ? deps.showInfoDialog(
            `Coral ${readyVersion} is ready`,
            'The update will install when you quit Coral.',
          )
        : Promise.resolve()
    }

    if (activeDownload) {
      const downloadingVersion = updateStateVersion(updateState)
      return interactive && downloadingVersion
        ? deps.showInfoDialog(
            `Coral ${downloadingVersion} is downloading`,
            'You will be notified when the update is ready. It will install after Coral quits.',
          )
        : activeDownload.then(() => undefined)
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

  return { check, download, getUpdateState, install, onUpdateStateChange, quitAndInstall }
}

export type DownloadOutcome = { ok: true } | { ok: false; error: unknown }
const NOTHING_TO_DOWNLOAD: DownloadOutcome = { ok: true }
type CheckOutcome =
  | { ok: false; error: unknown }
  | { ok: true; result: UpdateCheckResultLike | null }
