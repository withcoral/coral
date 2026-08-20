import { Notification, app, autoUpdater as nativeAutoUpdater, dialog } from 'electron'
import { createRequire } from 'node:module'
import { join } from 'node:path'
import type { AppUpdater } from 'electron-updater'

import { releaseTarget } from '../shared/release-targets'
import type { DesktopUpdateState, DesktopUpdateStateListener } from '../shared/types'
import {
  createDesktopUpdater,
  UNSUPPORTED_UPDATE_DETAIL,
  type DesktopUpdater,
} from './auto-update-core'
import {
  clearUpdateIntent,
  discardUpdateIntent,
  shouldExitForUpdateIntent,
  writeUpdateIntent,
} from './update-intent'

const require = createRequire(import.meta.url)
const RELEASE_UPDATER_BUNDLE_MARKER = '[coral-updater] release updater enabled'
const UPDATE_INTENT_FILENAME = 'update-intent.json'

// Baked in at build time by electron.vite.config.ts (true only when
// CORAL_DESKTOP_RELEASE=1). `typeof` guard keeps it safe if the define is ever
// absent (e.g. a non-electron-vite build path).
declare const __CORAL_DESKTOP_RELEASE__: boolean
const isReleaseBuild =
  typeof __CORAL_DESKTOP_RELEASE__ !== 'undefined' && __CORAL_DESKTOP_RELEASE__

// The path electron-updater's AppImageUpdater operates on: it downloads against
// this file, unlinks it, and moves the new image over it (doInstall). It reads
// the value straight from the environment and refuses to run when it is unset
// or relative, so this gate matches the updater's own precondition rather than
// trying to detect an AppImage independently. An extracted AppDir does not set
// it — the AppRun template assigns a fallback without exporting it.
function runningAppImagePath(): string | null {
  const path = process.env.APPIMAGE?.trim()
  return path ? path : null
}

// Updates need a release build of a package that can replace itself in place.
// The platform half of that is the shared release-target table; the rest is
// local: an unsigned QA or local build never polls, and on Linux only the
// AppImage can swap itself, because dpkg owns the files of an installed deb.
// Anything excluded here gets no polling and no menu item.
export function desktopUpdatesSupported(): boolean {
  if (!isReleaseBuild || !app.isPackaged) return false
  if (!releaseTarget(process.platform)) return false
  return process.platform !== 'linux' || runningAppImagePath() !== null
}

// Runs once the updater has moved the new image over the old path and is about
// to quit. Electron starts the replacement only after this process exits, so
// the new instance finds the single-instance lock free. See desktopUpdater()
// for why the updater itself starts nothing.
function scheduleAppImageRelaunch(): void {
  if (process.platform !== 'linux') return
  const appImagePath = runningAppImagePath()
  if (!appImagePath) return

  // Empty args, not Electron's default `process.argv.slice(1)`: that carries
  // the flags AppRun added for this launch, and AppRun adds them again.
  app.relaunch({ execPath: appImagePath, args: [] })
}

let updater: DesktopUpdater | null = null
let installFailureHandler = () => app.exit(0)
const UNSUPPORTED_UPDATE_STATE: DesktopUpdateState = { status: 'unsupported' }

function updateIntentPath(): string {
  return join(app.getPath('userData'), UPDATE_INTENT_FILENAME)
}

function desktopUpdater(): DesktopUpdater {
  if (!updater) {
    const { autoUpdater } = require('electron-updater') as { autoUpdater: AppUpdater }
    // Linux only, and the platform check is load-bearing: the two updaters read
    // this flag for different things. AppImageUpdater starts the new image from
    // inside its install step, while this process still holds the
    // single-instance lock — false there, and scheduleAppImageRelaunch() starts
    // it after the exit instead. MacUpdater reads the same flag to choose
    // between the Squirrel hand-off and a plain app.quit(); false there would
    // skip the hand-off and leave the staged update uninstalled.
    if (process.platform === 'linux') autoUpdater.autoRunAppAfterInstall = false
    updater = createDesktopUpdater({
      updater: autoUpdater,
      appVersion: () => app.getVersion(),
      showInfoDialog: async (message, detail) => {
        await dialog.showMessageBox({ type: 'info', message, detail })
      },
      showErrorDialog: async (message, detail) => {
        await dialog.showMessageBox({ type: 'error', message, detail })
      },
      showConfirmDialog: async (message, detail, confirmLabel) => {
        // Confirm is index 0 so Return accepts it; Escape maps to cancelId.
        const { response } = await dialog.showMessageBox({
          type: 'info',
          message,
          detail,
          buttons: [confirmLabel, 'Later'],
          defaultId: 0,
          cancelId: 1,
        })
        return response === 0
      },
      showNotification: (title, body) => {
        new Notification({ title, body }).show()
      },
      recordUpdateIntent: (targetVersion) => {
        writeUpdateIntent(updateIntentPath(), targetVersion)
      },
      clearUpdateIntent: () => {
        clearUpdateIntent(updateIntentPath())
      },
      // Core clears the marker before this callback; the lifecycle handler then
      // allows a normal quit from the already-stopped state.
      onInstallFailure: () => installFailureHandler(),
    })
  }
  return updater
}

export function shouldExitForPendingDesktopUpdate(): boolean {
  if (!desktopUpdatesSupported()) return false
  return shouldExitForUpdateIntent(updateIntentPath(), app.getVersion())
}

export function clearPendingDesktopUpdateIntent(): void {
  if (!desktopUpdatesSupported()) return
  discardUpdateIntent(updateIntentPath())
}

export function installAutoUpdater({
  allowUpdateQuit,
  onInstallFailure,
}: {
  allowUpdateQuit: () => void
  onInstallFailure: () => void
}): void {
  if (!desktopUpdatesSupported()) return

  installFailureHandler = onInstallFailure
  // electron-updater emits this on the native updater only after the install
  // step succeeds, so a failed install schedules no relaunch.
  nativeAutoUpdater.once('before-quit-for-update', () => {
    scheduleAppImageRelaunch()
    allowUpdateQuit()
  })
  console.info(RELEASE_UPDATER_BUNDLE_MARKER)
  desktopUpdater().install()
}

export function quitAndInstallDesktopUpdate(): boolean {
  if (!desktopUpdatesSupported()) return false
  return desktopUpdater().quitAndInstall()
}

// Rejects on a failed transfer so the renderer can report it; the core has
// already rolled the state back to `available` for a retry.
export async function downloadDesktopUpdate(): Promise<void> {
  if (!desktopUpdatesSupported()) return

  const outcome = await desktopUpdater().download()
  if (outcome.ok) return
  throw outcome.error instanceof Error ? outcome.error : new Error(String(outcome.error))
}

export function getDesktopUpdateState(): DesktopUpdateState {
  if (!desktopUpdatesSupported()) return UNSUPPORTED_UPDATE_STATE
  return desktopUpdater().getUpdateState()
}

export function onDesktopUpdateStateChange(listener: DesktopUpdateStateListener): () => void {
  if (!desktopUpdatesSupported()) return () => {}
  return desktopUpdater().onUpdateStateChange(listener)
}

export async function checkForDesktopUpdates({
  interactive,
}: {
  interactive: boolean
}): Promise<void> {
  if (!desktopUpdatesSupported()) {
    if (interactive) {
      await dialog.showMessageBox({
        type: 'info',
        message: 'Update checks are unavailable for this build',
        detail: UNSUPPORTED_UPDATE_DETAIL,
      })
    }
    return
  }
  await desktopUpdater().check({ interactive })
}
