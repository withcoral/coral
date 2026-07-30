import { Notification, app, autoUpdater as nativeAutoUpdater, dialog } from 'electron'
import { createRequire } from 'node:module'
import { join } from 'node:path'
import type { AppUpdater } from 'electron-updater'

import type { DesktopUpdateState, DesktopUpdateStateListener } from '../shared/types'
import { createDesktopUpdater, type DesktopUpdater } from './auto-update-core'
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

// Updates only work in a signed macOS release build: only those publish an
// update feed (latest-mac.yml), and Squirrel.Mac refuses to install an update
// into an unsigned app. Unsigned QA/local builds and Windows/Linux packages
// therefore get no polling and no menu item.
export function desktopUpdatesSupported(): boolean {
  return isReleaseBuild && app.isPackaged && process.platform === 'darwin'
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
    updater = createDesktopUpdater({
      updater: autoUpdater,
      appVersion: () => app.getVersion(),
      showInfoDialog: async (message, detail) => {
        await dialog.showMessageBox({ type: 'info', message, detail })
      },
      showErrorDialog: async (message, detail) => {
        await dialog.showMessageBox({ type: 'error', message, detail })
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
  nativeAutoUpdater.once('before-quit-for-update', allowUpdateQuit)
  console.info(RELEASE_UPDATER_BUNDLE_MARKER)
  desktopUpdater().install()
}

export function quitAndInstallDesktopUpdate(): boolean {
  if (!desktopUpdatesSupported()) return false
  return desktopUpdater().quitAndInstall()
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
        detail: 'Coral can check for desktop updates only from a packaged macOS release build.',
      })
    }
    return
  }
  await desktopUpdater().check({ interactive })
}
