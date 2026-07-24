import { Notification, app, dialog } from 'electron'
import { createRequire } from 'node:module'
import type { AppUpdater } from 'electron-updater'

import { createDesktopUpdater, type DesktopUpdater } from './auto-update-core'

const require = createRequire(import.meta.url)
const RELEASE_UPDATER_BUNDLE_MARKER = '[coral-updater] release updater enabled'

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
    })
  }
  return updater
}

export function installAutoUpdater(): void {
  if (!desktopUpdatesSupported()) return
  console.info(RELEASE_UPDATER_BUNDLE_MARKER)
  desktopUpdater().install()
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
