import { Notification, app, dialog } from 'electron'
import { createRequire } from 'node:module'
import type { AppUpdater } from 'electron-updater'

import { createDesktopUpdater, type DesktopUpdater } from './auto-update-core'

const require = createRequire(import.meta.url)

// Only macOS release builds publish update metadata (latest-mac.yml); a
// packaged Windows/Linux app would poll for feeds that do not exist.
export function desktopUpdatesSupported(): boolean {
  return app.isPackaged && process.platform === 'darwin'
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
