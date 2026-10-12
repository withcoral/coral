import { Menu, nativeImage, Tray } from 'electron'
import { join } from 'node:path'

import type { MenuCheckbox } from './background'
import type { UpdateMenuItem } from './updates'

export interface TrayState {
  settings: MenuCheckbox[]
  /** The downloaded update that waits to install, and the item that installs it. */
  update: { version: string; item: UpdateMenuItem } | undefined
}

/**
 * Adds Coral's tray item, the menu bar item on macOS. Returns the function
 * that redraws it, or undefined when Electron cannot make one. A desktop with
 * no tray, such as stock GNOME, shows nothing; there the person launches Coral
 * again to open the window.
 */
export function installTray(
  icons: string,
  actions: { open: () => void; quit: () => void },
  log: (message: string) => void,
): ((state: TrayState) => void) | undefined {
  let tray: Tray
  try {
    tray = new Tray(trayIcon(icons))
  } catch (error) {
    log(`tray: ${String(error)}`)
    return undefined
  }
  // Windows opens an app from its tray icon. macOS and Linux show the menu.
  if (process.platform === 'win32') tray.on('click', actions.open)
  return ({ settings, update }) => {
    const waiting: Electron.MenuItemConstructorOptions[] = update
      ? [
          {
            label: update.item.label,
            enabled: update.item.enabled,
            click: () => void update.item.click(),
          },
          { type: 'separator' },
        ]
      : []
    tray.setContextMenu(
      Menu.buildFromTemplate([
        ...waiting,
        { label: 'Open Coral', click: actions.open },
        { type: 'separator' },
        ...settings.map((item): Electron.MenuItemConstructorOptions => ({
          type: 'checkbox',
          ...item,
        })),
        { type: 'separator' },
        { label: 'Quit Coral', click: actions.quit },
      ]),
    )
    tray.setToolTip(update ? `Coral ${update.version} is ready to install` : 'Coral')
    // The menu bar shows no tooltip until the pointer rests on the item, so a
    // mark beside the icon says an update waits, as the Dock badge does.
    if (process.platform === 'darwin') tray.setTitle(update ? '•' : '')
  }
}

function trayIcon(icons: string): Electron.NativeImage | string {
  // A template image, which macOS tints for the menu bar. Electron finds the
  // `@2x` file beside it.
  if (process.platform === 'darwin') return join(icons, 'trayTemplate.png')
  if (process.platform === 'win32') return join(icons, 'icon.ico')
  return nativeImage.createFromPath(join(icons, 'icon.png')).resize({ width: 32, height: 32 })
}
