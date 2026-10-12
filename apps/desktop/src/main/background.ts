// What Desktop does without a window, apart from Electron so tests can drive
// it. Coral serves agent clients only while Desktop runs, so Desktop keeps
// running after the last window closes, and the person can start it at login.
// docs/adr/0014-desktop-serves-in-the-background.md records why. index.ts
// connects the dialog, the menus and the tray.
import { readDesktopFile, updateDesktopFile } from './desktop-file'

/** Starts Desktop with no window. The login item passes it where it can. */
export const BACKGROUND_ARG = '--background'

/**
 * Whether an earlier launch told the person that Coral keeps running, from
 * `file`, Desktop's `desktop.json`. A file that cannot be read says no: the
 * supervisor reads it too, and reports it.
 */
export async function readToldStillRunning(file: string): Promise<boolean> {
  const sections = await readDesktopFile(file).catch((): Record<string, unknown> => ({}))
  // 1.0.0-beta.8 kept a setting here. A person who left it on was told then;
  // one who turned it off must learn that closing the window no longer quits.
  return sections.toldStillRunning === true || sections.keepRunning === true
}

/** Keeps that in `file`, beside the sections that other modules own. */
export function writeToldStillRunning(file: string): Promise<void> {
  return updateDesktopFile(file, { toldStillRunning: true })
}

/** This package's registration to start when the person logs in. The system keeps it. */
export interface LoginItem {
  enabled(): boolean
  set(enabled: boolean): void
}

export interface MenuCheckbox {
  label: string
  checked: boolean
  click: () => void
}

export interface Background {
  /**
   * Coral keeps running when its last window closes. The first time, Desktop
   * tells the person so. Resolves once the person has read the notice.
   */
  lastWindowClosed(): Promise<void>
  /** The settings, for the application menu and the tray. */
  settings(): MenuCheckbox[]
}

export function createBackground(deps: {
  /** Whether an earlier launch told the person that Coral keeps running. */
  told: boolean
  saveTold: () => Promise<void>
  /** Tells the person that Coral still runs, and how to quit. */
  tellStillRunning: () => Promise<void>
  /** Absent when this package cannot register itself. */
  loginItem: LoginItem | undefined
  log: (message: string) => void
  /** Called whenever a checkbox changes. */
  onChange: () => void
}): Background {
  const { loginItem } = deps
  let told = deps.told
  let telling: Promise<void> | undefined

  async function lastWindowClosed() {
    if (told) return
    telling ??= deps
      .tellStillRunning()
      .then(() => {
        // This launch does not repeat the notice even when the file does not take it.
        told = true
        return deps.saveTold()
      })
      .catch((error: unknown) => deps.log(`background: the notice was not kept: ${String(error)}`))
      .finally(() => (telling = undefined))
    await telling
  }

  function settings(): MenuCheckbox[] {
    if (!loginItem) return []
    return [
      {
        label: 'Start at login',
        checked: loginItem.enabled(),
        click: () => {
          try {
            loginItem.set(!loginItem.enabled())
          } catch (error) {
            deps.log(`background: the login item did not change: ${String(error)}`)
          }
          deps.onChange()
        },
      },
    ]
  }

  return { lastWindowClosed, settings }
}
