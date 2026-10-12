// Where the desktop suite's Desktop keeps its data. Desktop finds its data
// folder as it does for a person, so the app has no test hook.
//
// On Linux each test gets a temporary home, which HOME and XDG_CONFIG_HOME
// select. On Windows the suite does not rely on variables to move the profile:
// it uses the user's real one. That is safe only on a disposable machine, so
// setup refuses to start when Desktop data is already there, and each test
// deletes only what an earlier test in this run left.
import { existsSync } from 'node:fs'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { DESKTOP_DATA, type DesktopApp } from './config.ts'

/** Where every launch in one test keeps Desktop's data. */
export interface DesktopProfile {
  /** The folder Electron's `app.getPath('appData')` names for this profile. */
  appData: string
  /** The variables that select this profile. */
  env: Record<string, string>
}

/** Fails the run before any test when a profile could hold a person's data. */
export function checkDesktopProfiles(desktop: DesktopApp): void {
  // macOS finds the profile through the system, not HOME, so a temporary home
  // would not keep a person's data apart.
  if (process.platform !== 'linux' && process.platform !== 'win32')
    throw new Error('The desktop suite runs on Linux and Windows')
  if (process.platform !== 'win32') return
  const data = join(windowsAppData(desktop), DESKTOP_DATA)
  if (existsSync(data))
    throw new Error(`Desktop data exists at ${data}; run the desktop suite on a disposable machine`)
}

/**
 * Creates an empty profile. `remove` deletes it; Windows has none, so the last
 * test's data stays and the Windows leg can check that uninstalling keeps it.
 */
export async function freshDesktopProfile(
  desktop: DesktopApp,
): Promise<{ profile: DesktopProfile; remove?: () => Promise<void> }> {
  if (process.platform === 'win32') {
    const appData = windowsAppData(desktop)
    // checkDesktopProfiles found no data here, so this run wrote what is here.
    await rm(join(appData, DESKTOP_DATA), { recursive: true, force: true })
    return { profile: { appData, env: {} } }
  }
  const home = await mkdtemp(join(tmpdir(), 'coral-desktop-'))
  return {
    profile: {
      appData: join(home, '.config'),
      env: {
        HOME: home,
        XDG_CONFIG_HOME: join(home, '.config'),
        XDG_CACHE_HOME: join(home, '.cache'),
        XDG_DATA_HOME: join(home, '.local', 'share'),
      },
    },
    remove: () => rm(home, { recursive: true, force: true }),
  }
}

function windowsAppData(desktop: DesktopApp): string {
  const appData = desktop.env.APPDATA
  if (!appData) throw new Error('The desktop suite needs APPDATA on Windows')
  return appData
}
