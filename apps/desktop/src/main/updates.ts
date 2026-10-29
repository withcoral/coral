// Desktop's update flow, apart from Electron so tests can drive it. A check
// downloads what it finds in the background. Installing waits for the person:
// Restart to Update, or the next time they quit Coral. The person can also
// start an update from Reef's sidebar notice. updater.ts connects
// electron-updater, native dialogs and the application menu.

/** The parts of electron-updater's AppUpdater that the flow uses. */
export interface UpdaterLike {
  checkForUpdates(): Promise<{
    isUpdateAvailable: boolean
    updateInfo: { version: string }
  } | null>
  downloadUpdate(): Promise<unknown>
}

/** Native dialogs and notifications. A question resolves true for yes. */
export interface UpdatePrompts {
  upToDate(version: string): Promise<void>
  /** Says that the release the sidebar notice names has nothing to install yet. */
  notReady(version: string): Promise<void>
  offerInstall(version: string): Promise<boolean>
  failed(step: 'check' | 'download' | 'install', error: unknown): Promise<void>
  /** Says a downloaded update is ready; `restart` installs it now. */
  notifyReady(version: string, restart: () => void): void
}

/** Who waits for a check: nobody, the menu's Check for Updates…, or a notice click. */
type Asker = { by: 'nobody' } | { by: 'menu' } | { by: 'notice'; version: string }

type UpdateState =
  | { status: 'idle' }
  | { status: 'checking' }
  | { status: 'downloading'; version: string }
  | { status: 'downloaded'; version: string }
  | { status: 'installing'; version: string }

export interface UpdateMenuItem {
  label: string
  enabled: boolean
  click: () => Promise<void>
}

export interface Updates {
  check(options: { interactive: boolean }): Promise<void>
  /**
   * Answers a click on Reef's notice of `version`. It runs as Check for
   * Updates… does. When it downloads an update, or one waits already, it
   * ends in the restart question. Otherwise it says why it cannot.
   */
  offerRelease(version: string): Promise<void>
  menuItem(): UpdateMenuItem
  /** The version that waits to install, or null. */
  ready(): string | null
}

export function createUpdates(deps: {
  updater: UpdaterLike
  currentVersion: string
  prompts: UpdatePrompts
  /** Stops Coral and hands the downloaded update to the platform's installer. */
  install: (version: string) => Promise<void>
  /** Called whenever the menu item changes. */
  onChange: () => void
  /** Shows a download's progress from 0 to 1, or clears it with null. */
  progress: (fraction: number | null) => void
}): Updates {
  const { updater, prompts } = deps
  let state: UpdateState = { status: 'idle' }
  // Who waits for the check under way.
  let asker: Asker = { by: 'nobody' }
  // One restart question at a time.
  let offering = false

  const moveTo = (next: UpdateState) => {
    state = next
    deps.onChange()
  }

  async function check({ interactive }: { interactive: boolean }) {
    // A downloaded update installs on quit, so nothing replaces it.
    if (state.status !== 'idle') return
    asker = interactive ? { by: 'menu' } : { by: 'nobody' }
    await run()
  }

  async function offerRelease(version: string) {
    switch (state.status) {
      case 'idle':
        asker = { by: 'notice', version }
        return run()
      case 'checking':
      case 'downloading':
        // The check under way ends in the restart question instead.
        asker = { by: 'notice', version }
        return
      case 'downloaded':
        return offerInstall(state.version)
      case 'installing':
        return
    }
  }

  // Each outcome reads `asker` when it arrives, since a notice click may come
  // while the check runs.
  async function run() {
    moveTo({ status: 'checking' })
    let result
    try {
      result = await updater.checkForUpdates()
    } catch (error) {
      if (endWith({ status: 'idle' }).by !== 'nobody') await prompts.failed('check', error)
      return
    }
    if (!result?.isUpdateAvailable) {
      const asked = endWith({ status: 'idle' })
      if (asked.by === 'notice') await prompts.notReady(asked.version)
      else if (asked.by === 'menu') await prompts.upToDate(deps.currentVersion)
      return
    }
    const version = result.updateInfo.version
    moveTo({ status: 'downloading', version })
    deps.progress(0)
    try {
      await updater.downloadUpdate()
    } catch (error) {
      // The next check tries again.
      deps.progress(null)
      if (endWith({ status: 'idle' }).by !== 'nobody') await prompts.failed('download', error)
      return
    }
    deps.progress(null)
    if (endWith({ status: 'downloaded', version }).by !== 'nobody') await offerInstall(version)
    else prompts.notifyReady(version, () => void install())
  }

  /** Ends the check in `next`, and answers who waited for it. */
  function endWith(next: UpdateState) {
    const asked = asker
    asker = { by: 'nobody' }
    moveTo(next)
    return asked
  }

  async function offerInstall(version: string) {
    if (offering) return
    offering = true
    try {
      if (await prompts.offerInstall(version)) await install()
    } finally {
      offering = false
    }
  }

  async function install() {
    if (state.status !== 'downloaded') return
    const { version } = state
    moveTo({ status: 'installing', version })
    try {
      await deps.install(version)
    } catch (error) {
      moveTo({ status: 'downloaded', version })
      await prompts.failed('install', error)
    }
  }

  function menuItem(): UpdateMenuItem {
    switch (state.status) {
      case 'idle':
        return item('Check for Updates…', () => check({ interactive: true }))
      case 'checking':
        return item('Checking for Updates…')
      case 'downloading':
        return item(`Downloading Coral ${state.version}…`)
      case 'downloaded':
        return item(`Restart to Update Coral ${state.version}`, install)
      case 'installing':
        return item(`Installing Coral ${state.version}…`)
    }
  }

  const ready = () => (state.status === 'downloaded' ? state.version : null)

  return { check, offerRelease, menuItem, ready }
}

/** A menu item that runs `click`, or a disabled one while a step is under way. */
function item(label: string, click?: () => Promise<void>): UpdateMenuItem {
  return { label, enabled: !!click, click: click ?? (async () => {}) }
}
