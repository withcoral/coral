// Desktop's update flow, apart from Electron so tests can drive it. A check
// downloads what it finds in the background. Installing waits for the person:
// Restart to Update, or the next time they quit Coral. updater.ts connects
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
  offerInstall(version: string): Promise<boolean>
  failed(step: 'check' | 'download' | 'install', error: unknown): Promise<void>
  /** Says a downloaded update is ready; `restart` installs it now. */
  notifyReady(version: string, restart: () => void): void
}

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
}): Updates {
  const { updater, prompts } = deps
  let state: UpdateState = { status: 'idle' }

  const moveTo = (next: UpdateState) => {
    state = next
    deps.onChange()
  }

  async function check({ interactive }: { interactive: boolean }) {
    // A downloaded update installs on quit, so nothing replaces it.
    if (state.status !== 'idle') return
    moveTo({ status: 'checking' })
    let result
    try {
      result = await updater.checkForUpdates()
    } catch (error) {
      moveTo({ status: 'idle' })
      if (interactive) await prompts.failed('check', error)
      return
    }
    if (!result?.isUpdateAvailable) {
      moveTo({ status: 'idle' })
      if (interactive) await prompts.upToDate(deps.currentVersion)
      return
    }
    const version = result.updateInfo.version
    moveTo({ status: 'downloading', version })
    try {
      await updater.downloadUpdate()
    } catch (error) {
      // The next check tries again.
      moveTo({ status: 'idle' })
      if (interactive) await prompts.failed('download', error)
      return
    }
    moveTo({ status: 'downloaded', version })
    if (!interactive) prompts.notifyReady(version, () => void install())
    else if (await prompts.offerInstall(version)) await install()
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

  return { check, menuItem, ready }
}

/** A menu item that runs `click`, or a disabled one while a step is under way. */
function item(label: string, click?: () => Promise<void>): UpdateMenuItem {
  return { label, enabled: !!click, click: click ?? (async () => {}) }
}
