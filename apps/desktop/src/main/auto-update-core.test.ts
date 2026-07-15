import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import {
  PERIODIC_UPDATE_CHECK_INTERVAL_MS,
  STARTUP_UPDATE_CHECK_DELAY_MS,
  createDesktopUpdater,
  type DesktopUpdaterDeps,
  type UpdateCheckResultLike,
  type UpdaterLike,
} from './auto-update-core'

type Listener = (...args: unknown[]) => void

function createFakeUpdater() {
  const listeners = new Map<string, Listener[]>()
  const fake = {
    autoDownload: false,
    autoInstallOnAppQuit: false,
    on(event: string, listener: Listener) {
      listeners.set(event, [...(listeners.get(event) ?? []), listener])
      return fake
    },
    checkForUpdates: vi.fn(async (): Promise<UpdateCheckResultLike | null> => null),
    emit(event: string, ...args: unknown[]) {
      for (const listener of listeners.get(event) ?? []) listener(...args)
    },
  }
  // The generic (event, ...args) shape cannot satisfy UpdaterLike's typed
  // `on` overloads under strict function types; the double is still
  // behaviorally faithful.
  return fake as UpdaterLike & typeof fake
}

function createDeps(updater: UpdaterLike): DesktopUpdaterDeps & {
  notifications: Array<{ title: string; body: string }>
  infoDialogs: Array<{ message: string; detail: string }>
  errorDialogs: Array<{ message: string; detail: string }>
} {
  const notifications: Array<{ title: string; body: string }> = []
  const infoDialogs: Array<{ message: string; detail: string }> = []
  const errorDialogs: Array<{ message: string; detail: string }> = []
  return {
    updater,
    appVersion: () => '1.2.3',
    showInfoDialog: async (message, detail) => {
      infoDialogs.push({ message, detail })
    },
    showErrorDialog: async (message, detail) => {
      errorDialogs.push({ message, detail })
    },
    showNotification: (title, body) => {
      notifications.push({ title, body })
    },
    notifications,
    infoDialogs,
    errorDialogs,
  }
}

beforeEach(() => {
  vi.useFakeTimers()
  vi.spyOn(console, 'info').mockImplementation(() => {})
  vi.spyOn(console, 'error').mockImplementation(() => {})
})

afterEach(() => {
  vi.useRealTimers()
  vi.restoreAllMocks()
})

describe('install', () => {
  it('configures auto download and install-on-quit', () => {
    const updater = createFakeUpdater()
    createDesktopUpdater(createDeps(updater)).install()
    expect(updater.autoDownload).toBe(true)
    expect(updater.autoInstallOnAppQuit).toBe(true)
  })

  it('checks shortly after startup and again every interval', async () => {
    const updater = createFakeUpdater()
    createDesktopUpdater(createDeps(updater)).install()

    expect(updater.checkForUpdates).not.toHaveBeenCalled()
    await vi.advanceTimersByTimeAsync(STARTUP_UPDATE_CHECK_DELAY_MS)
    expect(updater.checkForUpdates).toHaveBeenCalledTimes(1)
    await vi.advanceTimersByTimeAsync(PERIODIC_UPDATE_CHECK_INTERVAL_MS)
    expect(updater.checkForUpdates).toHaveBeenCalledTimes(2)
    await vi.advanceTimersByTimeAsync(PERIODIC_UPDATE_CHECK_INTERVAL_MS)
    expect(updater.checkForUpdates).toHaveBeenCalledTimes(3)
  })

  it('is idempotent: a second install neither re-schedules nor re-subscribes', async () => {
    const updater = createFakeUpdater()
    const deps = createDeps(updater)
    const desktopUpdater = createDesktopUpdater(deps)
    desktopUpdater.install()
    desktopUpdater.install()

    await vi.advanceTimersByTimeAsync(STARTUP_UPDATE_CHECK_DELAY_MS)
    expect(updater.checkForUpdates).toHaveBeenCalledTimes(1)

    updater.emit('update-downloaded', { version: '1.2.4' })
    expect(deps.notifications).toHaveLength(1)
  })
})

describe('update-downloaded notifications', () => {
  it('notifies once per staged version even when the event repeats', () => {
    const updater = createFakeUpdater()
    const deps = createDeps(updater)
    createDesktopUpdater(deps).install()

    updater.emit('update-downloaded', { version: '1.2.4' })
    updater.emit('update-downloaded', { version: '1.2.4' })
    expect(deps.notifications).toHaveLength(1)
    expect(deps.notifications[0]).toEqual({
      title: 'Coral update ready',
      body: 'Coral 1.2.4 will install when you quit the app.',
    })
  })

  it('notifies again when a newer version is staged', () => {
    const updater = createFakeUpdater()
    const deps = createDeps(updater)
    createDesktopUpdater(deps).install()

    updater.emit('update-downloaded', { version: '1.2.4' })
    updater.emit('update-downloaded', { version: '1.2.5' })
    expect(deps.notifications.map((n) => n.body)).toEqual([
      'Coral 1.2.4 will install when you quit the app.',
      'Coral 1.2.5 will install when you quit the app.',
    ])
  })
})

describe('interactive checks', () => {
  it('reports up to date when no update is available', async () => {
    const updater = createFakeUpdater()
    vi.mocked(updater.checkForUpdates).mockResolvedValue({
      isUpdateAvailable: false,
      updateInfo: { version: '1.2.3' },
    })
    const deps = createDeps(updater)
    await createDesktopUpdater(deps).check({ interactive: true })

    expect(deps.infoDialogs).toEqual([
      { message: 'Coral is up to date', detail: 'You are running Coral 1.2.3.' },
    ])
  })

  it('reports the downloading version when an update is available', async () => {
    const updater = createFakeUpdater()
    vi.mocked(updater.checkForUpdates).mockResolvedValue({
      isUpdateAvailable: true,
      updateInfo: { version: '1.2.4' },
    })
    const deps = createDeps(updater)
    await createDesktopUpdater(deps).check({ interactive: true })

    expect(deps.infoDialogs).toEqual([
      {
        message: 'Coral 1.2.4 is downloading',
        detail:
          'You will be notified when the update is ready. It will install after Coral quits.',
      },
    ])
  })

  it('reports the update as ready (not downloading) once it has been staged', async () => {
    const updater = createFakeUpdater()
    vi.mocked(updater.checkForUpdates).mockResolvedValue({
      isUpdateAvailable: true,
      updateInfo: { version: '1.2.4' },
    })
    const deps = createDeps(updater)
    const desktopUpdater = createDesktopUpdater(deps)
    desktopUpdater.install()
    // Background download completes, then the user manually checks again.
    updater.emit('update-downloaded', { version: '1.2.4' })
    await desktopUpdater.check({ interactive: true })

    expect(deps.infoDialogs).toEqual([
      {
        message: 'Coral 1.2.4 is ready',
        detail: 'The update will install when you quit Coral.',
      },
    ])
    // The staged-version notification must not fire a second time.
    expect(deps.notifications).toHaveLength(1)
  })

  it('explains when update checks are unavailable', async () => {
    const updater = createFakeUpdater()
    vi.mocked(updater.checkForUpdates).mockResolvedValue(null)
    const deps = createDeps(updater)
    await createDesktopUpdater(deps).check({ interactive: true })

    expect(deps.infoDialogs).toEqual([
      {
        message: 'Update checks are unavailable for this build',
        detail: 'Coral can check for desktop updates only from a packaged macOS release build.',
      },
    ])
  })

  it('surfaces check failures in an error dialog without throwing', async () => {
    const updater = createFakeUpdater()
    vi.mocked(updater.checkForUpdates).mockRejectedValue(new Error('feed unreachable'))
    const deps = createDeps(updater)
    await expect(createDesktopUpdater(deps).check({ interactive: true })).resolves.toBeUndefined()

    expect(deps.errorDialogs).toEqual([
      { message: 'Update check failed', detail: 'feed unreachable' },
    ])
  })
})

describe('background checks', () => {
  it('logs check failures without showing dialogs', async () => {
    const updater = createFakeUpdater()
    vi.mocked(updater.checkForUpdates).mockRejectedValue(new Error('feed unreachable'))
    const deps = createDeps(updater)
    await expect(createDesktopUpdater(deps).check({ interactive: false })).resolves.toBeUndefined()

    expect(deps.errorDialogs).toHaveLength(0)
    expect(deps.infoDialogs).toHaveLength(0)
    expect(console.error).toHaveBeenCalledWith(
      '[coral-updater] update check failed: feed unreachable',
    )
  })

  it('does not show dialogs on successful background checks', async () => {
    const updater = createFakeUpdater()
    vi.mocked(updater.checkForUpdates).mockResolvedValue({
      isUpdateAvailable: true,
      updateInfo: { version: '1.2.4' },
    })
    const deps = createDeps(updater)
    await createDesktopUpdater(deps).check({ interactive: false })

    expect(deps.infoDialogs).toHaveLength(0)
    expect(deps.errorDialogs).toHaveLength(0)
  })
})
