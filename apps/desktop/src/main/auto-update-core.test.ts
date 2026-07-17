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
    listenerCount(event: string) {
      return listeners.get(event)?.length ?? 0
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

function availableUpdate(
  version = '1.2.4',
  downloadPromise: Promise<unknown> | null = Promise.resolve(),
): UpdateCheckResultLike {
  return {
    isUpdateAvailable: true,
    updateInfo: { version },
    downloadPromise,
  }
}

function deferredPromise(): {
  promise: Promise<void>
  resolve: () => void
  reject: (error: unknown) => void
} {
  let resolve!: () => void
  let reject!: (error: unknown) => void
  const promise = new Promise<void>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise
    reject = rejectPromise
  })
  return { promise, reject, resolve }
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
    expect(updater.listenerCount('update-downloaded')).toBe(1)
  })
})

describe('download completion notifications', () => {
  it('waits for the download handoff promise instead of the earlier event', async () => {
    const updater = createFakeUpdater()
    const download = deferredPromise()
    vi.mocked(updater.checkForUpdates).mockResolvedValue(
      availableUpdate('1.2.4', download.promise),
    )
    const deps = createDeps(updater)
    const desktopUpdater = createDesktopUpdater(deps)
    desktopUpdater.install()

    const check = desktopUpdater.check({ interactive: false })
    await vi.advanceTimersByTimeAsync(0)
    updater.emit('update-downloaded', { version: '1.2.4' })
    expect(deps.notifications).toHaveLength(0)

    download.resolve()
    await check
    expect(deps.notifications).toHaveLength(1)
    expect(deps.notifications[0]).toEqual({
      title: 'Coral update ready',
      body: 'Coral 1.2.4 will install when you quit the app.',
    })
  })

  it('notifies once when overlapping checks share a download promise', async () => {
    const updater = createFakeUpdater()
    const download = deferredPromise()
    vi.mocked(updater.checkForUpdates).mockResolvedValue(
      availableUpdate('1.2.4', download.promise),
    )
    const deps = createDeps(updater)
    const desktopUpdater = createDesktopUpdater(deps)

    const firstCheck = desktopUpdater.check({ interactive: false })
    const secondCheck = desktopUpdater.check({ interactive: false })
    await vi.advanceTimersByTimeAsync(0)
    download.resolve()
    await Promise.all([firstCheck, secondCheck])

    expect(deps.notifications).toEqual([
      {
        title: 'Coral update ready',
        body: 'Coral 1.2.4 will install when you quit the app.',
      },
    ])
  })

  it('notifies again when a newer version is staged', async () => {
    const updater = createFakeUpdater()
    vi.mocked(updater.checkForUpdates)
      .mockResolvedValueOnce(availableUpdate('1.2.4'))
      .mockResolvedValueOnce(availableUpdate('1.2.5'))
    const deps = createDeps(updater)
    const desktopUpdater = createDesktopUpdater(deps)

    await desktopUpdater.check({ interactive: false })
    await desktopUpdater.check({ interactive: false })
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
    vi.mocked(updater.checkForUpdates).mockResolvedValue(availableUpdate())
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
    vi.mocked(updater.checkForUpdates).mockResolvedValue(availableUpdate())
    const deps = createDeps(updater)
    const desktopUpdater = createDesktopUpdater(deps)
    // Background download completes, then the user manually checks again.
    await desktopUpdater.check({ interactive: false })
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

  it('surfaces download failures after reporting that the update started', async () => {
    const updater = createFakeUpdater()
    const download = deferredPromise()
    const dialog = deferredPromise()
    vi.mocked(updater.checkForUpdates).mockResolvedValue(
      availableUpdate('1.2.4', download.promise),
    )
    const deps = createDeps(updater)
    deps.showInfoDialog = async (message, detail) => {
      deps.infoDialogs.push({ message, detail })
      await dialog.promise
    }

    const check = createDesktopUpdater(deps).check({ interactive: true })
    await vi.advanceTimersByTimeAsync(0)
    download.reject(new Error('zip handoff failed'))
    await vi.advanceTimersByTimeAsync(0)
    dialog.resolve()
    await expect(check).resolves.toBeUndefined()

    expect(deps.infoDialogs[0]?.message).toBe('Coral 1.2.4 is downloading')
    expect(deps.errorDialogs).toEqual([
      { message: 'Update download failed', detail: 'zip handoff failed' },
    ])
    expect(deps.notifications).toHaveLength(0)
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
    vi.mocked(updater.checkForUpdates).mockResolvedValue(availableUpdate())
    const deps = createDeps(updater)
    await createDesktopUpdater(deps).check({ interactive: false })

    expect(deps.infoDialogs).toHaveLength(0)
    expect(deps.errorDialogs).toHaveLength(0)
  })

  it('consumes download failures without showing dialogs or notifying', async () => {
    const updater = createFakeUpdater()
    vi.mocked(updater.checkForUpdates).mockResolvedValue(
      availableUpdate('1.2.4', Promise.reject(new Error('zip handoff failed'))),
    )
    const deps = createDeps(updater)

    await expect(createDesktopUpdater(deps).check({ interactive: false })).resolves.toBeUndefined()

    expect(deps.infoDialogs).toHaveLength(0)
    expect(deps.errorDialogs).toHaveLength(0)
    expect(deps.notifications).toHaveLength(0)
    expect(console.error).toHaveBeenCalledWith(
      '[coral-updater] update download failed: zip handoff failed',
    )
  })
})
