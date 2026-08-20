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

const DOWNLOADING_DETAIL =
  'You will be notified when the update is ready. It will install after Coral quits.'

function createFakeUpdater() {
  const listeners = new Map<string, Listener[]>()
  const fake = {
    autoDownload: true,
    autoInstallOnAppQuit: true,
    on(event: string, listener: Listener) {
      listeners.set(event, [...(listeners.get(event) ?? []), listener])
      return fake
    },
    checkForUpdates: vi.fn(async (): Promise<UpdateCheckResultLike | null> => null),
    downloadUpdate: vi.fn(async (): Promise<void> => {}),
    quitAndInstall: vi.fn(),
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
  confirmDialogs: Array<{ message: string; detail: string; confirmLabel: string }>
  // Mutable so a test can answer the confirmation before the check runs.
  confirmAnswer: { value: boolean }
} {
  const notifications: Array<{ title: string; body: string }> = []
  const infoDialogs: Array<{ message: string; detail: string }> = []
  const errorDialogs: Array<{ message: string; detail: string }> = []
  const confirmDialogs: Array<{ message: string; detail: string; confirmLabel: string }> = []
  const confirmAnswer = { value: false }
  return {
    updater,
    appVersion: () => '1.2.3',
    showInfoDialog: async (message, detail) => {
      infoDialogs.push({ message, detail })
    },
    showErrorDialog: async (message, detail) => {
      errorDialogs.push({ message, detail })
    },
    showConfirmDialog: async (message, detail, confirmLabel) => {
      confirmDialogs.push({ message, detail, confirmLabel })
      return confirmAnswer.value
    },
    showNotification: (title, body) => {
      notifications.push({ title, body })
    },
    recordUpdateIntent: vi.fn(),
    clearUpdateIntent: vi.fn(),
    onInstallFailure: vi.fn(),
    notifications,
    infoDialogs,
    errorDialogs,
    confirmDialogs,
    confirmAnswer,
  }
}

function availableUpdate(version = '1.2.4'): UpdateCheckResultLike {
  return {
    isUpdateAvailable: true,
    updateInfo: { version },
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
  it('leaves every automatic step to the caller', async () => {
    const updater = createFakeUpdater()
    createDesktopUpdater(createDeps(updater)).install()

    // No download without a click, and no install on an ordinary quit.
    expect(updater.autoDownload).toBe(false)
    expect(updater.autoInstallOnAppQuit).toBe(false)
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

describe('update state', () => {
  it('parks an available update until a download is requested', async () => {
    const updater = createFakeUpdater()
    const download = deferredPromise()
    vi.mocked(updater.checkForUpdates).mockResolvedValue(availableUpdate('1.2.4'))
    vi.mocked(updater.downloadUpdate).mockReturnValue(download.promise)
    const desktopUpdater = createDesktopUpdater(createDeps(updater))
    const states: ReturnType<typeof desktopUpdater.getUpdateState>[] = []
    desktopUpdater.onUpdateStateChange((state) => states.push(state))
    desktopUpdater.install()

    expect(desktopUpdater.getUpdateState()).toEqual({ status: 'idle' })

    await desktopUpdater.check({ interactive: false })
    expect(updater.downloadUpdate).not.toHaveBeenCalled()
    expect(desktopUpdater.getUpdateState()).toEqual({ status: 'available', version: '1.2.4' })

    const started = desktopUpdater.download()
    expect(desktopUpdater.getUpdateState()).toEqual({
      status: 'downloading',
      version: '1.2.4',
    })

    // The event fires before the archive is reachable through the local proxy.
    updater.emit('update-downloaded', { version: '1.2.4' })
    expect(desktopUpdater.getUpdateState()).toEqual({
      status: 'downloading',
      version: '1.2.4',
    })

    download.resolve()
    await expect(started).resolves.toEqual({ ok: true })

    expect(desktopUpdater.getUpdateState()).toEqual({ status: 'ready', version: '1.2.4' })
    expect(states).toEqual([
      { status: 'available', version: '1.2.4' },
      { status: 'downloading', version: '1.2.4' },
      { status: 'ready', version: '1.2.4' },
    ])
  })

  it('returns to available when a download fails', async () => {
    const updater = createFakeUpdater()
    const download = deferredPromise()
    vi.mocked(updater.checkForUpdates).mockResolvedValue(availableUpdate('1.2.4'))
    vi.mocked(updater.downloadUpdate).mockReturnValue(download.promise)
    const desktopUpdater = createDesktopUpdater(createDeps(updater))
    const states: ReturnType<typeof desktopUpdater.getUpdateState>[] = []
    desktopUpdater.onUpdateStateChange((state) => states.push(state))

    await desktopUpdater.check({ interactive: false })
    const started = desktopUpdater.download()
    download.reject(new Error('zip handoff failed'))

    await expect(started).resolves.toEqual({
      ok: false,
      error: new Error('zip handoff failed'),
    })
    expect(desktopUpdater.getUpdateState()).toEqual({
      status: 'available',
      version: '1.2.4',
    })
    expect(states).toEqual([
      { status: 'available', version: '1.2.4' },
      { status: 'downloading', version: '1.2.4' },
      { status: 'available', version: '1.2.4' },
    ])
  })

  it('deduplicates unchanged state and supports unsubscribing', async () => {
    const updater = createFakeUpdater()
    vi.mocked(updater.checkForUpdates)
      .mockResolvedValueOnce({
        isUpdateAvailable: false,
        updateInfo: { version: '1.2.3' },
      })
      .mockResolvedValueOnce(availableUpdate('1.2.4'))
      .mockResolvedValueOnce({
        isUpdateAvailable: false,
        updateInfo: { version: '1.2.3' },
      })
    const desktopUpdater = createDesktopUpdater(createDeps(updater))
    const listener = vi.fn()
    const unsubscribe = desktopUpdater.onUpdateStateChange(listener)

    await desktopUpdater.check({ interactive: false })
    expect(listener).not.toHaveBeenCalled()

    await desktopUpdater.check({ interactive: false })
    expect(listener).toHaveBeenCalledOnce()
    expect(listener).toHaveBeenLastCalledWith({ status: 'available', version: '1.2.4' })

    unsubscribe()
    await desktopUpdater.check({ interactive: false })
    expect(desktopUpdater.getUpdateState()).toEqual({ status: 'idle' })
    expect(listener).toHaveBeenCalledOnce()
  })
})

describe('user-initiated downloads', () => {
  it('ignores requests made outside the available state', async () => {
    const updater = createFakeUpdater()
    vi.mocked(updater.checkForUpdates).mockResolvedValue(availableUpdate())
    const desktopUpdater = createDesktopUpdater(createDeps(updater))

    await expect(desktopUpdater.download()).resolves.toEqual({ ok: true })
    expect(updater.downloadUpdate).not.toHaveBeenCalled()

    await desktopUpdater.check({ interactive: false })
    await desktopUpdater.download()
    expect(updater.downloadUpdate).toHaveBeenCalledOnce()

    // Already staged: a second request must not re-stage the same archive.
    await desktopUpdater.download()
    expect(updater.downloadUpdate).toHaveBeenCalledOnce()
  })

  it('joins the transfer already in flight instead of starting a second', async () => {
    const updater = createFakeUpdater()
    const download = deferredPromise()
    vi.mocked(updater.checkForUpdates).mockResolvedValue(availableUpdate())
    vi.mocked(updater.downloadUpdate).mockReturnValue(download.promise)
    const deps = createDeps(updater)
    const desktopUpdater = createDesktopUpdater(deps)
    await desktopUpdater.check({ interactive: false })

    const first = desktopUpdater.download()
    const second = desktopUpdater.download()
    download.resolve()
    await Promise.all([first, second])

    expect(updater.downloadUpdate).toHaveBeenCalledOnce()
    expect(deps.notifications).toEqual([
      {
        title: 'Coral update ready',
        body: 'Coral 1.2.4 will install when you quit the app.',
      },
    ])
  })

  it('does not touch the feed while a download holds the local proxy', async () => {
    const updater = createFakeUpdater()
    const download = deferredPromise()
    vi.mocked(updater.checkForUpdates).mockResolvedValue(availableUpdate())
    vi.mocked(updater.downloadUpdate).mockReturnValue(download.promise)
    const desktopUpdater = createDesktopUpdater(createDeps(updater))
    await desktopUpdater.check({ interactive: false })

    const started = desktopUpdater.download()
    const backgroundCheck = desktopUpdater.check({ interactive: false })
    expect(updater.checkForUpdates).toHaveBeenCalledOnce()

    download.resolve()
    await Promise.all([started, backgroundCheck])
    expect(updater.checkForUpdates).toHaveBeenCalledOnce()
  })

  it('keeps its state when a check started before the click finds nothing', async () => {
    const updater = createFakeUpdater()
    const feed = deferredPromise()
    const download = deferredPromise()
    vi.mocked(updater.checkForUpdates)
      .mockResolvedValueOnce(availableUpdate('1.2.4'))
      .mockImplementationOnce(async () => {
        await feed.promise
        return { isUpdateAvailable: false, updateInfo: { version: '1.2.3' } }
      })
    vi.mocked(updater.downloadUpdate).mockReturnValue(download.promise)
    const desktopUpdater = createDesktopUpdater(createDeps(updater))
    const states: ReturnType<typeof desktopUpdater.getUpdateState>[] = []
    await desktopUpdater.check({ interactive: false })
    desktopUpdater.onUpdateStateChange((state) => states.push(state))

    // The release is pulled from the feed between the periodic check and the
    // click: its stale result must not blank the pill mid-transfer.
    const backgroundCheck = desktopUpdater.check({ interactive: false })
    const started = desktopUpdater.download()
    feed.resolve()
    await backgroundCheck
    expect(desktopUpdater.getUpdateState()).toEqual({ status: 'downloading', version: '1.2.4' })

    download.resolve()
    await started
    expect(states).toEqual([
      { status: 'downloading', version: '1.2.4' },
      { status: 'ready', version: '1.2.4' },
    ])
  })

  it('logs a failure without dialogs or a ready notification', async () => {
    const updater = createFakeUpdater()
    vi.mocked(updater.checkForUpdates).mockResolvedValue(availableUpdate())
    vi.mocked(updater.downloadUpdate).mockRejectedValue(new Error('zip handoff failed'))
    const deps = createDeps(updater)
    const desktopUpdater = createDesktopUpdater(deps)
    await desktopUpdater.check({ interactive: false })

    await expect(desktopUpdater.download()).resolves.toEqual({
      ok: false,
      error: new Error('zip handoff failed'),
    })
    expect(deps.infoDialogs).toHaveLength(0)
    expect(deps.errorDialogs).toHaveLength(0)
    expect(deps.notifications).toHaveLength(0)
    expect(console.error).toHaveBeenCalledWith(
      '[coral-updater] update download failed: zip handoff failed',
    )
  })
})

describe('explicit install hand-off', () => {
  it('waits for the local update payload and hands off only once', async () => {
    const events: string[] = []
    const updater = createFakeUpdater()
    const download = deferredPromise()
    vi.mocked(updater.checkForUpdates).mockResolvedValue(availableUpdate('1.2.4'))
    vi.mocked(updater.downloadUpdate).mockReturnValue(download.promise)
    const deps = createDeps(updater)
    vi.mocked(deps.recordUpdateIntent).mockImplementation((version) => {
      events.push(`intent:write:${version}`)
    })
    updater.quitAndInstall.mockImplementation(() => {
      events.push('updater:quitAndInstall')
    })
    const desktopUpdater = createDesktopUpdater(deps)
    desktopUpdater.install()

    await desktopUpdater.check({ interactive: false })
    const started = desktopUpdater.download()
    expect(desktopUpdater.quitAndInstall()).toBe(false)
    expect(deps.recordUpdateIntent).not.toHaveBeenCalled()
    expect(updater.quitAndInstall).not.toHaveBeenCalled()

    download.resolve()
    await started
    expect(desktopUpdater.quitAndInstall()).toBe(true)
    expect(desktopUpdater.quitAndInstall()).toBe(true)
    expect(updater.quitAndInstall).toHaveBeenCalledOnce()
    expect(events).toEqual(['intent:write:1.2.4', 'updater:quitAndInstall'])
  })

  it('clears intent before finishing quit on an updater failure', async () => {
    const events: string[] = []
    const updater = createFakeUpdater()
    vi.mocked(updater.checkForUpdates).mockResolvedValue(availableUpdate())
    const deps = createDeps(updater)
    vi.mocked(deps.clearUpdateIntent).mockImplementation(() => {
      events.push('intent:clear')
    })
    vi.mocked(deps.onInstallFailure).mockImplementation(() => {
      events.push('app:quit')
    })
    const desktopUpdater = createDesktopUpdater(deps)
    desktopUpdater.install()
    await desktopUpdater.check({ interactive: false })
    await desktopUpdater.download()
    expect(desktopUpdater.quitAndInstall()).toBe(true)

    updater.emit('error', new Error('ShipIt hand-off failed'))

    expect(events).toEqual(['intent:clear', 'app:quit'])
  })

  it('does not start installation when the intent marker cannot be written', async () => {
    const updater = createFakeUpdater()
    vi.mocked(updater.checkForUpdates).mockResolvedValue(availableUpdate())
    const deps = createDeps(updater)
    vi.mocked(deps.recordUpdateIntent).mockImplementation(() => {
      throw new Error('disk full')
    })
    const desktopUpdater = createDesktopUpdater(deps)
    desktopUpdater.install()
    await desktopUpdater.check({ interactive: false })
    await desktopUpdater.download()

    expect(desktopUpdater.quitAndInstall()).toBe(false)
    expect(updater.quitAndInstall).not.toHaveBeenCalled()
  })

  it('does not treat an updater error before hand-off as an install failure', () => {
    const updater = createFakeUpdater()
    const deps = createDeps(updater)
    createDesktopUpdater(deps).install()

    updater.emit('error', new Error('background check failed'))

    expect(deps.clearUpdateIntent).not.toHaveBeenCalled()
    expect(deps.onInstallFailure).not.toHaveBeenCalled()
  })
})

describe('download completion notifications', () => {
  it('waits for the local proxy promise instead of the earlier event', async () => {
    const updater = createFakeUpdater()
    const download = deferredPromise()
    vi.mocked(updater.checkForUpdates).mockResolvedValue(availableUpdate('1.2.4'))
    vi.mocked(updater.downloadUpdate).mockReturnValue(download.promise)
    const deps = createDeps(updater)
    const desktopUpdater = createDesktopUpdater(deps)
    desktopUpdater.install()

    await desktopUpdater.check({ interactive: false })
    const started = desktopUpdater.download()
    updater.emit('update-downloaded', { version: '1.2.4' })
    expect(deps.notifications).toHaveLength(0)

    download.resolve()
    await started
    expect(deps.notifications).toHaveLength(1)
    expect(deps.notifications[0]).toEqual({
      title: 'Coral update ready',
      body: 'Coral 1.2.4 will install when you quit the app.',
    })
  })
})

describe('interactive checks', () => {
  it('reuses an in-flight background result', async () => {
    const updater = createFakeUpdater()
    const feed = deferredPromise()
    vi.mocked(updater.checkForUpdates)
      .mockImplementationOnce(async () => {
        await feed.promise
        return {
          isUpdateAvailable: false,
          updateInfo: { version: '1.2.3' },
        }
      })
      .mockRejectedValueOnce(new Error('transient second request failure'))
    const deps = createDeps(updater)
    const desktopUpdater = createDesktopUpdater(deps)

    const background = desktopUpdater.check({ interactive: false })
    const interactive = desktopUpdater.check({ interactive: true })
    feed.resolve()
    await Promise.all([background, interactive])

    expect(updater.checkForUpdates).toHaveBeenCalledOnce()
    expect(deps.infoDialogs).toEqual([
      { message: 'Coral is up to date', detail: 'You are running Coral 1.2.3.' },
    ])
    expect(deps.errorDialogs).toHaveLength(0)
  })

  it('offers the download and starts it when the user accepts', async () => {
    const updater = createFakeUpdater()
    vi.mocked(updater.checkForUpdates).mockResolvedValue(availableUpdate('1.2.4'))
    const deps = createDeps(updater)
    deps.confirmAnswer.value = true
    const desktopUpdater = createDesktopUpdater(deps)

    await desktopUpdater.check({ interactive: true })

    expect(deps.confirmDialogs).toEqual([
      {
        message: 'Coral 1.2.4 is available',
        detail: 'Download it now? The update installs after Coral quits.',
        confirmLabel: 'Download',
      },
    ])
    expect(updater.downloadUpdate).toHaveBeenCalledOnce()
    expect(desktopUpdater.getUpdateState()).toEqual({ status: 'ready', version: '1.2.4' })
  })

  it('leaves the update waiting when the user declines', async () => {
    const updater = createFakeUpdater()
    vi.mocked(updater.checkForUpdates).mockResolvedValue(availableUpdate('1.2.4'))
    const deps = createDeps(updater)
    const desktopUpdater = createDesktopUpdater(deps)

    await desktopUpdater.check({ interactive: true })

    expect(deps.confirmDialogs).toHaveLength(1)
    expect(updater.downloadUpdate).not.toHaveBeenCalled()
    expect(deps.notifications).toHaveLength(0)
    expect(desktopUpdater.getUpdateState()).toEqual({ status: 'available', version: '1.2.4' })
  })

  it('reports an in-flight download instead of re-checking the feed', async () => {
    const updater = createFakeUpdater()
    const download = deferredPromise()
    vi.mocked(updater.checkForUpdates).mockResolvedValue(availableUpdate('1.2.4'))
    vi.mocked(updater.downloadUpdate).mockReturnValue(download.promise)
    const deps = createDeps(updater)
    const desktopUpdater = createDesktopUpdater(deps)
    await desktopUpdater.check({ interactive: false })
    const started = desktopUpdater.download()

    await desktopUpdater.check({ interactive: true })

    expect(deps.infoDialogs).toEqual([
      { message: 'Coral 1.2.4 is downloading', detail: DOWNLOADING_DETAIL },
    ])
    expect(deps.confirmDialogs).toHaveLength(0)
    expect(updater.checkForUpdates).toHaveBeenCalledOnce()

    download.resolve()
    await started
  })

  it('reports the update as ready (not downloading) once it has been staged', async () => {
    const updater = createFakeUpdater()
    vi.mocked(updater.checkForUpdates).mockResolvedValue(availableUpdate())
    const deps = createDeps(updater)
    const desktopUpdater = createDesktopUpdater(deps)
    // The download finishes, then the user manually checks again.
    await desktopUpdater.check({ interactive: false })
    await desktopUpdater.download()
    await desktopUpdater.check({ interactive: true })

    expect(deps.infoDialogs).toEqual([
      {
        message: 'Coral 1.2.4 is ready',
        detail: 'The update will install when you quit Coral.',
      },
    ])
    expect(updater.checkForUpdates).toHaveBeenCalledOnce()
    // The staged-version notification must not fire a second time.
    expect(deps.notifications).toHaveLength(1)
  })

  it('reports the transfer that started mid-check instead of offering its own result', async () => {
    const updater = createFakeUpdater()
    const feed = deferredPromise()
    const download = deferredPromise()
    vi.mocked(updater.checkForUpdates)
      .mockResolvedValueOnce(availableUpdate('1.2.4'))
      .mockImplementationOnce(async () => {
        await feed.promise
        return availableUpdate('1.2.5')
      })
    vi.mocked(updater.downloadUpdate).mockReturnValue(download.promise)
    const deps = createDeps(updater)
    deps.confirmAnswer.value = true
    const desktopUpdater = createDesktopUpdater(deps)
    await desktopUpdater.check({ interactive: false })

    // 1.2.5 lands while 1.2.4 is already transferring. Confirming it would be a
    // promise the updater cannot keep: download() joins the 1.2.4 transfer.
    const interactive = desktopUpdater.check({ interactive: true })
    const started = desktopUpdater.download()
    feed.resolve()
    await interactive

    expect(deps.confirmDialogs).toHaveLength(0)
    expect(deps.infoDialogs).toEqual([
      { message: 'Coral 1.2.4 is downloading', detail: DOWNLOADING_DETAIL },
    ])
    expect(updater.downloadUpdate).toHaveBeenCalledOnce()

    download.resolve()
    await started
  })

  it('reports the staged archive when a download finishes mid-check', async () => {
    const updater = createFakeUpdater()
    const feed = deferredPromise()
    vi.mocked(updater.checkForUpdates)
      .mockResolvedValueOnce(availableUpdate('1.2.4'))
      .mockImplementationOnce(async () => {
        await feed.promise
        return availableUpdate('1.2.5')
      })
    const deps = createDeps(updater)
    deps.confirmAnswer.value = true
    const desktopUpdater = createDesktopUpdater(deps)
    await desktopUpdater.check({ interactive: false })

    // 1.2.4 reaches `ready` before the check returns 1.2.5. download() would
    // find nothing to do, so confirming 1.2.5 would silently do nothing at all.
    const interactive = desktopUpdater.check({ interactive: true })
    await desktopUpdater.download()
    feed.resolve()
    await interactive

    expect(deps.confirmDialogs).toHaveLength(0)
    expect(deps.infoDialogs).toEqual([
      {
        message: 'Coral 1.2.4 is ready',
        detail: 'The update will install when you quit Coral.',
      },
    ])
    expect(updater.downloadUpdate).toHaveBeenCalledOnce()
  })

  it('announces once when a second manual check arrives during a download', async () => {
    const updater = createFakeUpdater()
    const feed = deferredPromise()
    const download = deferredPromise()
    vi.mocked(updater.checkForUpdates)
      .mockResolvedValueOnce(availableUpdate('1.2.4'))
      .mockImplementationOnce(async () => {
        await feed.promise
        return availableUpdate('1.2.5')
      })
    vi.mocked(updater.downloadUpdate).mockReturnValue(download.promise)
    const deps = createDeps(updater)
    const desktopUpdater = createDesktopUpdater(deps)
    await desktopUpdater.check({ interactive: false })

    // The first manual check already owes the user a dialog. The second must
    // wait for it rather than queue an identical modal behind it.
    const first = desktopUpdater.check({ interactive: true })
    const started = desktopUpdater.download()
    const second = desktopUpdater.check({ interactive: true })
    feed.resolve()
    await Promise.all([first, second])

    expect(deps.infoDialogs).toEqual([
      { message: 'Coral 1.2.4 is downloading', detail: DOWNLOADING_DETAIL },
    ])
    expect(deps.confirmDialogs).toHaveLength(0)

    download.resolve()
    await started
  })

  it('announces once when a second manual check arrives after staging', async () => {
    const updater = createFakeUpdater()
    const feed = deferredPromise()
    vi.mocked(updater.checkForUpdates)
      .mockResolvedValueOnce(availableUpdate('1.2.4'))
      .mockImplementationOnce(async () => {
        await feed.promise
        return availableUpdate('1.2.5')
      })
    const deps = createDeps(updater)
    const desktopUpdater = createDesktopUpdater(deps)
    await desktopUpdater.check({ interactive: false })

    const first = desktopUpdater.check({ interactive: true })
    await desktopUpdater.download()
    const second = desktopUpdater.check({ interactive: true })
    feed.resolve()
    await Promise.all([first, second])

    expect(deps.infoDialogs).toEqual([
      {
        message: 'Coral 1.2.4 is ready',
        detail: 'The update will install when you quit Coral.',
      },
    ])
  })

  it('offers the update once when two manual checks overlap', async () => {
    const updater = createFakeUpdater()
    const feed = deferredPromise()
    vi.mocked(updater.checkForUpdates).mockImplementationOnce(async () => {
      await feed.promise
      return availableUpdate('1.2.4')
    })
    const deps = createDeps(updater)
    const desktopUpdater = createDesktopUpdater(deps)

    const first = desktopUpdater.check({ interactive: true })
    const second = desktopUpdater.check({ interactive: true })
    feed.resolve()
    await Promise.all([first, second])

    expect(updater.checkForUpdates).toHaveBeenCalledOnce()
    expect(deps.confirmDialogs).toHaveLength(1)
  })

  it('surfaces a failure of the download it just started', async () => {
    const updater = createFakeUpdater()
    vi.mocked(updater.checkForUpdates).mockResolvedValue(availableUpdate('1.2.4'))
    vi.mocked(updater.downloadUpdate).mockRejectedValue(new Error('zip handoff failed'))
    const deps = createDeps(updater)
    deps.confirmAnswer.value = true

    await expect(
      createDesktopUpdater(deps).check({ interactive: true }),
    ).resolves.toBeUndefined()

    expect(deps.errorDialogs).toEqual([
      { message: 'Update download failed', detail: 'zip handoff failed' },
    ])
    expect(deps.notifications).toHaveLength(0)
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

  it('finds an update without prompting, downloading, or notifying', async () => {
    const updater = createFakeUpdater()
    vi.mocked(updater.checkForUpdates).mockResolvedValue(availableUpdate())
    const deps = createDeps(updater)
    const desktopUpdater = createDesktopUpdater(deps)
    await desktopUpdater.check({ interactive: false })

    expect(deps.infoDialogs).toHaveLength(0)
    expect(deps.errorDialogs).toHaveLength(0)
    expect(deps.confirmDialogs).toHaveLength(0)
    expect(deps.notifications).toHaveLength(0)
    expect(updater.downloadUpdate).not.toHaveBeenCalled()
    expect(desktopUpdater.getUpdateState()).toEqual({ status: 'available', version: '1.2.4' })
  })
})
