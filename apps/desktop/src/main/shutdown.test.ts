import { describe, expect, it, vi } from 'vitest'

import { createShutdownCoordinator } from './shutdown'

function deferredPromise(): {
  promise: Promise<void>
  resolve: () => void
} {
  let resolve!: () => void
  const promise = new Promise<void>((resolvePromise) => {
    resolve = resolvePromise
  })
  return { promise, resolve }
}

function quitEvent() {
  return { preventDefault: vi.fn() }
}

describe('shutdown coordinator', () => {
  it('finishes service shutdown before recording and handing off an update', async () => {
    const events: string[] = []
    const stopped = deferredPromise()
    const coordinator = createShutdownCoordinator({
      stopServices: () => {
        events.push('sidecar:stop:start')
        return stopped.promise.then(() => {
          events.push('sidecar:stop:end')
        })
      },
      installReadyUpdate: () => {
        events.push('intent:write:1.2.4')
        events.push('updater:quitAndInstall')
        return true
      },
      quit: () => {
        events.push('app:quit')
      },
    })
    const event = quitEvent()

    coordinator.beforeQuit(event)
    expect(event.preventDefault).toHaveBeenCalledOnce()
    expect(events).toEqual(['sidecar:stop:start'])

    stopped.resolve()
    await stopped.promise
    await Promise.resolve()
    expect(events).toEqual([
      'sidecar:stop:start',
      'sidecar:stop:end',
      'intent:write:1.2.4',
      'updater:quitAndInstall',
    ])
  })

  it('suppresses duplicate quit requests while services are stopping', async () => {
    const stopped = deferredPromise()
    const stopServices = vi.fn(() => stopped.promise)
    const installReadyUpdate = vi.fn(() => false)
    const quit = vi.fn()
    const coordinator = createShutdownCoordinator({
      stopServices,
      installReadyUpdate,
      quit,
    })
    const firstEvent = quitEvent()
    const secondEvent = quitEvent()

    coordinator.beforeQuit(firstEvent)
    coordinator.beforeQuit(secondEvent)

    expect(firstEvent.preventDefault).toHaveBeenCalledOnce()
    expect(secondEvent.preventDefault).toHaveBeenCalledOnce()
    expect(stopServices).toHaveBeenCalledOnce()
    expect(installReadyUpdate).not.toHaveBeenCalled()

    stopped.resolve()
    await stopped.promise
    await Promise.resolve()
    expect(quit).toHaveBeenCalledOnce()
  })

  it('blocks manual quits during hand-off until Electron begins its update quit', async () => {
    const coordinator = createShutdownCoordinator({
      stopServices: async () => {},
      installReadyUpdate: () => true,
      quit: vi.fn(),
    })

    coordinator.beforeQuit(quitEvent())
    await Promise.resolve()

    const manualQuitEvent = quitEvent()
    coordinator.beforeQuit(manualQuitEvent)
    expect(manualQuitEvent.preventDefault).toHaveBeenCalledOnce()

    coordinator.allowQuit()
    const updateQuitEvent = quitEvent()
    coordinator.beforeQuit(updateQuitEvent)
    expect(updateQuitEvent.preventDefault).not.toHaveBeenCalled()
  })

  it('allows an ordinary quit after the updater hand-off fails', async () => {
    const quit = vi.fn()
    const coordinator = createShutdownCoordinator({
      stopServices: async () => {},
      installReadyUpdate: () => true,
      quit,
    })

    coordinator.beforeQuit(quitEvent())
    await Promise.resolve()
    coordinator.quitAfterUpdateFailure()

    expect(quit).toHaveBeenCalledOnce()
    const failureQuitEvent = quitEvent()
    coordinator.beforeQuit(failureQuitEvent)
    expect(failureQuitEvent.preventDefault).not.toHaveBeenCalled()
  })

  it('still checks for a ready update when there are no active services', async () => {
    const installReadyUpdate = vi.fn(() => true)
    const quit = vi.fn()
    const coordinator = createShutdownCoordinator({
      stopServices: async () => {},
      installReadyUpdate,
      quit,
    })

    coordinator.beforeQuit(quitEvent())
    await Promise.resolve()

    expect(installReadyUpdate).toHaveBeenCalledOnce()
    expect(quit).not.toHaveBeenCalled()
  })

  it('falls back to an ordinary quit when no update is ready', async () => {
    const quit = vi.fn()
    const coordinator = createShutdownCoordinator({
      stopServices: async () => {},
      installReadyUpdate: () => false,
      quit,
    })

    coordinator.beforeQuit(quitEvent())
    await Promise.resolve()

    expect(quit).toHaveBeenCalledOnce()
  })
})
