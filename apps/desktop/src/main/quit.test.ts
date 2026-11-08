import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { createQuitLifecycle, type QuitLifecycle, type StartOptions } from './quit'

/** A stop or a start of Coral that the test settles by hand. */
class Pending {
  readonly promise: Promise<void>
  resolve!: () => void
  reject!: (error: unknown) => void
  constructor() {
    this.promise = new Promise<void>((resolve, reject) => {
      this.resolve = resolve
      this.reject = reject
    })
  }
}

/** Lets settled promises run their callbacks. */
const settle = () => new Promise<void>((resolve) => setTimeout(resolve, 0))

/**
 * Drives the lifecycle as Electron does: quitting the app emits before-quit,
 * and the app ends unless a listener prevents it.
 */
function setup(platform: NodeJS.Platform = 'darwin') {
  const stops: Pending[] = []
  const starts: { pending: Pending; options: StartOptions | undefined }[] = []
  const calls = { exit: 0, closeWindows: 0 }
  const startFailures: unknown[] = []
  const logged: string[] = []
  const app = { ended: false }

  /** app.quit() from anywhere: the Dock, Command+Q, the installer. */
  const quitApp = () => {
    let prevented = false
    quit.beforeQuit({ preventDefault: () => (prevented = true) })
    if (!prevented) app.ended = true
    return prevented
  }
  const quit: QuitLifecycle = createQuitLifecycle({
    platform,
    stopCoral: () => {
      const pending = new Pending()
      stops.push(pending)
      return pending.promise
    },
    startCoral: (options) => {
      const pending = new Pending()
      starts.push({ pending, options })
      return pending.promise
    },
    closeWindows: () => void (calls.closeWindows += 1),
    exit: () => {
      calls.exit += 1
      quitApp()
    },
    onStartFailure: (error) => void startFailures.push(error),
    log: (message) => void logged.push(message),
  })
  /** Settles every stop so far. */
  const stopAll = async () => {
    for (const stop of stops) stop.resolve()
    await settle()
  }
  return { quit, quitApp, app, stops, starts, calls, startFailures, logged, stopAll }
}

let unhandled: unknown[] = []
const onUnhandled = (reason: unknown) => void unhandled.push(reason)
beforeEach(() => {
  unhandled = []
  process.on('unhandledRejection', onUnhandled)
})
afterEach(() => void process.off('unhandledRejection', onUnhandled))

describe('a quit', () => {
  it('on macOS, from the Dock or Command+Q, only closes the windows', async () => {
    const { quit, quitApp, app, stops, calls } = setup('darwin')

    expect(quitApp()).toBe(true)
    await settle()

    expect(calls.closeWindows).toBe(1)
    expect(stops).toHaveLength(0)
    expect(app.ended).toBe(false)
    expect(quit.quitting()).toBe(false)
  })

  it.each([
    ['on macOS, Quit Completely', 'darwin', (quit: QuitLifecycle) => quit.quitCompletely()],
    [
      'on macOS, a quit as the session ends',
      'darwin',
      (quit: QuitLifecycle, quitApp: () => void) => {
        quit.sessionEnding()
        quitApp()
      },
    ],
    ['on Linux, a quit', 'linux', (_quit: QuitLifecycle, quitApp: () => void) => quitApp()],
    ['on Windows, a quit', 'win32', (_quit: QuitLifecycle, quitApp: () => void) => quitApp()],
  ] as const)('%s stops Coral, then ends the app', async (_name, platform, act) => {
    const { quit, quitApp, app, stops, calls } = setup(platform)
    act(quit, quitApp)
    const exits = calls.exit

    expect(stops).toHaveLength(1)
    expect(quit.quitting()).toBe(true)
    expect(app.ended).toBe(false)

    stops[0].resolve()
    await settle()

    expect(calls.exit - exits).toBe(1)
    expect(app.ended).toBe(true)
    expect(stops).toHaveLength(1)
    expect(calls.closeWindows).toBe(0)
  })

  it('stops Coral once when a second quit arrives while it stops', async () => {
    const { quitApp, app, stops, calls, stopAll } = setup('linux')

    quitApp()
    expect(quitApp()).toBe(true)
    await stopAll()

    expect(stops).toHaveLength(1)
    expect(calls.exit).toBe(1)
    expect(app.ended).toBe(true)
  })

  it('ends the app and logs the failure when Coral does not stop cleanly', async () => {
    const { quitApp, app, stops, calls, logged } = setup('linux')

    quitApp()
    stops[0].reject(new Error('reef did not exit'))
    await settle()

    expect(calls.exit).toBe(1)
    expect(app.ended).toBe(true)
    expect(logged).toEqual(['quit: Error: reef did not exit'])
    expect(unhandled).toEqual([])
  })
})

describe('starting Coral', () => {
  it('restarts by stopping, then starting', async () => {
    const { quit, stops, starts, stopAll } = setup()

    quit.restart()
    expect(starts).toHaveLength(0)
    await stopAll()

    expect(stops).toHaveLength(1)
    expect(starts).toHaveLength(1)
  })

  it('starts on a new port when asked', () => {
    const { quit, starts } = setup()

    quit.start({ replacePort: 'mcp' })

    expect(starts.map(({ options }) => options)).toEqual([{ replacePort: 'mcp' }])
  })

  it.each([
    [
      'restart after Quit began',
      (quit: QuitLifecycle, quitApp: () => void) => {
        quitApp()
        quit.restart()
      },
    ],
    [
      'restart when Quit begins while it stops',
      (quit: QuitLifecycle, quitApp: () => void) => {
        quit.restart()
        quitApp()
      },
    ],
    [
      'start after Quit began',
      (quit: QuitLifecycle, quitApp: () => void) => {
        quitApp()
        quit.start({ replacePort: 'reef' })
      },
    ],
  ] as const)('does not %s', async (_name, act) => {
    const { quit, quitApp, starts, stopAll } = setup('linux')

    act(quit, quitApp)
    await stopAll()

    expect(starts).toHaveLength(0)
    expect(quit.quitting()).toBe(true)
  })

  it.each([
    [
      'restart',
      async (quit: QuitLifecycle, stopAll: () => Promise<void>) => {
        quit.restart()
        await stopAll()
      },
    ],
    ['start', async (quit: QuitLifecycle) => quit.start()],
    ['resumeAfterUpdate', async (quit: QuitLifecycle) => quit.resumeAfterUpdate()],
  ] as const)('passes a failed start in %s to onStartFailure', async (_name, act) => {
    const { quit, starts, startFailures, stopAll } = setup()
    const failure = new Error('port taken')

    await act(quit, stopAll)
    starts[0].pending.reject(failure)
    await settle()

    expect(startFailures).toEqual([failure])
    expect(unhandled).toEqual([])
  })
})

describe('an update', () => {
  it("stops Coral, then lets the installer's quit end the app", async () => {
    const { quit, quitApp, app, stops } = setup('darwin')

    const stopping = quit.stopForUpdate()
    expect(quit.quitting()).toBe(true)
    stops[0].resolve()
    await stopping

    expect(quitApp()).toBe(false)
    expect(app.ended).toBe(true)
    expect(stops).toHaveLength(1)
  })

  it('starts Coral again when the hand-off fails', async () => {
    const { quit, quitApp, app, stops, starts, calls } = setup('darwin')
    const stopping = quit.stopForUpdate()
    stops[0].resolve()
    await stopping

    quit.resumeAfterUpdate()

    expect(starts).toHaveLength(1)
    expect(quit.quitting()).toBe(false)
    // A later Dock quit only closes the windows again.
    expect(quitApp()).toBe(true)
    expect(calls.closeWindows).toBe(1)
    expect(app.ended).toBe(false)
  })
})
