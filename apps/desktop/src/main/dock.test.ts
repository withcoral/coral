import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { createDockPresence, DOCK_SETTLE_MS, type Dock } from './dock'

/** A Dock that, like macOS, drops a call that follows another within a second. */
class FakeDock implements Dock {
  visible = true
  failure: Error | null = null
  private calledAt = -Infinity
  private settled() {
    const settled = Date.now() - this.calledAt >= 1000
    this.calledAt = Date.now()
    return settled
  }
  async show() {
    if (this.failure) throw this.failure
    if (this.settled()) this.visible = true
  }
  hide() {
    if (this.settled()) this.visible = false
  }
}

function setup({ hasDock = true }: { hasDock?: boolean } = {}) {
  const dock = new FakeDock()
  const state = { wanted: true }
  const logged: string[] = []
  const sync = createDockPresence({
    dock: hasDock ? dock : undefined,
    wanted: () => state.wanted,
    log: (message) => logged.push(message),
  })
  const want = (wanted: boolean) => {
    state.wanted = wanted
    sync()
  }
  return { dock, want, logged }
}

beforeEach(() => void vi.useFakeTimers())
afterEach(() => void vi.useRealTimers())

describe('Coral in the Dock', () => {
  it('leaves the Dock when it is not wanted there, and returns', async () => {
    const { dock, want } = setup()

    want(false)
    expect(dock.visible).toBe(false)

    await vi.advanceTimersByTimeAsync(DOCK_SETTLE_MS)
    want(true)
    expect(dock.visible).toBe(true)
  })

  // A window that opens and closes within a second: macOS drops the hide.
  it.each([
    ['leaves', [true, false], false],
    ['stays when the window is back by then', [true, false, true], true],
  ] as const)('%s after a change that came too soon', async (_name, changes, visible) => {
    const { dock, want } = setup()
    want(false)
    await vi.advanceTimersByTimeAsync(DOCK_SETTLE_MS)

    for (const wanted of changes) want(wanted)
    await vi.advanceTimersByTimeAsync(DOCK_SETTLE_MS)

    expect(dock.visible).toBe(visible)
  })

  it('logs a Dock that does not show, and tries again', async () => {
    const { dock, want, logged } = setup()
    want(false)
    await vi.advanceTimersByTimeAsync(DOCK_SETTLE_MS)
    dock.failure = new Error('no Dock')

    want(true)
    await vi.advanceTimersByTimeAsync(0)

    expect(logged).toEqual(['dock: Error: no Dock'])
    expect(dock.visible).toBe(false)

    dock.failure = null
    await vi.advanceTimersByTimeAsync(DOCK_SETTLE_MS)
    want(true)
    expect(dock.visible).toBe(true)
  })

  it('does nothing where there is no Dock', () => {
    const { dock, want } = setup({ hasDock: false })

    want(false)

    expect(dock.visible).toBe(true)
    expect(vi.getTimerCount()).toBe(0)
  })
})
