import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'

import {
  createBackground,
  readToldStillRunning,
  writeToldStillRunning,
  type LoginItem,
} from './background'

/** A login item the operating system keeps, which can refuse a change. */
class FakeLoginItem implements LoginItem {
  registered = false
  failure: Error | null = null
  enabled() {
    return this.registered
  }
  set(enabled: boolean) {
    if (this.failure) throw this.failure
    this.registered = enabled
  }
}

/** Records what Desktop tells the person and what it keeps. */
function setup({
  told = false,
  canStartAtLogin = true,
}: { told?: boolean; canStartAtLogin?: boolean } = {}) {
  const loginItem = canStartAtLogin ? new FakeLoginItem() : undefined
  const logged: string[] = []
  const calls = { told: 0, saved: 0, changed: 0 }
  const failures = { save: null as Error | null }
  const background = createBackground({
    told,
    saveTold: async () => {
      if (failures.save) throw failures.save
      calls.saved += 1
    },
    tellStillRunning: async () => {
      calls.told += 1
    },
    loginItem,
    log: (message) => logged.push(message),
    onChange: () => (calls.changed += 1),
  })
  const checkbox = (label: string) => {
    const found = background.settings().find((item) => item.label === label)
    if (!found) throw new Error(`No "${label}" checkbox`)
    return found
  }
  return { background, logged, calls, failures, loginItem, checkbox }
}

const LOGIN = 'Start at login'

describe('when the last window closes', () => {
  it('tells the person once that Coral keeps running', async () => {
    const { background, calls } = setup()

    await background.lastWindowClosed()
    await background.lastWindowClosed()

    expect(calls).toMatchObject({ told: 1, saved: 1 })
  })

  it('says nothing when an earlier launch told the person', async () => {
    const { background, calls } = setup({ told: true })

    await background.lastWindowClosed()

    expect(calls).toMatchObject({ told: 0, saved: 0 })
  })

  it('shows one notice when a second close arrives while it is open', async () => {
    const { background, calls } = setup()

    await Promise.all([background.lastWindowClosed(), background.lastWindowClosed()])

    expect(calls.told).toBe(1)
  })

  it('does not repeat the notice in this launch when it cannot be kept', async () => {
    const { background, failures, logged, calls } = setup()
    failures.save = new Error('disk full')

    await background.lastWindowClosed()
    await background.lastWindowClosed()

    expect(calls.told).toBe(1)
    expect(logged).toEqual([expect.stringContaining('disk full')])
  })
})

describe('the settings', () => {
  it('registers and removes the login item', () => {
    const { loginItem, calls, checkbox } = setup()
    expect(checkbox(LOGIN).checked).toBe(false)

    checkbox(LOGIN).click()
    expect(loginItem!.registered).toBe(true)
    expect(checkbox(LOGIN).checked).toBe(true)

    checkbox(LOGIN).click()
    expect(loginItem!.registered).toBe(false)
    expect(calls.changed).toBe(2)
  })

  it('shows what the system kept when it refuses the login item', () => {
    const { loginItem, logged, calls, checkbox } = setup()
    loginItem!.failure = new Error('not permitted')

    checkbox(LOGIN).click()

    expect(loginItem!.registered).toBe(false)
    expect(checkbox(LOGIN).checked).toBe(false)
    expect(logged).toEqual([expect.stringContaining('not permitted')])
    // The menu redraws, so a checkbox the system toggled shows the truth.
    expect(calls.changed).toBe(1)
  })

  it('offers no login item when this package cannot register one', () => {
    const { background } = setup({ canStartAtLogin: false })

    expect(background.settings()).toEqual([])
  })
})

describe('the notice kept in desktop.json', () => {
  it.each([
    ['is missing', undefined, false],
    ['is not JSON', '{ not json', false],
    ['holds another type', '{ "toldStillRunning": "yes" }', false],
    ['says so', '{ "toldStillRunning": true }', true],
    // 1.0.0-beta.8 kept a setting. Off made the window's close quit Coral,
    // so that person must learn that it no longer does.
    ['holds the earlier setting, on', '{ "keepRunning": true }', true],
    ['holds the earlier setting, off', '{ "keepRunning": false }', false],
  ])('when the file %s', async (_name, content, told) => {
    const dir = await mkdtemp(join(tmpdir(), 'coral-background-'))
    try {
      const file = join(dir, 'desktop.json')
      if (content !== undefined) await writeFile(file, content)

      expect(await readToldStillRunning(file)).toBe(told)
    } finally {
      await rm(dir, { recursive: true, force: true })
    }
  })

  it('reads back what it wrote', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'coral-background-'))
    try {
      const file = join(dir, 'desktop.json')
      await writeToldStillRunning(file)

      expect(await readToldStillRunning(file)).toBe(true)
    } finally {
      await rm(dir, { recursive: true, force: true })
    }
  })
})
