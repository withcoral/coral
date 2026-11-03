import { describe, expect, it } from 'vitest'

import { createUpdates, type UpdatePrompts, type UpdaterLike } from './updates'

/**
 * A stand-in for electron-updater: each check answers with `latest`, and a
 * download fetches whatever the last check found, as electron-updater does.
 * `hold` stops a step until `release()`; `held()` resolves once it waits.
 */
class FakeUpdater implements UpdaterLike {
  latest = '1.0.0'
  checkFailure: Error | null = null
  downloadFailure: Error | null = null
  downloaded: string[] = []
  hold: 'check' | 'download' | null = null
  release = () => {}
  private found = '1.0.0'
  private reached = () => {}
  private released = new Promise<void>((resolve) => (this.release = resolve))
  private waiting = new Promise<void>((resolve) => (this.reached = resolve))

  held() {
    return this.waiting
  }

  async checkForUpdates() {
    await this.gate('check')
    if (this.checkFailure) throw this.checkFailure
    this.found = this.latest
    return { isUpdateAvailable: this.latest !== '1.0.0', updateInfo: { version: this.latest } }
  }

  async downloadUpdate() {
    await this.gate('download')
    if (this.downloadFailure) throw this.downloadFailure
    this.downloaded.push(this.found)
    return []
  }

  private async gate(step: 'check' | 'download') {
    if (this.hold !== step) return
    this.reached()
    await this.released
  }
}

/** Records every prompt, and answers the restart question with `restart`. */
function setup({ restart = false } = {}) {
  const updater = new FakeUpdater()
  const shown: string[] = []
  const notifications: (() => void)[] = []
  const installs: string[] = []
  const menu: string[] = []
  // What ready() answers after each change, without repeats.
  const ready: (string | null)[] = []
  const failures = { install: null as Error | null }
  const progress: (number | null)[] = []
  const prompts: UpdatePrompts = {
    upToDate: async (version) => void shown.push(`up to date ${version}`),
    notReady: async (version) => void shown.push(`not ready ${version}`),
    offerInstall: async (version) => (shown.push(`offer restart ${version}`), restart),
    failed: async (step) => void shown.push(`failed ${step}`),
    notifyReady: (version, open) => {
      shown.push(`ready ${version}`)
      notifications.push(open)
    },
  }
  const updates = createUpdates({
    updater,
    currentVersion: '1.0.0',
    prompts,
    install: async (version) => {
      installs.push(version)
      if (failures.install) throw failures.install
    },
    onChange: () => {
      menu.push(updates.menuItem().label)
      if (ready.at(-1) !== updates.ready() || !ready.length) ready.push(updates.ready())
    },
    progress: (fraction) => void progress.push(fraction),
  })
  return { updater, updates, shown, notifications, installs, menu, ready, failures, progress }
}

describe('a check Coral runs by itself', () => {
  it('downloads what it finds without asking, then says once that it is ready', async () => {
    const { updater, updates, shown, installs } = setup()
    updater.latest = '1.1.0'

    await updates.check({ interactive: false })
    await updates.check({ interactive: false })

    expect(updater.downloaded).toEqual(['1.1.0'])
    expect(shown).toEqual(['ready 1.1.0'])
    expect(installs).toEqual([])
    expect(updates.menuItem()).toMatchObject({
      label: 'Restart to Update Coral 1.1.0',
      enabled: true,
    })
  })
})

describe('a ready update', () => {
  it('is ready only between its download and its install', async () => {
    const s = setup({ restart: true })
    s.updater.latest = '1.1.0'
    s.failures.install = new Error('installer refused')

    await s.updates.check({ interactive: true })

    // Downloaded, then installing, then ready again after the failure.
    expect(s.ready).toEqual([null, '1.1.0', null, '1.1.0'])
  })

  it('installs when the person opens the notification', async () => {
    const { updater, updates, notifications, installs } = setup()
    updater.latest = '1.1.0'
    await updates.check({ interactive: false })

    notifications[0]()
    await Promise.resolve()

    expect(installs).toEqual(['1.1.0'])
  })
})

describe('a check the person asked for', () => {
  it('says Coral is up to date when the feed has nothing newer', async () => {
    const { updates, shown, updater } = setup()

    await updates.check({ interactive: true })

    expect(shown).toEqual(['up to date 1.0.0'])
    expect(updater.downloaded).toEqual([])
    expect(updates.menuItem()).toMatchObject({ label: 'Check for Updates…', enabled: true })
  })

  it.each([
    { restart: true, installs: ['1.1.0'] },
    { restart: false, installs: [] },
  ])(
    'downloads, then asks to restart, and installs only on yes (restart: $restart)',
    async ({ restart, installs }) => {
      const s = setup({ restart })
      s.updater.latest = '1.1.0'

      await s.updates.check({ interactive: true })

      expect(s.updater.downloaded).toEqual(['1.1.0'])
      expect(s.shown).toEqual(['offer restart 1.1.0'])
      expect(s.installs).toEqual(installs)
    },
  )
})

describe('a click on the sidebar notice', () => {
  it.each([
    { before: 'idle', latest: '1.1.0', fail: null, shown: ['offer restart 1.1.0'] },
    { before: 'idle', latest: '1.0.0', fail: null, shown: ['not ready 1.1.0'] },
    // A check Coral runs by itself ends in the question, not the notification.
    { before: 'check', latest: '1.1.0', fail: null, shown: ['offer restart 1.1.0'] },
    { before: 'check', latest: '1.0.0', fail: null, shown: ['not ready 1.1.0'] },
    { before: 'download', latest: '1.1.0', fail: null, shown: ['offer restart 1.1.0'] },
    {
      before: 'downloaded',
      latest: '1.1.0',
      fail: null,
      shown: ['ready 1.1.0', 'offer restart 1.1.0'],
    },
    { before: 'idle', latest: '1.1.0', fail: 'check', shown: ['failed check'] },
    { before: 'idle', latest: '1.1.0', fail: 'download', shown: ['failed download'] },
  ] as const)(
    'while $before, with $latest on the feed, shows $shown',
    async ({ before, latest, fail, shown }) => {
      const s = setup({ restart: true })
      s.updater.latest = latest
      if (fail) s.updater[`${fail}Failure`] = new Error('offline')
      let background: Promise<void> = Promise.resolve()
      if (before === 'downloaded') await s.updates.check({ interactive: false })
      if (before === 'check' || before === 'download') {
        s.updater.hold = before
        background = s.updates.check({ interactive: false })
        await s.updater.held()
      }

      const click = s.updates.offerRelease('1.1.0')
      s.updater.release()
      await Promise.all([click, background])

      expect(s.shown).toEqual(shown)
      expect(s.installs).toEqual(shown.at(-1) === 'offer restart 1.1.0' ? ['1.1.0'] : [])
    },
  )

  it('asks only once when the person clicks again while the question is open', async () => {
    const s = setup()
    s.updater.latest = '1.1.0'
    await s.updates.check({ interactive: false })

    await Promise.all([s.updates.offerRelease('1.1.0'), s.updates.offerRelease('1.1.0')])

    expect(s.shown).toEqual(['ready 1.1.0', 'offer restart 1.1.0'])
  })
})

describe('the download progress', () => {
  it.each([
    { downloadFailure: null, progress: [0, null] },
    { downloadFailure: new Error('offline'), progress: [0, null] },
  ])('starts with the download and clears when it ends (failure: $downloadFailure)', async (c) => {
    const s = setup()
    s.updater.latest = '1.1.0'
    s.updater.downloadFailure = c.downloadFailure

    await s.updates.check({ interactive: false })

    expect(s.progress).toEqual(c.progress)
  })
})

describe('a step that fails', () => {
  it.each([
    {
      step: 'check',
      fail: (s: Setup) => (s.updater.checkFailure = new Error('offline')),
      label: 'Check for Updates…',
    },
    {
      step: 'download',
      fail: (s: Setup) => (s.updater.downloadFailure = new Error('offline')),
      label: 'Check for Updates…',
    },
    {
      step: 'install',
      fail: (s: Setup) => (s.failures.install = new Error('installer refused')),
      label: 'Restart to Update Coral 1.1.0',
    },
  ])('reports the $step to the person who asked, and offers it again', async (c) => {
    const s = setup({ restart: true })
    s.updater.latest = '1.1.0'
    c.fail(s)

    await s.updates.check({ interactive: true })

    expect(s.shown.at(-1)).toBe(`failed ${c.step}`)
    expect(s.updates.menuItem()).toMatchObject({ label: c.label, enabled: true })
  })

  it.each([
    { step: 'check', fail: (s: Setup) => (s.updater.checkFailure = new Error('offline')) },
    { step: 'download', fail: (s: Setup) => (s.updater.downloadFailure = new Error('offline')) },
  ])('stays quiet when a check Coral runs by itself fails to $step', async (c) => {
    const s = setup()
    s.updater.latest = '1.1.0'
    c.fail(s)

    await s.updates.check({ interactive: false })

    expect(s.shown).toEqual([])
    expect(s.updates.menuItem()).toMatchObject({ label: 'Check for Updates…', enabled: true })
  })
})

describe('the menu', () => {
  it('follows each step, and disables itself while one is under way', async () => {
    const { updater, updates, menu } = setup({ restart: true })
    updater.latest = '1.1.0'

    await updates.check({ interactive: true })

    expect(menu).toEqual([
      'Checking for Updates…',
      'Downloading Coral 1.1.0…',
      'Restart to Update Coral 1.1.0',
      'Installing Coral 1.1.0…',
    ])
  })
})

type Setup = ReturnType<typeof setup>
