import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { createServer, type Server } from 'node:net'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { spawnLauncher, type Launcher, type ProcessExit } from './process'
import { runtimeLauncher } from './runtime'
import {
  createSupervisor,
  SupervisorError,
  type Role,
  type Supervisor,
  type SupervisorState,
} from './supervisor'

const stubs = resolve(import.meta.dirname, '..', '..', 'test')

let dataDir: string
let supervisor: Supervisor | undefined
let launched: { role: Role; exit?: ProcessExit }[]
let states: SupervisorState[]
const occupied: Server[] = []

beforeEach(async () => {
  dataDir = await mkdtemp(join(tmpdir(), 'coral-desktop-supervisor-'))
  launched = []
  states = []
})

afterEach(async () => {
  await supervisor?.stop()
  supervisor = undefined
  await Promise.all(occupied.splice(0).map((server) => new Promise((done) => server.close(done))))
  await rm(dataDir, { recursive: true, force: true })
})

function stub(role: Role, mode: string): Launcher {
  // The runtime stub runs under Desktop's own runtime launcher, so tests cover
  // how Desktop stops it.
  const options = {
    args: [join(stubs, `stub-${role}.mjs`), mode],
    cwd: dataDir,
    baseEnv: process.env,
  }
  const launch =
    role === 'runtime'
      ? runtimeLauncher(process.execPath, options)
      : spawnLauncher(process.execPath, options)
  return (env) => {
    const child = launch(env)
    const record: (typeof launched)[number] = { role }
    launched.push(record)
    void child.exited.then((exit) => (record.exit = exit))
    return child
  }
}

const RESTART = { attempts: 2, windowMs: 60_000, backoffMs: 50 }

function supervise(runtime = 'ready', reef = 'ready', restart = RESTART) {
  supervisor = createSupervisor({
    dataDir,
    runtime: stub('runtime', runtime),
    reef: stub('reef', reef),
    readinessTimeoutMs: 1500,
    checkUpdates: false,
    stopTimeoutMs: 500,
    restart,
  })
  supervisor.on('state', (state) => states.push(state))
  return supervisor
}

const roles = () => launched.map(({ role }) => role)
const alive = () => launched.filter(({ exit }) => !exit).map(({ role }) => role)
const statuses = () => states.map(({ status }) => status)

async function failureOf(promise: Promise<unknown>) {
  const error = await promise.then(
    () => undefined,
    (reason: unknown) => reason,
  )
  expect(error).toBeInstanceOf(SupervisorError)
  return (error as SupervisorError).failure
}

/** Makes a running stub exit, as a crash would. */
async function crash(origin: string) {
  await fetch(`${origin}/exit`).catch(() => {})
}

async function occupy(port: number) {
  const server = createServer()
  await new Promise<void>((done) => server.listen(port, '127.0.0.1', done))
  occupied.push(server)
}

describe('start', () => {
  it('starts the runtime, then Reef in local mode against it', async () => {
    const endpoints = await supervise().start()

    expect(statuses()).toEqual(['starting', 'running'])
    expect(roles()).toEqual(['runtime', 'reef'])
    expect(alive()).toEqual(['runtime', 'reef'])
    expect((await fetch(`${endpoints.mcp}/readyz`)).status).toBe(200)
    const reef = (await (await fetch(`${endpoints.reef}/env`)).json()) as Record<string, string>
    expect(reef).toMatchObject({
      HOST: '127.0.0.1',
      PORT: new URL(endpoints.reef).port,
      CORAL_WEB_AUTH_MODE: 'disabled',
      CORAL_UPSTREAM_OAUTH_CALLBACK_URL: 'http://127.0.0.1:8779/provider-oauth/callback',
    })
    expect(reef.CORAL_ENDPOINT).toMatch(/^http:\/\/127\.0\.0\.1:\d+$/)
  })

  it('keeps the MCP and Reef ports across launches', async () => {
    const first = await supervise().start()
    await supervisor!.stop()
    const second = await supervise().start()

    expect(second).toEqual(first)
  })

  it.each([
    ['the runtime exits', 'fail', 'ready', { kind: 'exited', process: 'runtime' }, ['runtime']],
    [
      'the runtime never becomes ready',
      'hang',
      'ready',
      { kind: 'timeout', process: 'runtime' },
      ['runtime'],
    ],
    ['Reef exits', 'ready', 'exit', { kind: 'exited', process: 'reef' }, ['runtime', 'reef']],
    [
      'Reef never becomes ready',
      'ready',
      'hang',
      { kind: 'timeout', process: 'reef' },
      ['runtime', 'reef'],
    ],
  ])('fails and stops everything when %s', async (_, runtime, reef, failure, started) => {
    expect(await failureOf(supervise(runtime, reef).start())).toMatchObject(failure)

    expect(statuses()).toEqual(['starting', 'failed'])
    expect(roles()).toEqual(started)
    expect(alive()).toEqual([])
  })

  it('treats a runtime that exits after it is ready, while Reef starts, as a crash', async () => {
    const failure = await failureOf(supervise('leave').start())

    expect(failure).toMatchObject({ kind: 'crashed', process: 'runtime', exit: { code: 8 } })
    expect(states).not.toContainEqual(expect.objectContaining({ status: 'running' }))
    // The stub leaves on every launch, so the restarts run out.
    await expect
      .poll(() => states.at(-1), { timeout: 5000 })
      .toMatchObject({ status: 'failed', restarting: false })
    expect(roles().filter((role) => role === 'runtime')).toHaveLength(RESTART.attempts + 1)
    // The failure shows at once; stopping Reef finishes after it.
    await expect.poll(alive).toEqual([])
  })

  it('reports what a process wrote before it exited', async () => {
    const failure = await failureOf(supervise('fail').start())

    expect(failure).toMatchObject({ kind: 'exited', exit: { code: 3 } })
    expect(failure.kind === 'exited' && failure.output).toContain('stub runtime refused to start')
  })

  it('refuses a kept port that another process holds, and launches nothing', async () => {
    const { mcp } = await supervise().start()
    await supervisor!.stop()
    const port = Number(new URL(mcp).port)
    await occupy(port)
    launched = []
    states = []

    const failure = await failureOf(supervise().start())

    expect(failure).toEqual({ kind: 'port-in-use', listener: 'mcp', port })
    expect(launched).toEqual([])
  })

  it('fails, and launches nothing, when its port file is unreadable', async () => {
    await writeFile(join(dataDir, 'desktop.json'), '{ not json')

    expect(await failureOf(supervise().start())).toMatchObject({ kind: 'error' })
    expect(statuses()).toEqual(['starting', 'failed'])
    expect(launched).toEqual([])
  })

  it('moves only the replaced listener to a new port, and keeps it', async () => {
    const first = await supervise().start()
    await supervisor!.stop()
    await occupy(Number(new URL(first.mcp).port))

    const replaced = await supervise().start({ replacePort: 'mcp' })
    await supervisor!.stop()
    const again = await supervise().start()

    expect(replaced.reef).toBe(first.reef)
    expect(replaced.mcp).not.toBe(first.mcp)
    expect(again).toEqual(replaced)
  })

  it('shares one start between concurrent callers', async () => {
    const supervisor = supervise()
    const [first, second] = await Promise.all([supervisor.start(), supervisor.start()])

    expect(second).toEqual(first)
    expect(roles()).toEqual(['runtime', 'reef'])
  })
})

describe('stop', () => {
  it('stops Reef, then lets the runtime drain by closing its stdin', async () => {
    await supervise().start()
    await supervisor!.stop()

    expect(statuses()).toEqual(['starting', 'running', 'stopped'])
    expect(launched).toEqual([
      { role: 'runtime', exit: { code: 0, signal: null } },
      // Windows has no signal handlers, so stopping Reef there ends it at once.
      {
        role: 'reef',
        exit:
          process.platform === 'win32'
            ? { code: null, signal: 'SIGTERM' }
            : { code: 0, signal: null },
      },
    ])
  })

  it('kills a runtime that keeps running after its stdin closes', async () => {
    await supervise('stubborn').start()
    const started = Date.now()
    await supervisor!.stop()

    expect(Date.now() - started).toBeGreaterThanOrEqual(500)
    expect(launched[0]).toEqual({ role: 'runtime', exit: { code: null, signal: 'SIGKILL' } })
    expect(alive()).toEqual([])
  })

  it('cancels an automatic restart', async () => {
    const { mcp } = await supervise().start()
    await crash(mcp)
    await expect.poll(statuses).toContain('failed')
    await supervisor!.stop()
    await delay(RESTART.backoffMs * 4)

    expect(statuses().at(-1)).toBe('stopped')
    expect(roles()).toEqual(['runtime', 'reef'])
    expect(alive()).toEqual([])
  })

  it('cancels a start in progress', async () => {
    const outcome = supervise('hang')
      .start()
      .then(
        () => 'started',
        () => 'rejected',
      )
    await expect.poll(alive).toEqual(['runtime'])
    await supervisor!.stop()

    expect(await outcome).toBe('rejected')
    expect(statuses()).toEqual(['starting', 'stopped'])
    expect(alive()).toEqual([])
  })
})

describe('after start', () => {
  it.each([
    ['the runtime exits', ['mcp'], ['runtime', 'reef', 'runtime', 'reef']],
    ['Reef exits', ['reef'], ['runtime', 'reef', 'reef']],
    ['both exit at once', ['mcp', 'reef'], ['runtime', 'reef', 'runtime', 'reef']],
  ] as const)(
    'restarts what it needs at the same addresses when %s',
    async (_, crashed, relaunched) => {
      const endpoints = await supervise().start()
      await Promise.all(crashed.map((listener) => crash(endpoints[listener])))

      // One restart, whichever exit Desktop sees first.
      await expect.poll(() => statuses().filter((status) => status === 'starting')).toHaveLength(2)
      await expect.poll(() => states.at(-1)).toEqual({ status: 'running', endpoints })
      expect(states[2]).toMatchObject({ failure: { kind: 'crashed' }, restarting: true })
      expect(states).not.toContainEqual(expect.objectContaining({ restarting: false }))
      expect(roles()).toEqual(relaunched)
      expect(alive()).toEqual(['runtime', 'reef'])
    },
  )

  it('restarts the runtime too when it exits while only Reef restarts', async () => {
    const endpoints = await supervise().start()
    await crash(endpoints.reef)
    await expect.poll(statuses).toContain('failed')
    await crash(endpoints.mcp)

    await expect.poll(() => roles().filter((role) => role === 'runtime')).toHaveLength(2)
    await expect.poll(() => states.at(-1)).toEqual({ status: 'running', endpoints })
    expect(states).not.toContainEqual(expect.objectContaining({ restarting: false }))
    expect(alive()).toEqual(['runtime', 'reef'])
  })

  it.each([
    ['times out', 'stub.hang', { kind: 'timeout', process: 'runtime' }, true],
    ['refuses to start', 'stub.refuse', { kind: 'exited', process: 'runtime' }, false],
  ] as const)(
    'after a crash, retries a relaunch that %s only if it did not refuse',
    async (_, marker, failure, retried) => {
      const endpoints = await supervise().start()
      const markerPath = join(dataDir, 'runtime', marker)
      await writeFile(markerPath, '')
      await crash(endpoints.mcp)

      const relaunchFailure = () =>
        states.find((state) => state.status === 'failed' && state.failure.kind !== 'crashed')
      await expect.poll(relaunchFailure, { timeout: 5000 }).toMatchObject({
        failure,
        restarting: retried,
      })
      await rm(markerPath)
      if (retried) {
        await expect
          .poll(() => states.at(-1), { timeout: 5000 })
          .toEqual({ status: 'running', endpoints })
        expect(alive()).toEqual(['runtime', 'reef'])
      } else {
        await delay(RESTART.backoffMs * 4)
        expect(states.at(-1)).toMatchObject({ status: 'failed', restarting: false })
        expect(alive()).toEqual([])
      }
    },
  )

  it('starts afresh when asked while a restart waits', async () => {
    const endpoints = await supervise('ready', 'ready', { ...RESTART, backoffMs: 60_000 }).start()
    await crash(endpoints.reef)
    await expect.poll(statuses).toContain('failed')

    expect(await supervisor!.start()).toEqual(endpoints)
    expect(roles()).toEqual(['runtime', 'reef', 'runtime', 'reef'])
    expect(alive()).toEqual(['runtime', 'reef'])
  })

  it('stops restarting after repeated crashes and waits for a start', async () => {
    const supervisor = supervise()
    let endpoints = await supervisor.start()
    for (let crashes = 0; crashes < RESTART.attempts; crashes++) {
      await crash(endpoints.mcp)
      await expect
        .poll(() => states.filter((state) => state.status === 'running').length)
        .toBe(crashes + 2)
    }
    await crash(endpoints.mcp)

    await expect.poll(alive).toEqual([])
    expect(states.at(-1)).toMatchObject({
      status: 'failed',
      failure: { kind: 'crashed', process: 'runtime' },
      restarting: false,
    })
    await delay(RESTART.backoffMs * 4)
    expect(roles()).toHaveLength(2 * (RESTART.attempts + 1))

    endpoints = await supervisor.start()
    expect(alive()).toEqual(['runtime', 'reef'])
  })
})

describe('a state directory another runtime holds', () => {
  const lock = () => join(dataDir, 'runtime', 'stub.lock')
  beforeEach(async () => {
    await mkdir(dirname(lock()), { recursive: true })
    await writeFile(lock(), '')
  })

  it('retries the runtime until the directory is free', async () => {
    const started = supervise().start()
    await expect.poll(() => roles().length).toBeGreaterThanOrEqual(2)
    await rm(lock())

    await started
    expect(roles().filter((role) => role === 'runtime').length).toBeGreaterThanOrEqual(2)
    expect(statuses()).toEqual(['starting', 'running'])
    expect(alive()).toEqual(['runtime', 'reef'])
  })

  it('fails as locked when the directory stays held', async () => {
    expect(await failureOf(supervise().start())).toMatchObject({ kind: 'state-locked' })
    expect(roles().every((role) => role === 'runtime')).toBe(true)
    expect(alive()).toEqual([])
  })
})

it('regenerates the runtime configuration on every launch', async () => {
  const config = join(dataDir, 'runtime', 'config.toml')
  await supervise().start()
  await supervisor!.stop()
  await writeFile(config, 'edited = true\n')

  await supervise().start()

  expect(await readFile(config, 'utf8')).toMatch(/^# Generated by Coral Desktop/)
})

describe('database', () => {
  const url = 'postgres://coral:secret@127.0.0.1:5433/coral'

  async function runtimeLaunch(mcp: string) {
    const response = await fetch(`${mcp}/launch`)
    return (await response.json()) as { config: string; env: Record<string, string> }
  }

  it('starts the runtime on the Postgres database that desktop.json names', async () => {
    await writeFile(
      join(dataDir, 'desktop.json'),
      JSON.stringify({ database: { backend: 'postgres', url } }),
    )

    const { config, env } = await runtimeLaunch((await supervise().start()).mcp)

    expect(config).toContain(
      '\n[database]\nbackend = "postgres"\nurl_env = "CORAL_DESKTOP_DATABASE_URL"\n',
    )
    expect(config).not.toContain('secret')
    expect(env.CORAL_DESKTOP_DATABASE_URL).toBe(url)
  })

  it('leaves the runtime on its SQLite file when desktop.json names no database', async () => {
    const { config, env } = await runtimeLaunch((await supervise().start()).mcp)

    expect(config).not.toContain('[database]')
    expect(env).not.toHaveProperty('CORAL_DESKTOP_DATABASE_URL')
  })

  it('fails, and launches nothing, when the database section is invalid', async () => {
    await writeFile(
      join(dataDir, 'desktop.json'),
      JSON.stringify({ database: { backend: 'postgres', url: 'mysql://coral:secret@db/coral' } }),
    )

    const failure = await failureOf(supervise().start())

    expect(failure).toMatchObject({
      kind: 'error',
      message: expect.stringContaining('database.url'),
    })
    expect(JSON.stringify(failure)).not.toContain('secret')
    expect(launched).toEqual([])
  })
})
