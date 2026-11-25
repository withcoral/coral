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
/** Settles as each launched process exits. */
let exits: Promise<ProcessExit>[]
let states: SupervisorState[]
const occupied: Server[] = []

beforeEach(async () => {
  dataDir = await mkdtemp(join(tmpdir(), 'coral-desktop-supervisor-'))
  launched = []
  exits = []
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
    exits.push(child.exited)
    void child.exited.then((exit) => (record.exit = exit))
    return child
  }
}

const RESTART = { attempts: 2, windowMs: 60_000, backoffMs: 50 }
const READINESS_TIMEOUT_MS = 1500
const STOP_TIMEOUT_MS = 500
/** How long the supervisor waits for a process it killed to exit. */
const KILL_GRACE_MS = 1000

/**
 * A timeout for a test that starts Coral `starts` times and waits for the
 * supervisor's reports. Before each start the supervisor may stop both
 * processes, each within the stop timeout and the kill grace, and the start
 * then waits up to the readiness timeout for each process before it reports.
 * The first start stops nothing, which leaves room for the backoffs and waits.
 */
const startsTimeout = (starts: number) =>
  starts * 2 * (STOP_TIMEOUT_MS + KILL_GRACE_MS + READINESS_TIMEOUT_MS)

function supervise(
  runtime = 'ready',
  reef = 'ready',
  { restart = RESTART, stopTimeoutMs = STOP_TIMEOUT_MS } = {},
) {
  supervisor = createSupervisor({
    dataDir,
    runtime: stub('runtime', runtime),
    reef: stub('reef', reef),
    readinessTimeoutMs: READINESS_TIMEOUT_MS,
    checkUpdates: false,
    stopTimeoutMs,
    restart,
  })
  supervisor.on('state', (state) => states.push(state))
  return supervisor
}

/**
 * Resolves with the next state the supervisor reports that `matches`. Call it
 * before the action that leads to that state.
 */
function nextState(
  supervisor: Supervisor,
  matches: (state: SupervisorState) => boolean,
): Promise<SupervisorState> {
  return new Promise((resolve) => {
    const unsubscribe = supervisor.on('state', (state) => {
      if (!matches(state)) return
      unsubscribe()
      resolve(state)
    })
  })
}

const roles = () => launched.map(({ role }) => role)

/**
 * Matches the report a crash's recovery ends with: running once `relaunched`
 * says the restart has launched what it needs, or waiting for a start.
 */
const restartSettled =
  (relaunched: () => boolean = () => true) =>
  (state: SupervisorState) =>
    state.status === 'failed' ? !state.restarting : state.status === 'running' && relaunched()

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

/** The config.toml and the database URL variable that the stub runtime started with. */
async function runtimeLaunch(mcp: string) {
  const response = await fetch(`${mcp}/launch`)
  return (await response.json()) as { config: string; env: Record<string, string> }
}

/** Makes a running stub exit, as a crash would. */
async function crash(origin: string) {
  await fetch(`${origin}/exit`).catch(() => {})
}

/** Creates an empty file in the runtime's state directory, which changes how the stub starts. */
async function mark(name: string) {
  const path = join(dataDir, 'runtime', name)
  await mkdir(dirname(path), { recursive: true })
  await writeFile(path, '')
  return path
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
    { timeout: startsTimeout(2) },
    async (_, crashed, relaunched) => {
      const supervisor = supervise()
      const endpoints = await supervisor.start()
      // A restart that ran before the second exit was seen would launch more.
      const settled = nextState(
        supervisor,
        restartSettled(() => launched.length >= relaunched.length),
      )
      await Promise.all(crashed.map((listener) => crash(endpoints[listener])))

      expect(await settled).toEqual({ status: 'running', endpoints })
      // One restart, whichever exit Desktop sees first.
      expect(statuses().filter((status) => status === 'starting')).toHaveLength(2)
      expect(states[2]).toMatchObject({ failure: { kind: 'crashed' }, restarting: true })
      expect(states).not.toContainEqual(expect.objectContaining({ restarting: false }))
      expect(roles()).toEqual(relaunched)
      expect(alive()).toEqual(['runtime', 'reef'])
    },
  )

  // Reef's restart can run before the runtime's exit is seen, then the runtime's own.
  it(
    'restarts the runtime too when it exits while only Reef restarts',
    { timeout: startsTimeout(3) },
    async () => {
      const supervisor = supervise()
      const endpoints = await supervisor.start()
      const failed = nextState(supervisor, ({ status }) => status === 'failed')
      await crash(endpoints.reef)
      await failed
      const runtimes = () => roles().filter((role) => role === 'runtime').length
      const settled = nextState(
        supervisor,
        restartSettled(() => runtimes() === 2),
      )
      await crash(endpoints.mcp)

      expect(await settled).toEqual({ status: 'running', endpoints })
      expect(states).not.toContainEqual(expect.objectContaining({ restarting: false }))
      expect(alive()).toEqual(['runtime', 'reef'])
    },
  )

  it.each([
    ['times out is retried', 'stub.hang', { kind: 'timeout', process: 'runtime' }, true],
    [
      'refuses to start waits for a start',
      'stub.refuse',
      { kind: 'exited', process: 'runtime' },
      false,
    ],
    [
      'finds the directory held at the deadline waits for a start',
      'stub.lock-late',
      { kind: 'state-locked' },
      false,
    ],
  ] as const)('after a crash, a relaunch that %s', async (_, marker, failure, retried) => {
    const endpoints = await supervise().start()
    const markerPath = await mark(marker)
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
  })

  it('starts afresh when asked while a restart waits', async () => {
    const endpoints = await supervise('ready', 'ready', {
      restart: { ...RESTART, backoffMs: 60_000 },
    }).start()
    await crash(endpoints.reef)
    await expect.poll(statuses).toContain('failed')

    expect(await supervisor!.start()).toEqual(endpoints)
    expect(roles()).toEqual(['runtime', 'reef', 'runtime', 'reef'])
    expect(alive()).toEqual(['runtime', 'reef'])
  })

  it(
    'stops restarting after repeated crashes and waits for a start',
    { timeout: startsTimeout(RESTART.attempts + 2) },
    async () => {
      const supervisor = supervise()
      const endpoints = await supervisor.start()
      const runtimeCrash = { status: 'failed', failure: { kind: 'crashed', process: 'runtime' } }
      for (let crashes = 1; crashes <= RESTART.attempts + 1; crashes++) {
        const settled = nextState(supervisor, restartSettled())
        await crash(endpoints.mcp)
        expect(await settled).toMatchObject(
          crashes <= RESTART.attempts
            ? { status: 'running', endpoints }
            : { ...runtimeCrash, restarting: false },
        )
      }
      await Promise.all(exits)

      // Every crash is reported, and each restart is reported until they run out.
      const restart = [
        { ...runtimeCrash, restarting: true },
        { status: 'starting' },
        { status: 'running' },
      ]
      expect(states).toMatchObject([
        { status: 'starting' },
        { status: 'running' },
        ...Array.from({ length: RESTART.attempts }, () => restart).flat(),
        { ...runtimeCrash, restarting: false },
      ])
      expect(alive()).toEqual([])

      // A start aborts a pending restart, so first wait twice the backoff that
      // another restart would take, and check that none ran.
      await delay(RESTART.backoffMs * 2 ** RESTART.attempts * 2)
      expect(states).toHaveLength(2 + 3 * RESTART.attempts + 1)
      expect(roles()).toHaveLength(2 * (RESTART.attempts + 1))

      expect(await supervisor.start()).toEqual(endpoints)
      expect(statuses().slice(-3)).toEqual(['failed', 'starting', 'running'])
      expect(roles()).toHaveLength(2 * (RESTART.attempts + 2))
      expect(alive()).toEqual(['runtime', 'reef'])
    },
  )
})

describe('a state directory another runtime holds', () => {
  // The deadline can catch a runtime that has not started yet. It gets this
  // long to exit by itself once it is stopped, and the test outlasts the
  // readiness deadline, this stop and the kill that follows it.
  const LOCKED_STOP_TIMEOUT_MS = 10_000
  const LOCKED_TEST_TIMEOUT_MS = 15_000

  it('retries the runtime until the directory is free', async () => {
    const lock = await mark('stub.lock')
    const started = supervise().start()
    await expect.poll(() => roles().length).toBeGreaterThanOrEqual(2)
    await rm(lock)

    await started
    expect(roles().filter((role) => role === 'runtime').length).toBeGreaterThanOrEqual(2)
    expect(statuses()).toEqual(['starting', 'running'])
    expect(alive()).toEqual(['runtime', 'reef'])
  })

  it.each([
    ['the directory stays held', 'stub.lock'],
    // The deadline passes before the runtime has found the directory held.
    ['a runtime that is still starting at the deadline finds it held', 'stub.lock-late'],
  ])('fails as locked when %s', { timeout: LOCKED_TEST_TIMEOUT_MS }, async (_, marker) => {
    await mark(marker)

    const failure = await failureOf(
      supervise('ready', 'ready', { stopTimeoutMs: LOCKED_STOP_TIMEOUT_MS }).start(),
    )

    expect(failure).toMatchObject({ kind: 'state-locked' })
    expect(failure.kind === 'state-locked' && failure.output).toContain('state directory locked')
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
  const url = 'postgres://coral:db-password@127.0.0.1:5433/coral'

  it('starts the runtime on the Postgres database that desktop.json names', async () => {
    await writeFile(
      join(dataDir, 'desktop.json'),
      JSON.stringify({ database: { backend: 'postgres', url } }),
    )

    const { config, env } = await runtimeLaunch((await supervise().start()).mcp)

    expect(config).toContain(
      '\n[database]\nbackend = "postgres"\nurl_env = "CORAL_DESKTOP_DATABASE_URL"\n',
    )
    expect(config).not.toContain('db-password')
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

describe('search', () => {
  it('turns semantic search off in the runtime when desktop.json says so', async () => {
    await writeFile(join(dataDir, 'desktop.json'), JSON.stringify({ search: { semantic: false } }))

    const { config } = await runtimeLaunch((await supervise().start()).mcp)

    expect(config).toContain('\n[search]\nsemantic_search_enabled = false\n')
  })

  it('leaves semantic search to the runtime default when desktop.json has no search section', async () => {
    const { config } = await runtimeLaunch((await supervise().start()).mcp)

    expect(config).not.toContain('[search]')
  })

  it.each([
    { section: 'off', message: 'search must be an object' },
    { section: { semantic: 'no' }, message: 'search.semantic must be true or false' },
    { section: { semantic: false, model: 'x' }, message: 'search has an unknown key, model' },
  ])(
    'fails, and launches nothing, when the search section is $section',
    async ({ section, message }) => {
      await writeFile(join(dataDir, 'desktop.json'), JSON.stringify({ search: section }))

      const failure = await failureOf(supervise().start())

      expect(failure).toMatchObject({ kind: 'error', message: expect.stringContaining(message) })
      expect(launched).toEqual([])
    },
  )
})
