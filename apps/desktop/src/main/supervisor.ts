import { join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'

import { readDatabase } from './database'
import { PortInUseError, resolvePorts, type PersistedListener, type Ports } from './ports'
import type { Launcher, ManagedProcess, ProcessExit } from './process'
import {
  loopbackUrl,
  reefEnv,
  runtimeEnv,
  STATE_LOCKED_EXIT_CODE,
  writeRuntimeConfig,
  type ReefEnv,
} from './runtime'

export type Role = 'runtime' | 'reef'

export interface Endpoints {
  /** The origin the window loads and every Run Status Page URL names. */
  reef: string
  /** The origin of every Workspace MCP endpoint. */
  mcp: string
}

export type Failure =
  | { kind: 'port-in-use'; listener: PersistedListener; port: number }
  /** The process exited before it was ready. */
  | { kind: 'exited'; process: Role; exit: ProcessExit; output: string }
  /** The process exited after Coral was running. */
  | { kind: 'crashed'; process: Role; exit: ProcessExit; output: string }
  | { kind: 'timeout'; process: Role; output: string }
  /** Another runtime held the state directory until the start timed out. */
  | { kind: 'state-locked'; output: string }
  /** Desktop could not prepare the launch, for example its data folder. */
  | { kind: 'error'; message: string }

export type SupervisorState =
  | { status: 'stopped' }
  | { status: 'starting' }
  | { status: 'running'; endpoints: Endpoints }
  /** `restarting`: Coral starts again by itself; otherwise it waits for `start()`. */
  | { status: 'failed'; failure: Failure; restarting: boolean }

export class SupervisorError extends Error {
  constructor(readonly failure: Failure) {
    super(`Coral could not start: ${failure.kind}`)
  }
}

export interface RestartPolicy {
  /** Automatic restarts allowed within `windowMs`; the next crash waits for `start()`. */
  attempts: number
  windowMs: number
  /** The wait before the first restart in the window. Each further restart waits twice as long. */
  backoffMs: number
}

export interface SupervisorOptions {
  /** Desktop's private data directory. The supervisor owns `desktop.json` and `runtime/` in it. */
  dataDir: string
  runtime: Launcher
  reef: Launcher<ReefEnv>
  readinessTimeoutMs: number
  /** Whether the runtime shows a newer release: only when this package can update itself. */
  checkUpdates: boolean
  /** How long a process may take to stop before it is killed. Defaults to 10 seconds. */
  stopTimeoutMs?: number
  restart?: RestartPolicy
}

export interface StartOptions {
  /** Allocate a new port for this listener instead of the kept one. */
  replacePort?: PersistedListener
}

export interface Supervisor {
  /**
   * Starts the runtime, then Reef once the runtime is ready, and resolves when
   * Reef is ready. Rejects with `SupervisorError` and leaves no process running
   * when either fails to start. Repeated calls share one start; a call during
   * an automatic restart's wait starts at once.
   *
   * A process that exits once Coral is running is restarted: the runtime with
   * Reef, Reef alone. A restart that times out counts as another attempt. A
   * runtime that refuses to start is not retried, except while another runtime
   * still holds its state directory.
   */
  start(options?: StartOptions): Promise<Endpoints>
  /**
   * Stops Reef, then the runtime, including a start or restart in progress.
   * Each process gets `stopTimeoutMs` to finish its work before it is killed.
   */
  stop(): Promise<void>
  /** Calls `listener` on every state change. Returns the unsubscribe function. */
  on(event: 'state', listener: (state: SupervisorState) => void): () => void
}

const READINESS_POLL_MS = 100
const READINESS_REQUEST_TIMEOUT_MS = 2000
const STOP_TIMEOUT_MS = 10_000
const KILL_GRACE_MS = 1000
const LOCK_RETRY_MS = 250
const DEFAULT_RESTART: RestartPolicy = { attempts: 3, windowMs: 60_000, backoffMs: 1000 }

export function createSupervisor(options: SupervisorOptions): Supervisor {
  const policy = options.restart ?? DEFAULT_RESTART
  const stopTimeoutMs = options.stopTimeoutMs ?? STOP_TIMEOUT_MS
  const listeners = new Set<(state: SupervisorState) => void>()
  let state: SupervisorState = { status: 'stopped' }
  /** `ready` once the process has passed its readiness check. */
  let running: { role: Role; child: ManagedProcess; ready: boolean }[] = []
  let current: { promise: Promise<Endpoints>; controller: AbortController } | null = null
  /** The first process to exit during the current start. */
  let startFailure: Failure | undefined
  /** Settles once every process that stopAll() was asked to stop has exited. */
  let teardown = Promise.resolve()
  /** The ports of the last launch, which a Reef-only restart reuses. */
  let ports: Ports | null = null
  /** When each crash in the restart window happened. */
  let crashes: number[] = []
  /** The pending automatic restart. stop() and start() abort it. */
  let recovery: AbortController | null = null

  function publish(next: SupervisorState) {
    state = next
    for (const listener of listeners) listener(next)
  }

  function launch<Env extends Record<string, string>>(
    role: Role,
    launcher: Launcher<Env>,
    env: Env,
    signal: AbortSignal,
  ) {
    signal.throwIfAborted()
    const entry = { role, child: launcher(env), ready: false }
    running.push(entry)
    void entry.child.exited.then((exit) => {
      // stopAll() removes what it stops; any other exit is the process's own.
      if (!running.includes(entry)) return
      running = running.filter((other) => other !== entry)
      const output = entry.child.outputTail()
      // A process that leaves before it is ready fails the start. One that
      // leaves later crashed, even while a start waits for the other.
      if (current && !entry.ready) {
        startFailure ??= { kind: 'exited', process: role, exit, output }
        return
      }
      const failure: Failure = { kind: 'crashed', process: role, exit, output }
      current?.controller.abort(new SupervisorError(failure))
      current = null
      void recover(failure)
    })
    return entry
  }

  /**
   * Reports a crash, or an automatic restart that timed out, then restarts what
   * it took down, within the restart policy.
   */
  async function recover(failure: Failure) {
    // A crash while a restart waits, such as the runtime following Reef, joins
    // that restart instead of counting again.
    const joining = recovery !== null
    recovery?.abort()
    const controller = new AbortController()
    recovery = controller
    const now = Date.now()
    if (!joining) crashes = [...crashes.filter((at) => now - at < policy.windowMs), now]
    const restarting = crashes.length <= policy.attempts
    // Reef needs the runtime; the runtime is restarted together with Reef.
    const from: Role = running.some(({ role }) => role === 'runtime') ? 'reef' : 'runtime'
    publish({ status: 'failed', failure, restarting })
    if (from === 'runtime' || !restarting) await stopAll()
    if (!restarting) {
      if (recovery === controller) recovery = null
      return
    }

    const wait = policy.backoffMs * 2 ** (crashes.length - 1)
    await delay(wait, undefined, { signal: controller.signal }).catch(() => {})
    // stop(), start() or a later crash took over.
    if (controller.signal.aborted) return
    recovery = null
    // begin() publishes the outcome; the window shows it.
    void begin({}, from, { automatic: true }).catch(() => {})
  }

  async function waitReady(
    entry: (typeof running)[number],
    url: string,
    deadline: number,
    signal: AbortSignal,
  ) {
    for (;;) {
      signal.throwIfAborted()
      if (startFailure) throw new SupervisorError(startFailure)
      if (Date.now() >= deadline)
        throw new SupervisorError({
          kind: 'timeout',
          process: entry.role,
          output: entry.child.outputTail(),
        })
      if (await isReady(url, signal)) {
        entry.ready = true
        return
      }
      await delay(READINESS_POLL_MS, undefined, { signal }).catch(() => {})
    }
  }

  async function startRuntime(ports: Ports, signal: AbortSignal) {
    const stateDir = join(options.dataDir, 'runtime')
    const database = await readDatabase(join(options.dataDir, 'desktop.json'))
    await writeRuntimeConfig(stateDir, ports, database, options.checkUpdates)
    const deadline = Date.now() + options.readinessTimeoutMs
    for (;;) {
      const runtime = launch(
        'runtime',
        options.runtime,
        runtimeEnv(stateDir, ports, database),
        signal,
      )
      try {
        // MCP readiness and gRPC `coral.readiness` report the same engine
        // check. Reef's own readiness probes gRPC, so its wait covers that
        // listener.
        return await waitReady(runtime, `${loopbackUrl(ports.mcp)}/readyz`, deadline, signal)
      } catch (error) {
        // A runtime that Desktop stopped, or that outlived a crashed Desktop,
        // may still be finishing its work in the same state directory.
        if (startFailure?.kind !== 'exited' || startFailure.exit.code !== STATE_LOCKED_EXIT_CODE)
          throw error
        if (Date.now() + LOCK_RETRY_MS >= deadline)
          throw new SupervisorError({ kind: 'state-locked', output: startFailure.output })
        startFailure = undefined
        await delay(LOCK_RETRY_MS, undefined, { signal })
      }
    }
  }

  async function run(
    { replacePort }: StartOptions,
    from: Role,
    signal: AbortSignal,
  ): Promise<Endpoints> {
    // The last launch's processes may still hold the ports.
    await teardown
    if (from === 'runtime' || !ports) {
      try {
        ports = await resolvePorts(join(options.dataDir, 'desktop.json'), { replace: replacePort })
      } catch (error) {
        if (error instanceof PortInUseError)
          throw new SupervisorError({
            kind: 'port-in-use',
            listener: error.listener,
            port: error.port,
          })
        throw error
      }
      await startRuntime(ports, signal)
    }

    const reef = launch('reef', options.reef, reefEnv(ports), signal)
    const deadline = Date.now() + options.readinessTimeoutMs
    await waitReady(reef, `${loopbackUrl(ports.reef)}/readyz`, deadline, signal)
    // The runtime can leave after Reef's check reached it: never report it running.
    if (startFailure) throw new SupervisorError(startFailure)
    return { reef: loopbackUrl(ports.reef), mcp: loopbackUrl(ports.mcp) }
  }

  /** Stops every process, Reef first. Later starts wait for it to finish. */
  function stopAll(): Promise<void> {
    const stopping = running.toReversed()
    running = []
    const previous = teardown
    teardown = (async () => {
      await previous
      for (const { child } of stopping) await stopProcess(child, stopTimeoutMs)
    })()
    return teardown
  }

  function begin(
    startOptions: StartOptions,
    from: Role,
    { automatic = false } = {},
  ): Promise<Endpoints> {
    const controller = new AbortController()
    startFailure = undefined
    publish({ status: 'starting' })
    const promise = run(startOptions, from, controller.signal).then(
      (endpoints) => {
        // stop(), or a crash's recovery, owns teardown and the state of a
        // start it cancelled.
        if (controller.signal.aborted) throw cancelled(controller.signal)
        current = null
        publish({ status: 'running', endpoints })
        return endpoints
      },
      async (error: unknown) => {
        if (controller.signal.aborted) throw cancelled(controller.signal)
        await stopAll()
        current = null
        const failure =
          error instanceof SupervisorError
            ? error.failure
            : {
                kind: 'error' as const,
                message: error instanceof Error ? error.message : String(error),
              }
        // An automatic restart that ran out of time uses one of its attempts.
        // A process that refused to start, or a port taken meanwhile, waits
        // for the user.
        if (automatic && failure.kind === 'timeout') void recover(failure)
        else publish({ status: 'failed', failure, restarting: false })
        throw new SupervisorError(failure)
      },
    )
    current = { promise, controller }
    return promise
  }

  function start(startOptions: StartOptions = {}): Promise<Endpoints> {
    if (current) return current.promise
    if (state.status === 'running') return Promise.resolve(state.endpoints)
    // A start from outside is a fresh chance: it replaces a pending restart,
    // stops what that restart kept running, and forgets earlier crashes.
    recovery?.abort()
    recovery = null
    crashes = []
    if (running.length > 0) void stopAll()
    return begin(startOptions, 'runtime')
  }

  async function stop() {
    recovery?.abort()
    recovery = null
    const cancelled = current
    current = null
    cancelled?.controller.abort()
    await stopAll()
    await cancelled?.promise.catch(() => {})
    publish({ status: 'stopped' })
  }

  return {
    start,
    stop,
    on(_event, listener) {
      listeners.add(listener)
      return () => listeners.delete(listener)
    },
  }
}

/** Why a start ended early: the crash that cancelled it, or a stop. */
function cancelled(signal: AbortSignal): Error {
  return signal.reason instanceof SupervisorError
    ? signal.reason
    : new Error('Coral stopped before it was ready')
}

async function isReady(url: string, signal: AbortSignal): Promise<boolean> {
  try {
    const response = await fetch(url, {
      redirect: 'manual',
      signal: AbortSignal.any([signal, AbortSignal.timeout(READINESS_REQUEST_TIMEOUT_MS)]),
    })
    await response.body?.cancel()
    return response.status === 200
  } catch {
    return false
  }
}

async function exitsWithin(child: ManagedProcess, ms: number): Promise<boolean> {
  const timer = new AbortController()
  try {
    return await Promise.race([
      child.exited.then(() => true),
      delay(ms, false, { signal: timer.signal }),
    ])
  } finally {
    timer.abort()
  }
}

async function stopProcess(child: ManagedProcess, timeoutMs: number) {
  child.stop()
  if (await exitsWithin(child, timeoutMs)) return
  child.kill()
  await exitsWithin(child, KILL_GRACE_MS)
}
