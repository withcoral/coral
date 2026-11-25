import { spawn } from 'node:child_process'
import { randomBytes } from 'node:crypto'
import { appendFile, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'

import { createClient } from '@connectrpc/connect'
import { createGrpcTransport } from '@connectrpc/connect-node'

import { reefRoot, repoRoot, type FixtureAddress, type Target } from './config.ts'
import {
  prepareAuthenticatedTarget,
  reservePort,
  type PortReservation,
  type SessionLifetimes,
} from './authenticated-target.ts'
import { startInstanceControl } from './instance-control.ts'
import { envSecretStore, startSecretStoreFixture } from './secret-store-fixture.ts'
import { startRoundRobinProxy, type Backend } from './round-robin-proxy.ts'

export async function bounded<T>(work: Promise<T>, ms: number, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    return await Promise.race([
      work,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms)
      }),
    ])
  } finally {
    clearTimeout(timer)
  }
}

/** Another process took a port that the harness reserved, before the child could bind it. */
class ReservedPortTaken extends Error {
  readonly port: number
  constructor(port: number, message: string) {
    super(message)
    this.port = port
  }
}

/**
 * Returns the port of `ports` that a child's final error report says it could
 * not bind because the address is in use, or `undefined`.
 *
 * The report runs from the last line that begins one, `Error: ` or `Coral web
 * startup failed: `, to the end. Coral and the Run Executor end with `Error: failed to bind … to 127.0.0.1:<port>` or `Error:
 * bind the health listener on 127.0.0.1:<port>`, then a cause line `Address
 * already in use`. Reef ends with `Coral web startup failed: HTTP listener:
 * listen EADDRINUSE: address already in use 127.0.0.1:<port>`.
 */
export function reservedPortTaken(output: string, ports: number[]) {
  const lines = output.trimEnd().split('\n')
  const start = lines.findLastIndex((line) => /^(?:Error: |Coral web startup failed: )/.test(line))
  if (start === -1) return undefined
  const report = lines.slice(start)
  if (!report.some((line) => /address already in use/i.test(line))) return undefined
  return ports.find((port) => {
    const bind = new RegExp(String.raw`\b(?:bind|listen)\b.*\b127\.0\.0\.1:${port}(?!\d)`)
    return report.some((line) => bind.test(line))
  })
}

/**
 * Starts a child that binds the `reserved` ports. Each reservation is released
 * just before the child starts. If the child exits during startup because one
 * of them was taken, the error is a `ReservedPortTaken`.
 */
async function startProcess(
  command: string,
  args: string[],
  env: NodeJS.ProcessEnv,
  log: string,
  {
    privateValues = [],
    reserved = [],
    note,
  }: {
    /** Values that never leave this process: each copy of the output redacts them. */
    privateValues?: string[]
    reserved?: PortReservation[]
    /** A line for the log before the child's own output. */
    note?: string
  } = {},
) {
  if (note) await appendFile(log, `${note}\n`)
  await Promise.all(reserved.map((reservation) => reservation.release()))
  const child = spawn(command, args, {
    cwd: reefRoot,
    env,
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  let output = ''
  let failed: Error | undefined
  // Every copy of the output that leaves this process, to the log or in an error.
  const redact = (text: string) =>
    privateValues.reduce(
      (redacted, value) => (value ? redacted.replaceAll(value, '[redacted]') : redacted),
      text,
    )
  let writes = Promise.resolve()
  child.on('error', (error) => {
    failed = error
  })
  for (const stream of [child.stdout!, child.stderr!]) {
    let pending = ''
    const save = (chunk: string) => {
      writes = writes
        .then(() => appendFile(log, redact(chunk)))
        .catch((error) => {
          failed = error
        })
    }
    stream.setEncoding('utf8').on('data', (chunk: string) => {
      output = (output + chunk).slice(-64 * 1024)
      pending += chunk
      const end = pending.lastIndexOf('\n')
      if (end !== -1) {
        save(pending.slice(0, end + 1))
        pending = pending.slice(end + 1)
      }
    })
    stream.once('end', () => save(pending))
  }
  const exited = new Promise<void>((resolve) => {
    child.once('close', () => resolve())
  })
  let stopping: Promise<void> | undefined
  // CI keeps no run directory, so a startup failure carries the cause itself.
  const ending = () => {
    const tail = redact(output.split('\n').slice(-40).join('\n').trim())
    return `See ${log}. Its last output:\n${tail}`
  }
  const running = async () => {
    if (failed) throw failed
    if (child.exitCode === null && child.signalCode === null) return
    // 'exit' can come before the pipes drain; 'close' brings the last lines.
    await Promise.race([exited, delay(1000)])
    const end = child.exitCode ?? child.signalCode
    const message = `Process exited (${end}) during startup. ${ending()}`
    const port = reservedPortTaken(
      output,
      reserved.map((reservation) => reservation.port),
    )
    throw port === undefined ? new Error(message) : new ReservedPortTaken(port, message)
  }
  return {
    pid: child.pid,
    /** Throws if the child has exited, as `match` does. */
    running,
    async match(pattern: RegExp) {
      const deadline = Date.now() + 30_000
      while (Date.now() < deadline) {
        if (failed) throw failed
        const match = output.match(pattern)
        if (match) return match[1]
        await running()
        await delay(50)
      }
      throw new Error(`Startup timed out. ${ending()}`)
    },
    stop() {
      stopping ??= (async () => {
        if (child.exitCode === null && child.signalCode === null) child.kill('SIGTERM')
        try {
          await bounded(exited, 5000, 'Process shutdown')
        } catch {
          child.kill('SIGKILL')
          await bounded(exited, 5000, 'Process kill')
        }
        await writes
      })()
      return stopping
    },
  }
}

type StartedProcess = Awaited<ReturnType<typeof startProcess>>

export async function ready(origin: string) {
  const deadline = Date.now() + 30_000
  while (Date.now() < deadline) {
    try {
      const response = await fetch(`${origin}/readyz`, {
        signal: AbortSignal.timeout(2500),
        redirect: 'error',
      })
      await response.body?.cancel()
      if (response.ok) return
    } catch {
      /* Startup can briefly refuse connections. */
    }
    await delay(100)
  }
  throw new Error(`Readiness failed: ${origin}/readyz`)
}

/** Waits until the Run Executor whose health service is at `url` has linked to its Coral. */
async function linked(url: string, runExecutor: StartedProcess) {
  // Imported here: the harness's own Node tests import this module, and Node's
  // type stripping rejects the enums in generated code. Vitest loads them.
  const { Health, HealthCheckResponse_ServingStatus } =
    await import('../generated/grpc/health/v1/health_pb.ts')
  const health = createClient(Health, createGrpcTransport({ baseUrl: url }))
  const deadline = Date.now() + 30_000
  while (Date.now() < deadline) {
    // One that exited, as when another process took its health port, never links.
    await runExecutor.running()
    try {
      const { status } = await health.check({ service: 'coral.readiness' }, { timeoutMs: 2000 })
      if (status === HealthCheckResponse_ServingStatus.SERVING) return
    } catch {
      /* Startup can briefly refuse connections. */
    }
    await delay(100)
  }
  throw new Error(`The Run Executor at ${url} did not link`)
}

interface StackOptions {
  coralBinary: string
  childEnv: NodeJS.ProcessEnv
  runDir: string
  authenticated?: boolean
  probes?: boolean
  postgresURL?: string
  fixtures?: FixtureAddress
  privateNetworkAccess?: 'deny' | 'https' | 'http-and-https'
  /** Two Coral Instances share one Postgres database behind a round-robin proxy. */
  multiInstance?: boolean
  /** Shortened sign-in lifetimes for a scenario's own stack. */
  sessionLifetimes?: SessionLifetimes
}

const STARTUP_ATTEMPTS = 3

/**
 * Starts Coral and Reef in private state. When another process takes a port
 * that the harness reserved before its child binds it, the attempt stops and
 * the stack starts again on fresh ports and fresh state, which rebuilds every
 * origin that names them. Every other startup failure fails at once.
 *
 * A Postgres stack makes one attempt. Coral migrates its database before it
 * binds a listener, so a collision always comes after a Coral used the one
 * database that the runner owns, and a fresh attempt cannot get a fresh one.
 */
export async function startStack(options: StackOptions) {
  if (options.postgresURL && !options.authenticated) {
    throw new Error('Postgres requires the main suite')
  }
  if (options.multiInstance && !options.postgresURL) {
    throw new Error('Several Coral Instances share one Postgres database')
  }
  const attempts = options.postgresURL ? 1 : STARTUP_ATTEMPTS
  for (let attempt = 1; ; attempt++) {
    try {
      return await startStackOnce(options, attempt)
    } catch (error) {
      if (!(error instanceof ReservedPortTaken) || attempt === attempts) throw error
      console.warn(
        `Another process took reserved port ${error.port} before its child bound it. ` +
          `Starting the stack again (attempt ${attempt + 1} of ${attempts}).`,
      )
    }
  }
}

async function startStackOnce(options: StackOptions, attempt: number) {
  // Each log keeps every attempt's output.
  const note = attempt > 1 ? `--- Reel: stack attempt ${attempt} ---` : undefined
  const rootDir = await mkdtemp(join(tmpdir(), 'coral-reel-'))
  // Each Coral Instance locks its own state root.
  const stateDirs = options.multiInstance
    ? [join(rootDir, 'coral-1'), join(rootDir, 'coral-2')]
    : [join(rootDir, 'coral')]
  const processes: StartedProcess[] = []
  const coralPids: number[] = []
  let authentication: Awaited<ReturnType<typeof prepareAuthenticatedTarget>> | undefined
  let secretStore: Awaited<ReturnType<typeof startSecretStoreFixture>> | undefined
  let localWeb: PortReservation | undefined
  let proxy: Awaited<ReturnType<typeof startRoundRobinProxy>> | undefined
  let control: Awaited<ReturnType<typeof startInstanceControl>> | undefined
  let stopping: Promise<void> | undefined
  const stop = () => {
    stopping ??= (async () => {
      const errors: unknown[] = []
      for (const process of processes.toReversed()) {
        try {
          await process.stop()
        } catch (error) {
          errors.push(error)
        }
      }
      try {
        await control?.stop()
        await proxy?.stop()
        await authentication?.close()
        await secretStore?.close()
        await localWeb?.release()
      } catch (error) {
        errors.push(error)
      }
      // Never remove storage from underneath a process that failed to stop.
      if (errors.length)
        throw new AggregateError(errors, `Stack cleanup failed; state retained at ${rootDir}`)
      await rm(rootDir, { recursive: true, force: true })
    })()
    return stopping
  }
  try {
    // Server bootstrap opens its filesystem vault under CORAL_CONFIG_DIR.
    // Keep harness files outside Coral's state root: its fresh-state fence must
    // recognize only the actual bootstrap configuration, without exceptions.
    const homeDir = join(rootDir, 'home')
    await Promise.all([...stateDirs, homeDir].map((directory) => mkdir(directory)))
    const childEnv = { ...options.childEnv, HOME: homeDir, XDG_CONFIG_HOME: homeDir }
    if (options.fixtures?.tls) {
      await mkdir(join(rootDir, 'certificates'))
      Object.assign(childEnv, {
        SSL_CERT_FILE: options.fixtures.tls.caFile,
        SSL_CERT_DIR: join(rootDir, 'certificates'),
        NODE_EXTRA_CA_CERTS: options.fixtures.tls.caFile,
      })
    }
    // The proxy listens first, so every instance advertises its origins.
    if (options.multiInstance) proxy = await startRoundRobinProxy()
    if (options.authenticated)
      authentication = await prepareAuthenticatedTarget(stateDirs, proxy, options.sessionLifetimes)
    else localWeb = await reservePort()
    // The address provider fixtures use, so `[outbound]` treats the store as it treats them.
    if (options.authenticated && options.fixtures)
      secretStore = await startSecretStoreFixture(options.fixtures)
    // The covering test License sets the Enterprise Edition, so the KV v2 store reads. Only a
    // build without a Release Date, such as Reel's, trusts its key (ADR 0038).
    const license = secretStore
      ? (
          await readFile(
            join(repoRoot, 'crates/coral-test-support/test-licenses/covering.license'),
            'utf8',
          )
        )
          .split('\n')
          .find((line) => line.startsWith('v4.public.'))
      : undefined
    // Instances share one credential key, or none could read what another sealed.
    const credentialKey = options.multiInstance ? randomBytes(32).toString('base64') : undefined
    // Several instances start no local Run Executor: each one's only Run
    // Executor dials it with the operator token, as an operator runs them.
    const runExecutorToken = options.multiInstance
      ? randomBytes(32).toString('base64url')
      : undefined
    const coralEnv = {
      ...childEnv,
      ...(runExecutorToken ? { E2E_RUN_EXECUTOR_TOKEN: runExecutorToken } : {}),
      ...(localWeb ? { CORAL_LOCAL_WEB_URL: localWeb.url } : {}),
      ...(options.postgresURL ? { E2E_STACK_POSTGRES_URL: options.postgresURL } : {}),
      ...(credentialKey ? { E2E_CREDENTIAL_KEY: credentialKey } : {}),
      ...authentication?.coralEnv,
      ...(secretStore
        ? { E2E_SECRET_STORE_SECRET_ID: secretStore.secretId, ...envSecretStore.env }
        : {}),
    }
    const instances: Backend[] = []
    // The Coral Instance id each printed, which the Runs it holds record.
    const instanceIds: string[] = []
    for (const [index, stateDir] of stateDirs.entries()) {
      await writeFile(
        join(stateDir, 'config.toml'),
        // The stack stops Coral within 5 s: Runs still live are interrupted at
        // once instead of draining.
        '[runtime]\nshutdown_drain_seconds = 0\n' +
          (runExecutorToken
            ? 'local_run_executors = false\n' +
              "[listeners.run_executor]\nbind = '127.0.0.1:0'\ntoken_env = 'E2E_RUN_EXECUTOR_TOKEN'\n"
            : '') +
          // With semantic search on, each instance downloads the model
          // (133 MB) from GitHub and runs the Embedder.
          '[search]\nsemantic_search_enabled = false\n' +
          (options.privateNetworkAccess
            ? `[outbound]\nprivate_networks = "${options.privateNetworkAccess}"\n`
            : '') +
          (options.postgresURL
            ? "[database]\nbackend = 'postgres'\nurl_env = 'E2E_STACK_POSTGRES_URL'\n"
            : '') +
          (credentialKey ? "[credentials]\nkey_env = 'E2E_CREDENTIAL_KEY'\n" : '') +
          (license ? `[license]\nkey = '${license}'\n` : '') +
          (secretStore
            ? secretStore.config('E2E_SECRET_STORE_SECRET_ID') + envSecretStore.config
            : '') +
          (authentication?.config(index) ??
            `
[listeners.grpc]
bind = '127.0.0.1:0'
[listeners.mcp]
bind = '127.0.0.1:0'
[listeners.auth]
bind = '127.0.0.1:0'
`),
      )
      const coral = await startProcess(
        options.coralBinary,
        [],
        { ...coralEnv, CORAL_CONFIG_DIR: stateDir },
        join(options.runDir, options.multiInstance ? `coral-${index + 1}.log` : 'coral.log'),
        {
          privateValues: [
            options.postgresURL ?? '',
            credentialKey ?? '',
            runExecutorToken ?? '',
            ...(authentication ? Object.values(authentication.coralEnv) : []),
            ...(secretStore ? [secretStore.secretId, ...Object.values(envSecretStore.env)] : []),
          ],
          reserved: authentication?.coralPorts(index),
          note,
        },
      )
      processes.push(coral)
      if (coral.pid !== undefined) coralPids.push(coral.pid)
      instanceIds.push(await coral.match(/Coral Instance (\S+) started/))
      const grpcURL = await coral.match(
        /Coral gRPC server listening on (http:\/\/127\.0\.0\.1:\d+)/,
      )
      const mcpURL = await coral.match(
        /Coral MCP HTTP server serving each Workspace at (http:\/\/127\.0\.0\.1:\d+)/,
      )
      // The next instance starts once this one has migrated the shared database.
      await ready(mcpURL)
      if (runExecutorToken) {
        const listener = await coral.match(/Coral Run Executor listener on (\S+)/)
        const health = await reservePort()
        // It holds one Run at a time, so a journey can fill it with one waiting Run.
        const runExecutor = await startProcess(
          options.coralBinary,
          ['run-executor', '--coral-address', `http://${listener}`, '--max-runs-held', '1'],
          {
            ...childEnv,
            CORAL_RUN_EXECUTOR_TOKEN: runExecutorToken,
            CORAL_RUN_EXECUTOR_HEALTH_LISTEN: `127.0.0.1:${health.port}`,
          },
          join(options.runDir, `run-executor-${index + 1}.log`),
          { privateValues: [runExecutorToken], reserved: [health], note },
        ).catch(async (error: unknown) => {
          // No cleanup owns this reservation, so a start that fails before it
          // releases the port closes the listener here.
          await health.release()
          throw error
        })
        processes.push(runExecutor)
        await linked(health.url, runExecutor)
      }
      instances.push({ grpcURL, mcpURL, authURL: authentication?.authOrigins[index] ?? '' })
    }
    const [{ grpcURL, mcpURL }] = instances
    // Without sign-in, Coral completes Provider OAuth on its own loopback
    // listener, as Desktop does; Reef sends consent there.
    const providerCallback = authentication
      ? undefined
      : await processes[0].match(
          /Coral Provider OAuth callback listening on (http:\/\/127\.0\.0\.1:\d+\/provider-oauth\/callback)/,
        )
    proxy?.serve(instances)
    if (proxy)
      control = await startInstanceControl(proxy, options.postgresURL!, childEnv, instanceIds)
    if (authentication) await ready(authentication.authentication.issuerURL)
    const webPort = authentication?.webPort ?? localWeb
    // The bundle Docker and Desktop ship, with no node_modules beside it.
    const reef = await startProcess(
      process.execPath,
      ['server.mjs'],
      {
        ...childEnv,
        NODE_ENV: 'production',
        HOST: '127.0.0.1',
        ...(localWeb ? { PORT: String(localWeb.port) } : {}),
        CORAL_ENDPOINT: proxy?.grpcURL ?? grpcURL,
        CORAL_WEB_AUTH_MODE: 'disabled',
        ...(providerCallback ? { CORAL_UPSTREAM_OAUTH_CALLBACK_URL: providerCallback } : {}),
        ...authentication?.webEnv,
      },
      join(options.runDir, 'reef.log'),
      {
        privateValues: authentication ? [authentication.webEnv.CORAL_WEB_SESSION_SECRET] : [],
        reserved: webPort ? [webPort] : [],
        note,
      },
    )
    processes.push(reef)
    const port = await reef.match(/Coral web listening on .*:(\d+)/)
    const baseURL = `http://127.0.0.1:${port}`
    const expectedWebURL = authentication?.webEnv.CORAL_WEB_PUBLIC_URL ?? localWeb?.url
    if (baseURL !== expectedWebURL)
      throw new Error('Reef did not bind its configured handoff origin')
    await ready(baseURL)
    const target: Target = {
      name: options.probes
        ? 'harness'
        : options.multiInstance
          ? 'multi-instance'
          : options.postgresURL
            ? 'main-postgres'
            : authentication
              ? 'main-sqlite'
              : 'local-mode',
      baseURL,
      // Behind the proxy, every client reaches Coral through it.
      grpcURL: proxy?.grpcURL ?? grpcURL,
      mcpURL: proxy?.mcpURL ?? mcpURL,
      // Coral's auth listener with sign-in, or its loopback Provider OAuth callback without.
      publicOrigins: [
        baseURL,
        proxy?.mcpURL ?? mcpURL,
        authentication?.authentication.issuerURL ?? new URL(providerCallback!).origin,
      ],
      ...(authentication ? { authentication: authentication.authentication } : {}),
      ...(control ? { instances: { controlURL: control.url } } : {}),
      coralPids,
    }
    return { target, stop, pids: processes.map((child) => child.pid) }
  } catch (error) {
    try {
      await stop()
    } catch (cleanup) {
      console.warn(cleanup)
      // A stack that did not stop can still hold its state: start no other.
      if (error instanceof ReservedPortTaken)
        throw new AggregateError(
          [error, cleanup],
          'Stack cleanup failed after another process took a reserved port; not starting again',
          { cause: cleanup },
        )
    }
    throw error
  }
}
