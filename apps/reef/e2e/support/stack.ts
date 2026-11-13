import { spawn } from 'node:child_process'
import { randomBytes } from 'node:crypto'
import { appendFile, mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'

import { reefRoot, type FixtureAddress, type Target } from './config.ts'
import {
  prepareAuthenticatedTarget,
  reservePort,
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

function startProcess(
  command: string,
  args: string[],
  env: NodeJS.ProcessEnv,
  log: string,
  privateValues: string[] = [],
) {
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
  return {
    pid: child.pid,
    async match(pattern: RegExp) {
      const deadline = Date.now() + 30_000
      while (Date.now() < deadline) {
        if (failed) throw failed
        const match = output.match(pattern)
        if (match) return match[1]
        if (child.exitCode !== null || child.signalCode !== null) {
          // 'exit' can come before the pipes drain; 'close' brings the last lines.
          await Promise.race([exited, delay(1000)])
          const end = child.exitCode ?? child.signalCode
          throw new Error(`Process exited (${end}) during startup. ${ending()}`)
        }
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

export async function startStack(options: {
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
}) {
  if (options.postgresURL && !options.authenticated) {
    throw new Error('Postgres requires the main suite')
  }
  if (options.multiInstance && !options.postgresURL) {
    throw new Error('Several Coral Instances share one Postgres database')
  }
  const rootDir = await mkdtemp(join(tmpdir(), 'coral-reel-'))
  // Each Coral Instance locks its own state root.
  const stateDirs = options.multiInstance
    ? [join(rootDir, 'coral-1'), join(rootDir, 'coral-2')]
    : [join(rootDir, 'coral')]
  const processes: ReturnType<typeof startProcess>[] = []
  let authentication: Awaited<ReturnType<typeof prepareAuthenticatedTarget>> | undefined
  let secretStore: Awaited<ReturnType<typeof startSecretStoreFixture>> | undefined
  let localWeb: Awaited<ReturnType<typeof reservePort>> | undefined
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
    // Instances share one credential key, or none could read what another sealed.
    const credentialKey = options.multiInstance ? randomBytes(32).toString('base64') : undefined
    const coralEnv = {
      ...childEnv,
      ...(localWeb ? { CORAL_LOCAL_WEB_URL: localWeb.url } : {}),
      ...(options.postgresURL ? { E2E_STACK_POSTGRES_URL: options.postgresURL } : {}),
      ...(credentialKey ? { E2E_CREDENTIAL_KEY: credentialKey } : {}),
      ...authentication?.coralEnv,
      ...(secretStore
        ? { E2E_SECRET_STORE_SECRET_ID: secretStore.secretId, ...envSecretStore.env }
        : {}),
    }
    const instances: Backend[] = []
    for (const [index, stateDir] of stateDirs.entries()) {
      await writeFile(
        join(stateDir, 'config.toml'),
        // The stack stops Coral within 5 s: Runs still live are interrupted at
        // once instead of draining.
        '[runtime]\nshutdown_drain_seconds = 0\n' +
          (options.privateNetworkAccess
            ? `[outbound]\nprivate_networks = "${options.privateNetworkAccess}"\n`
            : '') +
          (options.postgresURL
            ? "[database]\nbackend = 'postgres'\nurl_env = 'E2E_STACK_POSTGRES_URL'\n"
            : '') +
          (credentialKey ? "[credentials]\nkey_env = 'E2E_CREDENTIAL_KEY'\n" : '') +
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
      await authentication?.releaseCoralPorts(index)
      const coral = startProcess(
        options.coralBinary,
        [],
        { ...coralEnv, CORAL_CONFIG_DIR: stateDir },
        join(options.runDir, options.multiInstance ? `coral-${index + 1}.log` : 'coral.log'),
        [
          options.postgresURL ?? '',
          credentialKey ?? '',
          ...(authentication ? Object.values(authentication.coralEnv) : []),
          ...(secretStore ? [secretStore.secretId, ...Object.values(envSecretStore.env)] : []),
        ],
      )
      processes.push(coral)
      const grpcURL = await coral.match(
        /Coral gRPC server listening on (http:\/\/127\.0\.0\.1:\d+)/,
      )
      const mcpURL = await coral.match(
        /Coral MCP HTTP server serving each Workspace at (http:\/\/127\.0\.0\.1:\d+)/,
      )
      // The next instance starts once this one has migrated the shared database.
      await ready(mcpURL)
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
    if (proxy) control = await startInstanceControl(proxy, options.postgresURL!, childEnv)
    if (authentication) await ready(authentication.authentication.issuerURL)
    await authentication?.releaseWebPort()
    await localWeb?.release()
    // The bundle Docker and Desktop ship, with no node_modules beside it.
    const reef = startProcess(
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
      authentication ? [authentication.webEnv.CORAL_WEB_SESSION_SECRET] : [],
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
    }
    return { target, stop, pids: processes.map((child) => child.pid) }
  } catch (error) {
    try {
      await stop()
    } catch (cleanup) {
      console.warn(cleanup)
    }
    throw error
  }
}
