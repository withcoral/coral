import { randomUUID } from 'node:crypto'
import { isIP } from 'node:net'
import { resolve } from 'node:path'
import type { AuthenticationTarget } from './oidc-fixture.ts'

export const reefRoot = resolve(import.meta.dirname, '../..')
export const repoRoot = resolve(reefRoot, '../..')
export const runsRoot = resolve(reefRoot, 'e2e/runs')
export const desktopRoot = resolve(repoRoot, 'apps/desktop')
/**
 * The unpacked Linux build that `npm run package -- --linux --dir` writes.
 * electron-builder names the architecture in the folder unless it is x64.
 */
export const desktopExecutable = resolve(
  desktopRoot,
  `dist/linux-${process.arch === 'x64' ? '' : `${process.arch}-`}unpacked/coral-app`,
)
/** The variables that let Electron open its window on the caller's display. */
export const DISPLAY_ENV = ['DISPLAY', 'XAUTHORITY', 'WAYLAND_DISPLAY', 'XDG_RUNTIME_DIR']
/**
 * The variables Windows programs need: the system folder, without which
 * networking fails, the user's profile folders and a writable temporary folder.
 * Other platforms do not set them.
 */
export const WINDOWS_ENV = ['SYSTEMROOT', 'APPDATA', 'LOCALAPPDATA', 'USERPROFILE', 'TEMP', 'TMP']
/** Desktop's data folder, named by its app id, under Electron's `appData` folder. */
export const DESKTOP_DATA = 'com.withcoral.app'

export interface Target {
  baseURL: string
  grpcURL?: string
  mcpURL?: string
  /** Origins of every listener outside the private gRPC one, in a stack Reel composes. */
  publicOrigins?: string[]
  name:
    | 'main-sqlite'
    | 'main-postgres'
    | 'local-mode'
    | 'desktop'
    | 'multi-instance'
    | 'attached-local'
    | 'harness'
  authentication?: AuthenticationTarget
  /** The Coral Instances behind the round-robin proxy, on the multi-instance suite. */
  instances?: InstancesTarget
}

export interface InstancesTarget {
  /** The loopback origin of the harness's proxy controls. */
  controlURL: string
}

export const SUITES = ['main', 'local-mode', 'desktop', 'multi-instance']

export interface RunConfig {
  runDir: string
  film: boolean
  ffmpegBinary: string | null
  target: Target
  fixtures?: FixtureAddress
  tlsDirectory?: string
  /** The packaged app the desktop suite launches, with the display it opens on. */
  desktop?: DesktopApp
}

export interface DesktopApp {
  executable: string
  env: Record<string, string>
}

export interface FixtureAddress {
  bindHost: string
  advertisedHost: string
  tls?: { certFile: string; keyFile: string; caFile: string }
}

export const loopbackFixtures: FixtureAddress = {
  bindHost: '127.0.0.1',
  advertisedHost: '127.0.0.1',
}

function fixtureAddress(env: NodeJS.ProcessEnv): FixtureAddress | undefined {
  const bindHost = env.E2E_FIXTURE_BIND_HOST
  const advertisedHost = env.E2E_FIXTURE_PUBLIC_HOST
  const tls = [env.E2E_FIXTURE_TLS_CERT, env.E2E_FIXTURE_TLS_KEY, env.E2E_FIXTURE_CA]
  if (!bindHost && !advertisedHost && !tls.some(Boolean)) return undefined
  if (
    !bindHost ||
    !isIP(bindHost) ||
    !advertisedHost ||
    !/^[a-zA-Z0-9.-]+$/.test(advertisedHost) ||
    (tls.some(Boolean) && !tls.every(Boolean))
  ) {
    throw new Error(
      'Fixture settings require a bind IP, advertised host, and either all TLS files or none',
    )
  }
  return {
    bindHost,
    advertisedHost,
    ...(tls.every(Boolean)
      ? {
          tls: {
            certFile: resolve(tls[0]!),
            keyFile: resolve(tls[1]!),
            caFile: resolve(tls[2]!),
          },
        }
      : {}),
  }
}

export type RunBootstrap = Omit<ReturnType<typeof bootstrapConfig>, 'ffmpegBinary'> & {
  ffmpegBinary: string | null
}

declare module 'vitest' {
  export interface ProvidedContext {
    reelBootstrap: RunBootstrap
    reel: RunConfig
  }
}

export function commandOptions(args: string[]) {
  let suite = 'main'
  let database = 'sqlite'
  const selected = new Set<string>()
  const vitest: string[] = []
  for (let index = 0; index < args.length; index++) {
    const argument = args[index]
    const [flag, inline] = argument.split('=', 2)
    if (flag === '--suite' || flag === '--database') {
      if (selected.has(flag)) throw new Error(`Specify ${flag} once`)
      selected.add(flag)
      const value = inline ?? args[++index]
      const allowed = flag === '--suite' ? SUITES : ['sqlite', 'postgres']
      if (!allowed.includes(value)) throw new Error(`${flag} requires ${allowed.join(' or ')}`)
      if (flag === '--suite') suite = value
      else database = value
    } else if (flag === '--desktop' || flag === '--project') {
      throw new Error(
        'Select --suite main, local-mode, desktop or multi-instance; use --database postgres for main',
      )
    } else vitest.push(argument)
  }
  if (suite !== 'main' && selected.has('--database'))
    throw new Error('Only the main suite selects a database')
  // Several Coral Instances share one database only on Postgres.
  if (suite === 'multi-instance') database = 'postgres'
  return { suite, database, vitest }
}

export function bootstrapConfig(env: NodeJS.ProcessEnv) {
  const attached = env.E2E_BASE_URL
  const suite = env.E2E_SUITE || 'main'
  const database = env.E2E_DATABASE || 'sqlite'
  const probes = env.E2E_HARNESS_PROBES === '1'
  if (!SUITES.includes(suite)) throw new Error('Unknown Reel suite')
  if (!['sqlite', 'postgres'].includes(database)) throw new Error('Unknown Reel database')
  const managed = suite === 'main' || suite === 'multi-instance'
  if (database === 'postgres' && (!managed || probes || attached))
    throw new Error('Postgres requires the managed main suite')
  if (suite === 'multi-instance' && database !== 'postgres')
    throw new Error('The multi-instance suite runs on Postgres')
  if (attached && suite !== 'local-mode')
    throw new Error('Attached mode requires --suite local-mode')
  let postgresURL: string | undefined
  if (database === 'postgres') {
    if (!env.E2E_POSTGRES_TEST_URL)
      throw new Error('Run Reel with --database postgres to provision a disposable database')
    // Only the runner supplies its selected disposable database. Never consume
    // developer CORAL_TEST_POSTGRES_URL, auth or telemetry settings here.
    try {
      const url = new URL(env.E2E_POSTGRES_TEST_URL)
      if (
        !['postgres:', 'postgresql:'].includes(url.protocol) ||
        !url.hostname ||
        url.pathname.length < 2 ||
        url.hash
      )
        throw new Error()
      postgresURL = url.href
    } catch {
      throw new Error('Reel requires a disposable Postgres database URL')
    }
  }
  const fixtures = fixtureAddress(env)
  if (attached) {
    const url = new URL(attached)
    if (
      env.E2E_ALLOW_ATTACHED !== '1' ||
      !['127.0.0.1', '[::1]'].includes(url.hostname) ||
      url.protocol !== 'http:' ||
      url.username ||
      url.password ||
      url.search ||
      url.hash ||
      url.pathname !== '/'
    ) {
      throw new Error(
        'Attached mode requires a disposable loopback HTTP origin and E2E_ALLOW_ATTACHED=1',
      )
    }
  }
  // Container composition supplies the final host-visible directory name.
  // Native commands continue to own a fresh run identity at bootstrap.
  const runId = env.E2E_RUN_ID ?? `${new Date().toISOString().replaceAll(':', '-')}-${randomUUID()}`
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}\.\d{3}Z-[0-9a-f-]{36}$/.test(runId))
    throw new Error('Reel requires a timestamp and UUID run identifier')
  return {
    attached,
    suite,
    database,
    postgresURL,
    fixtures,
    tlsDirectory: env.E2E_TLS_DIRECTORY,
    source: env.E2E_SOURCE_COMMIT
      ? { commit: env.E2E_SOURCE_COMMIT, dirty: env.E2E_SOURCE_DIRTY === '1' }
      : undefined,
    desktop:
      suite === 'desktop'
        ? {
            // An installed app replaces the package that the runner builds.
            executable: env.E2E_DESKTOP_EXECUTABLE
              ? resolve(env.E2E_DESKTOP_EXECUTABLE)
              : desktopExecutable,
            // Electron needs the display; no developer settings cross into the app.
            env: {
              LANG: 'C.UTF-8',
              ...Object.fromEntries(
                ['PATH', ...DISPLAY_ENV, ...WINDOWS_ENV].flatMap((key) =>
                  env[key] ? [[key, env[key]]] : [],
                ),
              ),
            },
          }
        : undefined,
    film: env.E2E_FILM === '1',
    ffmpegBinary: env.E2E_FFMPEG_BINARY || 'ffmpeg',
    probes,
    runDir: resolve(runsRoot, runId),
    coralBinary: resolve(repoRoot, env.E2E_CORAL_BINARY || 'target/debug/coral'),
    // Deliberately do not forward developer Coral, database, auth or telemetry settings.
    childEnv: {
      PATH: env.PATH,
      HOME: env.HOME,
      ...Object.fromEntries(WINDOWS_ENV.map((key) => [key, env[key]])),
      LANG: 'C.UTF-8',
    },
  }
}
