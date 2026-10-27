import { spawn, spawnSync } from 'node:child_process'
import { resolve } from 'node:path'
import type { Writable } from 'node:stream'
import {
  bootstrapConfig,
  commandOptions,
  desktopExecutable,
  desktopRoot,
  DISPLAY_ENV,
  reefRoot,
  repoRoot,
} from '../support/config.ts'
import { LocalPostgres } from '../../../../scripts/postgres.ts'
import { validateFixtureTLS } from '../support/provider-fixtures.ts'

// Only explicit command options select a suite or database, never inherited settings.
const options = commandOptions(process.argv.slice(2))
if (options.vitest.includes('--help') || options.vitest.includes('-h')) {
  console.log(`Reel: the main product suite, plus focused local-mode, Desktop and multi-instance checks.

npm run test:e2e --prefix apps/reef -- [--suite main|local-mode|desktop|multi-instance] [--database sqlite|postgres] [Vitest filters]

Default: main with genuine sign-in and SQLite.
Postgres runs the same product scenarios in a disposable database.
Local mode runs the Reef build that Desktop ships, without Electron.
Desktop packages the Linux app and drives it in Electron; it needs a display.
E2E_DESKTOP_EXECUTABLE drives an installed app instead, on Linux or Windows.
Multi-instance runs two Coral Instances on one disposable Postgres database.
Examples: --database postgres accounts/    --suite local-mode    --suite multi-instance`)
  process.exit(0)
}
const desktop = options.suite === 'desktop'
// An installed app replaces the package; the runner then builds nothing for it.
const installedDesktop = desktop && Boolean(process.env.E2E_DESKTOP_EXECUTABLE)
const windows = process.platform === 'win32'
// npm is a batch file on Windows, which Node starts only through a shell.
// Run npm's own script with Node instead, as npm ran this one.
const npmCli = windows ? process.env.npm_execpath : undefined
const argumentsForVitest = options.vitest
// Windows variable names ignore case, but a copy keeps spellings such as
// `Path` and `SystemRoot`. Reel looks every name up in capitals.
const inherited = windows
  ? Object.fromEntries(
      Object.entries(process.env).map(([key, value]) => [key.toUpperCase(), value]),
    )
  : process.env
const settings: NodeJS.ProcessEnv = {
  ...inherited,
  E2E_SUITE: options.suite,
  E2E_DATABASE: options.database,
}
delete settings.E2E_POSTGRES_TEST_URL
let database: Awaited<ReturnType<LocalPostgres['provision']>> | undefined
let interrupted = false
let currentGroup: number | undefined
const stopGroup = (signal: NodeJS.Signals) => {
  if (!currentGroup) return
  // Windows has no process groups or signals. Ctrl+C already reaches every
  // process in the console, so only the forced stop ends the command's tree.
  if (windows) {
    if (signal === 'SIGKILL')
      spawnSync('taskkill', ['/pid', String(currentGroup), '/t', '/f'], { stdio: 'ignore' })
    return
  }
  try {
    process.kill(-currentGroup, signal)
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error
  }
}
const interrupt = () => {
  interrupted = true
  stopGroup('SIGTERM')
}
process.on('SIGINT', interrupt)
process.on('SIGTERM', interrupt)

try {
  if (desktop && !installedDesktop && process.platform !== 'linux')
    throw new Error(
      'The desktop suite packages the Linux app; run it in the Reel container, or set E2E_DESKTOP_EXECUTABLE to an installed app',
    )
  if (options.database === 'postgres') {
    if (settings.E2E_BASE_URL || settings.E2E_HARNESS_PROBES === '1')
      throw new Error('Postgres requires a stack that Reel starts, not attached mode or probes')
    database = await new LocalPostgres(settings).provision(settings.E2E_POSTGRES_URL)
    settings.E2E_POSTGRES_TEST_URL = database.url
  }
  // Validate before builds. Only explicit harness settings cross this seam.
  const bootstrap = bootstrapConfig(settings)
  if (bootstrap.fixtures) await validateFixtureTLS(bootstrap.fixtures)
  const env: NodeJS.ProcessEnv = { ...bootstrap.childEnv }
  for (const key of [
    'E2E_BASE_URL',
    'E2E_ALLOW_ATTACHED',
    'E2E_FILM',
    'E2E_FFMPEG_BINARY',
    'E2E_HARNESS_PROBES',
    'E2E_CORAL_BINARY',
    'E2E_DESKTOP_EXECUTABLE',
    'E2E_SUITE',
    'E2E_DATABASE',
    'E2E_POSTGRES_TEST_URL',
    'E2E_FIXTURE_BIND_HOST',
    'E2E_FIXTURE_PUBLIC_HOST',
    'E2E_FIXTURE_TLS_CERT',
    'E2E_FIXTURE_TLS_KEY',
    'E2E_FIXTURE_CA',
    'E2E_TLS_DIRECTORY',
    'E2E_SOURCE_COMMIT',
    'E2E_SOURCE_DIRTY',
    'E2E_RUN_ID',
    'TMPDIR',
    ...DISPLAY_ENV,
  ])
    if (settings[key]) env[key] = settings[key]

  async function run(command: string, args: string[], cwd: string, needsDatabase = false) {
    if (interrupted) throw new Error('E2E interrupted')
    const childEnv = { ...env }
    if (!needsDatabase) delete childEnv.E2E_POSTGRES_TEST_URL
    if (windows && command === 'npm' && !npmCli) throw new Error('Start Reel with npm on Windows')
    const commandLine =
      windows && command === 'npm' ? [process.execPath, npmCli!, ...args] : [command, ...args]
    const child = spawn(
      process.execPath,
      [resolve(reefRoot, 'e2e/scripts/owned-command.mjs'), ...commandLine],
      {
        cwd,
        env: childEnv,
        stdio: ['inherit', 'inherit', 'inherit', 'pipe'],
        // A process group lets the runner stop the whole command on POSIX. On
        // Windows a detached child gets its own console window instead.
        detached: !windows,
      },
    )
    const gate = child.stdio[3] as Writable | null
    const exited = new Promise<number | null>((settle, reject) => {
      child.once('error', reject)
      child.once('exit', settle)
      gate?.once('error', reject)
    })
    void exited.catch(() => {})
    currentGroup = child.pid
    // taskkill cannot find the orphans of a process that has exited, and
    // Windows may give its id to another process.
    if (windows) child.once('exit', () => (currentGroup = undefined))
    let timer: ReturnType<typeof setTimeout> | undefined
    const forceStop = () => {
      timer ??= setTimeout(() => stopGroup('SIGKILL'), 5000)
    }
    process.on('SIGINT', forceStop)
    process.on('SIGTERM', forceStop)
    try {
      if (!gate) throw new Error('Command startup pipe is unavailable')
      if (!interrupted) gate.end('start')
      else gate.end()
      if (interrupted) {
        stopGroup('SIGTERM')
        forceStop()
      }
      const code = await exited
      if (code !== 0) throw new Error(`${command} failed (${code ?? 'signal'})`)
    } finally {
      clearTimeout(timer)
      gate?.destroy()
      // A killed test runner can leave its application children alive. Stop only
      // this command's group before releasing its disposable database.
      stopGroup('SIGKILL')
      currentGroup = undefined
      process.off('SIGINT', forceStop)
      process.off('SIGTERM', forceStop)
    }
  }
  if (!env.E2E_BASE_URL && !installedDesktop) {
    if (!env.E2E_CORAL_BINARY)
      await run('cargo', ['build', '--locked', '-p', 'coral', '--bin', 'coral'], repoRoot)
    if (desktop) {
      // Package the app as users get it; staging builds its own Reef.
      await run('npm', ['run', 'stage', '--', '--runtime', bootstrap.coralBinary], desktopRoot)
      await run('npm', ['run', 'package', '--', '--linux', '--dir'], desktopRoot)
      // Playwright drives Electron through Node's inspector, which the shipped
      // fuses turn off. Turn on only that fuse, in this disposable build.
      await run(
        'npm',
        [
          'exec',
          '--',
          'electron-fuses',
          'write',
          '--app',
          desktopExecutable,
          'EnableNodeCliInspectArguments=on',
        ],
        desktopRoot,
      )
    } else await run('npm', ['run', 'build'], reefRoot)
  }
  await run('npm', ['run', 'e2e:proto:gen'], reefRoot)
  await run(
    process.execPath,
    [
      'node_modules/vitest/vitest.mjs',
      'run',
      '--config',
      'e2e/vitest.config.ts',
      ...argumentsForVitest,
    ],
    reefRoot,
    true,
  )
} catch (error) {
  console.error(error instanceof Error ? error.message : 'E2E failed')
  process.exitCode = 1
} finally {
  try {
    await database?.close()
  } catch (error) {
    console.error(error instanceof Error ? error.message : 'Reel cleanup failed')
    process.exitCode = 1
  }
  process.off('SIGINT', interrupt)
  process.off('SIGTERM', interrupt)
}
