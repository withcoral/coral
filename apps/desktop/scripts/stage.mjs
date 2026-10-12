// Stages what a packaged Desktop runs beside Electron, outside the asar:
//
//   resources/coral/coral       the runtime (coral.exe on Windows)
//   resources/reef/server.mjs   Reef's bundled server, with every dependency
//   resources/reef/build/client
//
// npm run stage -- [--runtime <path to a built coral binary>]
//
// Without --runtime it builds a release runtime, for Apple silicon on macOS.
import { spawnSync } from 'node:child_process'
import { chmod, cp, mkdir, mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { parseArgs } from 'node:util'

import { buildRuntime, repoRoot } from './runtime.mjs'

const desktopRoot = resolve(import.meta.dirname, '..')
const resources = join(desktopRoot, 'resources')
const { values } = parseArgs({ options: { runtime: { type: 'string' } } })

// npm runs this script in apps/desktop; resolve a relative path from where
// the person ran npm.
await stageRuntime(values.runtime && resolve(process.env.INIT_CWD ?? '.', values.runtime))
await stageReef()

async function stageRuntime(prebuilt) {
  const directory = join(resources, 'coral')
  const staged = join(directory, process.platform === 'win32' ? 'coral.exe' : 'coral')
  await rm(directory, { recursive: true, force: true })
  await mkdir(directory, { recursive: true })
  if (prebuilt) {
    await cp(prebuilt, staged)
  } else if (process.platform === 'darwin') {
    // The macOS app is arm64 only. Name the target, so an Intel Mac stages
    // an arm64 runtime too.
    await cp(buildRuntime({ release: true, target: 'aarch64-apple-darwin' }), staged)
  } else {
    await cp(buildRuntime({ release: true }), staged)
  }
  await chmod(staged, 0o755)
  console.log(`Staged the runtime at ${staged}`)
}

async function stageReef() {
  const directory = join(resources, 'reef')
  // Build outside apps/reef, so a concurrent build or dev server there keeps
  // its own output.
  const scratch = await mkdtemp(join(tmpdir(), 'coral-desktop-reef-'))
  try {
    run('npm', ['run', 'build', '--prefix', join(repoRoot, 'apps', 'reef')], {
      ...process.env,
      REEF_BUILD_DIRECTORY: join(scratch, 'build'),
    })
    await rm(directory, { recursive: true, force: true })
    await mkdir(directory, { recursive: true })
    // The bundle carries the server build, so only the client files go along.
    await cp(join(scratch, 'server.mjs'), join(directory, 'server.mjs'))
    await cp(join(scratch, 'build', 'client'), join(directory, 'build', 'client'), {
      recursive: true,
    })
  } finally {
    await rm(scratch, { recursive: true, force: true })
  }
  console.log(`Staged Reef at ${directory}`)
}

function run(command, args, env = process.env) {
  // npm is a batch file on Windows, which Node starts only through a shell
  // that splits paths with spaces. Run npm's own script with Node instead.
  const npm = command === 'npm' && process.env.npm_execpath
  const result = npm
    ? spawnSync(process.execPath, [npm, ...args], { env, stdio: 'inherit' })
    : spawnSync(command, args, { env, stdio: 'inherit', shell: process.platform === 'win32' })
  if (result.status !== 0)
    throw new Error(`${command} ${args.join(' ')} failed (${result.status ?? result.signal})`)
}
