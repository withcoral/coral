import { spawn, execFileSync } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { mkdir, mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { basename, join } from 'node:path'
import { bootstrapConfig, commandOptions, repoRoot } from '../support/config.ts'

const args = process.argv.slice(2)
const options = commandOptions(args)
if (args.includes('--help') || args.includes('-h')) await import('./run.ts')
let engine = process.env.REEL_CONTAINER_ENGINE
if (!engine) {
  for (const candidate of ['docker', 'podman']) {
    try {
      execFileSync(candidate, ['--version'], { stdio: 'ignore' })
      engine = candidate
      break
    } catch {
      /* Prefer Docker, otherwise use Podman. */
    }
  }
}
if (!engine) throw new Error('Docker or Podman is required for container Reel tests')
const name = `coral-reel-${randomUUID()}`
// Mount final run directories, not a staging root. The existing viewer can
// inspect live evidence, and the container can only change its own runs.
const artifacts = [bootstrapConfig({}).runDir]
if (process.env.REEL_SELF_CHECK === '1') artifacts.push(bootstrapConfig({}).runDir)
const scratch = await mkdtemp(join(tmpdir(), 'reel-container-'))
let child: ReturnType<typeof spawn> | undefined
let interrupted = false
let networkCreated = false
const stop = () => {
  interrupted = true
  child?.kill('SIGTERM')
  try {
    execFileSync(engine!, ['stop', '--time', '10', name], { stdio: 'ignore', timeout: 15_000 })
  } catch {
    /* The build may not have created the container yet. */
  }
}
process.on('SIGINT', stop)
process.on('SIGTERM', stop)
async function run(commandArgs: string[]) {
  if (interrupted) throw new Error('Reel interrupted')
  await new Promise<void>((resolve, reject) => {
    child = spawn(engine!, commandArgs, { cwd: repoRoot, stdio: 'inherit' })
    child.once('error', reject)
    child.once('exit', (code) =>
      code === 0 ? resolve() : reject(new Error(`${engine} failed (${code ?? 'signal'})`)),
    )
  })
  child = undefined
}
try {
  const rootless =
    engine === 'podman' &&
    execFileSync(engine, ['info', '--format', '{{.Host.Security.Rootless}}'], {
      encoding: 'utf8',
    }).trim() === 'true'
  const sha =
    process.env.E2E_SOURCE_COMMIT ??
    execFileSync('git', ['rev-parse', 'HEAD'], { cwd: repoRoot, encoding: 'utf8' }).trim()
  const dirty = process.env.E2E_SOURCE_COMMIT
    ? process.env.E2E_SOURCE_DIRTY === '1'
    : Boolean(
        execFileSync('git', ['status', '--porcelain'], { cwd: repoRoot, encoding: 'utf8' }).trim(),
      )
  const dockerfile = 'apps/reef/e2e/container/Dockerfile'
  await run([
    // Plain `docker build` ignores the builder that `docker buildx use` selects,
    // which is how CI supplies a cached one. A selected builder keeps its
    // result out of the local store unless asked to load it.
    ...(engine === 'docker' ? ['buildx', 'build', '--load'] : ['build']),
    ...(engine === 'podman' ? ['--ignorefile', `${dockerfile}.dockerignore`] : []),
    '--file',
    dockerfile,
    '--build-arg',
    `CORAL_GIT_SHA=${sha}`,
    '--iidfile',
    join(scratch, 'image'),
    '.',
  ])
  const image = (await readFile(join(scratch, 'image'), 'utf8')).trim()
  await run(['network', 'create', name])
  networkCreated = true
  for (const directory of artifacts) {
    await mkdir(directory, { recursive: true })
    console.log(`Reel host evidence: ${directory}`)
  }
  await run([
    'run',
    '--rm',
    '--name',
    name,
    '--network',
    name,
    '--network-alias',
    'sidecar',
    '--shm-size',
    '1g',
    ...(engine === 'podman' ? ['--security-opt', 'label=disable'] : []),
    ...artifacts.flatMap((directory) => [
      '--mount',
      `type=bind,src=${directory},dst=/workspace/apps/reef/e2e/runs/${basename(directory)}`,
    ]),
    '-e',
    `E2E_RUN_ID=${basename(artifacts[0])}`,
    ...(artifacts[1] ? ['-e', `E2E_SELF_CHECK_RUN_ID=${basename(artifacts[1])}`] : []),
    '-e',
    `REEL_UID=${rootless ? 0 : (process.getuid?.() ?? 0)}`,
    '-e',
    `REEL_GID=${rootless ? 0 : (process.getgid?.() ?? 0)}`,
    '-e',
    `E2E_FILM=${process.env.E2E_FILM === '1' ? '1' : '0'}`,
    '-e',
    `REEL_SELF_CHECK=${process.env.REEL_SELF_CHECK === '1' ? '1' : '0'}`,
    '-e',
    `REEL_DATABASE=${options.database}`,
    '-e',
    `E2E_SOURCE_COMMIT=${sha}`,
    '-e',
    `E2E_SOURCE_DIRTY=${dirty ? '1' : '0'}`,
    image,
    ...args,
  ])
} catch (error) {
  console.error(error instanceof Error ? error.message : 'Reel container failed')
  process.exitCode = 1
} finally {
  for (const commandArgs of [
    ['rm', '-f', name],
    ['network', 'rm', name],
  ]) {
    try {
      execFileSync(engine, commandArgs, { stdio: 'ignore', timeout: 20_000 })
    } catch {
      if (commandArgs[0] === 'network' && networkCreated) {
        console.error(`Reel network cleanup failed; inspect ${name}`)
        process.exitCode = 1
      }
    }
  }
  await rm(scratch, { recursive: true, force: true })
  process.off('SIGINT', stop)
  process.off('SIGTERM', stop)
}
