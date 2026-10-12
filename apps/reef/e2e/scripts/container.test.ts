import assert from 'node:assert/strict'
import { execFile, spawn } from 'node:child_process'
import { mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { promisify } from 'node:util'
import { repoRoot, runsRoot } from '../support/config.ts'

const exec = promisify(execFile)

function artifactIds(args: string[]) {
  const ids = args
    .filter((value) => /^(E2E_RUN_ID|E2E_SELF_CHECK_RUN_ID)=/.test(value))
    .map((value) => value.split('=')[1])
  return ids.length ? ids : [args[args.indexOf('--name') + 1]]
}

test('container runs expose live evidence at its final path and retain it after failure or interruption', async () => {
  const root = await mkdtemp(join(tmpdir(), 'reel-engine-'))
  const log = join(root, 'commands')
  const engine = join(root, 'engine')
  const exported: string[] = []
  await writeFile(
    engine,
    `#!${process.execPath}
import { appendFileSync, mkdirSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
const args = process.argv.slice(2)
const record = () => appendFileSync(${JSON.stringify(log)}, JSON.stringify(args) + '\\n')
if (args[0] !== 'run') record()
if (args[0] === 'build') writeFileSync(args[args.indexOf('--iidfile') + 1], 'fixture-image')
if (args[0] === 'run') {
  for (let index = 0; index < args.length; index++) {
    if (args[index] !== '--mount') continue
    const path = /src=([^,]+)/.exec(args[index + 1])[1]
    const name = args[args.indexOf('--name') + 1]
    const directory = args.some(value => value.startsWith('E2E_RUN_ID=')) ? path : join(path, name)
    mkdirSync(directory, { recursive: true })
    writeFileSync(join(directory, 'evidence'), 'fixture evidence')
  }
  record()
  if (args.includes('--fake-wait')) setTimeout(() => {}, 60000)
  else process.exit(args.includes('--fake-fail') ? 7 : 0)
}
`,
    { mode: 0o700 },
  )
  const env = {
    ...process.env,
    REEL_CONTAINER_ENGINE: engine,
    REEL_SELF_CHECK: '0',
    CORAL_CONFIG_DIR: 'private-canary',
    NODE_TLS_REJECT_UNAUTHORIZED: '0',
  }
  const commands = async () =>
    (await readFile(log, 'utf8'))
      .trim()
      .split('\n')
      .map((line) => JSON.parse(line) as string[])
  try {
    const results = await Promise.allSettled([
      exec(process.execPath, ['apps/reef/e2e/scripts/container.ts', '--fake-fail'], {
        cwd: repoRoot,
        env,
        timeout: 30000,
      }),
      exec(process.execPath, ['apps/reef/e2e/scripts/container.ts', '--database', 'postgres'], {
        cwd: repoRoot,
        env,
        timeout: 30000,
      }),
    ])
    assert.deepEqual(
      results.map((result) => result.status),
      ['rejected', 'fulfilled'],
    )
    const child = spawn(process.execPath, ['apps/reef/e2e/scripts/container.ts', '--fake-wait'], {
      cwd: repoRoot,
      env: { ...env, REEL_SELF_CHECK: '1' },
      stdio: 'ignore',
    })
    const done = new Promise<void>((resolve, reject) => {
      child.once('error', reject)
      child.once('exit', () => resolve())
    })
    for (let n = 0; ; n++) {
      if ((await commands()).some((args) => args[0] === 'run' && args.includes('--fake-wait')))
        break
      assert.ok(n < 100, 'runner starts within five seconds')
      await new Promise((resolve) => setTimeout(resolve, 50))
    }
    try {
      const running = (await commands()).find(
        (args) => args[0] === 'run' && args.includes('--fake-wait'),
      )!
      for (const id of artifactIds(running))
        assert.equal(
          await readFile(join(runsRoot, id, 'evidence'), 'utf8'),
          'fixture evidence',
          'evidence must be visible in the normal run directory before the container exits',
        )
    } finally {
      child.kill('SIGTERM')
      await done
    }
    const observed = await commands()
    const runs = observed.filter((args) => args[0] === 'run')
    assert.equal(runs.length, 3)
    const names = runs.map((args) => args[args.indexOf('--name') + 1])
    exported.push(...runs.flatMap(artifactIds))
    assert.equal(new Set(names).size, 3)
    assert.equal(new Set(exported).size, 4, 'self-check and product evidence have separate runs')
    for (const name of names) {
      assert.ok(observed.some((args) => args.join(' ') === `network rm ${name}`))
      assert.ok(!(await readdir(runsRoot)).includes(`.${name}`))
    }
    for (const id of exported)
      assert.equal(await readFile(join(runsRoot, id, 'evidence'), 'utf8'), 'fixture evidence')
    for (const args of runs) {
      for (const id of artifactIds(args))
        assert.ok(
          args.includes(
            `type=bind,src=${join(runsRoot, id)},dst=/workspace/apps/reef/e2e/runs/${id}`,
          ),
          'the container mounts only its own final run directories',
        )
      assert.ok(
        args.includes(
          args.includes('--database') ? 'REEL_DATABASE=postgres' : 'REEL_DATABASE=sqlite',
        ),
      )
      assert.ok(args.includes('--network-alias') && args.includes('sidecar'))
      assert.ok(
        !args
          .join(' ')
          .match(
            /private-canary|NODE_TLS_REJECT_UNAUTHORIZED|--privileged|--network=host|docker.sock/,
          ),
      )
    }
    assert.ok(observed.some((args) => args[0] === 'stop'))
  } finally {
    for (const args of await commands()) if (args[0] === 'run') exported.push(...artifactIds(args))
    for (const name of exported) await rm(join(runsRoot, name), { recursive: true, force: true })
    await rm(root, { recursive: true, force: true })
  }
})
