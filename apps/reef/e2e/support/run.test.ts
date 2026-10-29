import assert from 'node:assert/strict'
import { execFile } from 'node:child_process'
import { mkdtemp, readFile, rename, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { basename, join, sep } from 'node:path'
import { test } from 'node:test'
import { promisify } from 'node:util'

import { collectScenarios } from './artifacts.ts'
import { bootstrapConfig, reefRoot, runsRoot } from './config.ts'
import { postgresFixture } from '../../../../scripts/postgres.fixture.ts'

const exec = promisify(execFile)

const discover = async (database: string, suite = 'main', attached = '') => {
  const { stdout } = await exec(
    process.execPath,
    [
      'node_modules/vitest/vitest.mjs',
      'list',
      '--config',
      'e2e/vitest.config.ts',
      '--filesOnly',
      '--json',
    ],
    {
      cwd: reefRoot,
      env: {
        PATH: process.env.PATH,
        HOME: process.env.HOME,
        E2E_SUITE: suite,
        E2E_DATABASE: database,
        E2E_BASE_URL: attached,
      },
      timeout: 30_000,
    },
  )
  return JSON.parse(stdout) as { file: string; projectName: string }[]
}

test('product discovery includes new and renamed scenarios on both databases without a registry', async () => {
  const directory = await mkdtemp(join(reefRoot, 'e2e/scenarios/main/shell/discovery-'))
  const original = join(directory, 'new-journey.test.ts')
  const renamed = join(directory, 'renamed-journey.test.ts')
  try {
    await writeFile(original, '')
    const sqlite = await discover('sqlite')
    assert.ok(
      sqlite.some(({ file }) => file === original),
      'new product files are discovered',
    )
    assert.ok(sqlite.every(({ projectName }) => projectName === 'main-sqlite'))
    assert.ok(sqlite.some(({ file }) => file.endsWith('/accounts/authority.test.ts')))
    assert.ok(sqlite.some(({ file }) => file.endsWith('/providers/catalog/tool-count.test.ts')))
    const postgres = await discover('postgres')
    assert.deepEqual(
      postgres.map(({ file }) => file).toSorted(),
      sqlite.map(({ file }) => file).toSorted(),
    )
    assert.ok(postgres.every(({ projectName }) => projectName === 'main-postgres'))
    await rename(original, renamed)
    const next = await discover('sqlite')
    assert.ok(next.some(({ file }) => file === renamed))
    assert.ok(!next.some(({ file }) => file === original))
    for (const [suite, database] of [
      ['local-mode', 'sqlite'],
      ['desktop', 'sqlite'],
      ['multi-instance', 'postgres'],
    ]) {
      const focused = await discover(database, suite)
      assert.ok(focused.length > 0)
      assert.ok(
        focused.every(
          ({ file, projectName }) =>
            file.includes(`/e2e/scenarios/${suite}/`) && projectName === suite,
        ),
      )
      assert.ok(!sqlite.some(({ file }) => focused.some((check) => check.file === file)))
    }
    const attached = await discover('sqlite', 'local-mode', 'http://127.0.0.1:3000')
    assert.deepEqual(
      attached.map(({ file }) => file),
      [join(reefRoot, 'e2e/scenarios/local-mode/entry.test.ts')],
    )
  } finally {
    await rm(directory, { recursive: true, force: true })
  }
})

test('one Vitest command shares its run identity and converter decision across projects', async () => {
  const fixture = await mkdtemp(join(tmpdir(), 'reel-command-'))
  const runs = new Set<string>()
  try {
    const setup = join(fixture, 'setup.mjs')
    // Lightweight target adapters exercise real Vitest bootstrap, injection,
    // reporter ordering and teardown without booting Coral or an identity server.
    await writeFile(
      setup,
      `
      import { writeFile } from 'node:fs/promises'
      import { join } from 'node:path'
      export default async function(project) {
        const shared = project.getProvidedContext().reelBootstrap
        if (!shared) throw new Error('Shared bootstrap missing')
        const target = { name: project.name, baseURL: 'http://127.0.0.1:0' }
        project.provide('reel', { runDir: shared.runDir, target, film: shared.film, ffmpegBinary: shared.ffmpegBinary })
        const save = state => writeFile(join(shared.runDir, target.name, 'target.json'), JSON.stringify({ name: target.name, state, pids: [] }))
        await save('ready')
        return () => save('closed')
      }
    `,
    )
    const names = ['local-mode', 'main-sqlite', 'main-postgres']
    for (const name of names) {
      await writeFile(
        join(fixture, `${name}.test.ts`),
        `
        import { inject, test } from ${JSON.stringify(import.meta.resolve('vitest'))}
        import { writeFile } from 'node:fs/promises'
        import { join } from 'node:path'
        test('The same scenario on another target', async () => {
          const config = inject('reel')
          await writeFile(join(config.runDir, config.target.name, 'observed.json'), JSON.stringify(config))
        })
      `,
      )
    }
    const configPath = join(fixture, 'vitest.config.mjs')
    await writeFile(
      configPath,
      `export default ${JSON.stringify({
        root: reefRoot,
        test: {
          fileParallelism: false,
          maxWorkers: 1,
          reporters: [join(reefRoot, 'e2e/support/scenario-reporter.ts')],
          projects: names.map((name) => ({
            extends: true,
            test: { name, include: [join(fixture, `${name}.test.ts`)], globalSetup: [setup] },
          })),
        },
      })}`,
    )
    for (let command = 0; command < 2; command++) {
      const selectedRun = command === 0 ? bootstrapConfig({}).runDir : undefined
      const { stdout, stderr } = await exec(
        process.execPath,
        ['node_modules/vitest/vitest.mjs', 'run', '--config', configPath],
        {
          cwd: reefRoot,
          env: {
            ...process.env,
            E2E_BASE_URL: '',
            E2E_HARNESS_PROBES: '1',
            E2E_FFMPEG_BINARY: join(fixture, 'missing-ffmpeg'),
            E2E_SOURCE_COMMIT: 'a'.repeat(40),
            E2E_SOURCE_DIRTY: '0',
            E2E_RUN_ID: selectedRun ? basename(selectedRun) : undefined,
          },
          timeout: 30_000,
          killSignal: 'SIGKILL',
        },
      )
      const output = stdout + stderr
      const reported = [...output.matchAll(/Reel artifacts: (.+)\/index.html/g)].map(
        (match) => match[1],
      )
      for (const dir of reported) {
        assert.ok(dir.startsWith(runsRoot + sep))
        runs.add(dir)
      }
      assert.equal(reported.length, 1, `Projects must not create separate viewer runs:\n${output}`)
      assert.equal((output.match(/ffmpeg not found; keeping WebM recordings\./g) ?? []).length, 1)
      const dir = reported[0]
      if (selectedRun) assert.equal(dir, selectedRun, 'use the final container-mounted directory')
      const metadata = JSON.parse(await readFile(join(dir, 'run.json'), 'utf8'))
      assert.deepEqual(metadata.targets.toSorted(), names.toSorted())
      assert.equal(metadata.commit, 'a'.repeat(40), 'container source evidence needs no .git mount')
      assert.equal(metadata.dirty, false)
      const scenarios = await collectScenarios(dir)
      assert.equal(scenarios.length, names.length)
      assert.ok(scenarios.every(({ result }) => result.status === 'passed'))
      for (const name of names) {
        const observed = JSON.parse(await readFile(join(dir, name, 'observed.json'), 'utf8'))
        assert.equal(observed.runDir, dir)
        assert.equal(observed.ffmpegBinary, null)
        const target = JSON.parse(await readFile(join(dir, name, 'target.json'), 'utf8'))
        assert.equal(target.name, name)
        assert.equal(target.state, 'closed')
      }
    }
    assert.equal(runs.size, 2, 'Separate commands must retain separate run identities')
  } finally {
    await rm(fixture, { recursive: true, force: true })
    for (const dir of runs) await rm(dir, { recursive: true, force: true })
  }
})

test('failed product setup records a failed target, never a passing scenario', async () => {
  let runDir: string | undefined
  try {
    await assert.rejects(
      exec(
        process.execPath,
        [
          'node_modules/vitest/vitest.mjs',
          'run',
          '--config',
          'e2e/vitest.config.ts',
          'providers/catalog/tool-count',
        ],
        {
          cwd: reefRoot,
          env: {
            PATH: process.env.PATH,
            HOME: process.env.HOME,
            E2E_SUITE: 'main',
            E2E_DATABASE: 'sqlite',
            E2E_SOURCE_COMMIT: 'b'.repeat(40),
            E2E_SOURCE_DIRTY: '0',
            E2E_CORAL_BINARY: '/must-not-start',
            E2E_FIXTURE_BIND_HOST: '127.0.0.1',
            E2E_FIXTURE_PUBLIC_HOST: '10.254.254.254',
            E2E_FIXTURE_TLS_CERT: '/missing/cert',
            E2E_FIXTURE_TLS_KEY: '/missing/key',
            E2E_FIXTURE_CA: '/missing/ca',
          },
          timeout: 30_000,
        },
      ),
      (error: Error & { stdout?: string; stderr?: string }) => {
        const output = (error.stdout ?? '') + (error.stderr ?? '')
        runDir = output.match(/Reel artifacts: (.+)\/index.html/)?.[1]
        assert.match(output, /Fixture TLS requires readable/)
        return true
      },
    )
    assert.ok(runDir?.startsWith(runsRoot + sep))
    const metadata = JSON.parse(await readFile(join(runDir!, 'run.json'), 'utf8'))
    assert.deepEqual(metadata.targets, ['main-sqlite'])
    const target = JSON.parse(await readFile(join(runDir!, 'main-sqlite', 'target.json'), 'utf8'))
    assert.equal(target.state, 'failed')
    assert.match(target.error, /Target setup failed/)
    assert.deepEqual(target.pids, [])
    assert.ok((await collectScenarios(runDir!)).every(({ result }) => result.status !== 'passed'))
  } finally {
    if (runDir?.startsWith(runsRoot + sep)) await rm(runDir, { recursive: true, force: true })
  }
})

test('Postgres provisioning is released when Reel fixture validation fails before building', async (context) => {
  const fixture = await postgresFixture(context)
  await assert.rejects(
    exec(process.execPath, ['e2e/scripts/run.ts', '--database', 'postgres'], {
      cwd: reefRoot,
      env: {
        ...fixture.env,
        CORAL_TEST_POSTGRES_URL: 'postgres://private-canary@localhost/developer',
        E2E_FIXTURE_BIND_HOST: '127.0.0.1',
        E2E_FIXTURE_PUBLIC_HOST: '10.254.254.254',
        E2E_FIXTURE_TLS_CERT: '/missing/cert',
        E2E_FIXTURE_TLS_KEY: '/missing/key',
        E2E_FIXTURE_CA: '/missing/ca',
      },
      timeout: 30_000,
    }),
    (error: Error & { stdout?: string; stderr?: string }) => {
      assert.match(error.stderr ?? '', /Fixture TLS requires readable/)
      assert.doesNotMatch((error.stdout ?? '') + (error.stderr ?? ''), /private-canary/)
      return true
    },
  )
  const state = await fixture.state()
  assert.deepEqual(state.databases, [])
  assert.equal(state.dropped.length, 1)
  assert.match(state.dropped[0], /^coral_test_/)
})

test('owned commands wait for launch permission and exit if their owner disappears', async () => {
  const { spawn } = await import('node:child_process')
  for (const permission of ['', 'start']) {
    const child = spawn(
      process.execPath,
      [
        join(reefRoot, 'e2e/scripts/owned-command.mjs'),
        process.execPath,
        '-e',
        "process.stdout.write('application started')",
      ],
      {
        stdio: ['ignore', 'pipe', 'pipe', 'pipe'],
      },
    )
    let output = ''
    child.stdout!.on('data', (chunk) => {
      output += chunk
    })
    const done = new Promise<number | null>((resolve, reject) => {
      child.once('error', reject)
      child.once('exit', resolve)
    })
    await new Promise((resolve) => setTimeout(resolve, 100))
    assert.equal(output, '')
    const gate = child.stdio[3] as import('node:stream').Writable
    gate.end(permission)
    assert.equal(await done, permission ? 0 : 1)
    assert.equal(output, permission ? 'application started' : '')
  }
})

test('a closed command pipe fails through runner cleanup instead of an uncaught error', async () => {
  const fixture = await mkdtemp(join(tmpdir(), 'reel-closed-pipe-'))
  const completed = join(fixture, 'completed')
  const closed = join(fixture, 'closed')
  try {
    const entry = join(fixture, 'entry.mjs')
    await writeFile(
      entry,
      `
      import childProcess from 'node:child_process'
      import { syncBuiltinESMExports } from 'node:module'
      import { writeFile } from 'node:fs/promises'
      const spawn = childProcess.spawn
      childProcess.spawn = (command, args, options) => {
        if (!args[0]?.endsWith('/owned-command.mjs')) return spawn(command, args, options)
        const closed = ${JSON.stringify(closed)}
        const child = spawn(command, ['-e',
          "const fs = require('node:fs'); fs.closeSync(3); " +
          "fs.writeFileSync(" + JSON.stringify(closed) + ", String(process.pid)); " +
          "setTimeout(() => {}, 5000)"], options)
        const pipe = child.stdio[3]
        const end = pipe.end.bind(pipe)
        pipe.end = (...args) => {
          // Wait for the read end to close before Node processes pipe events.
          childProcess.spawnSync(process.execPath, ['-e',
            "while (!require('node:fs').existsSync(" + JSON.stringify(closed) + ")) " +
            "Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10)"],
            { timeout: 2000 })
          return end(...args)
        }
        return child
      }
      syncBuiltinESMExports()
      await import(${JSON.stringify(join(reefRoot, 'e2e/scripts/run.ts'))})
      await writeFile(${JSON.stringify(completed)}, 'runner completed')
      `,
    )
    await assert.rejects(
      exec(process.execPath, [entry], {
        cwd: reefRoot,
        env: {
          ...process.env,
          E2E_BASE_URL: '',
          E2E_HARNESS_PROBES: '1',
          E2E_CORAL_BINARY: '/unused-coral-binary',
        },
        timeout: 10_000,
      }),
      (error: Error & { code?: number; stderr?: string }) => {
        assert.equal(error.code, 1)
        assert.match(error.stderr ?? '', /EPIPE|failed \(signal\)/)
        assert.doesNotMatch(error.stderr ?? '', /Unhandled 'error' event/)
        return true
      },
    )
    assert.equal(await readFile(completed, 'utf8'), 'runner completed')
    const pid = Number(await readFile(closed, 'utf8'))
    assert.throws(() => process.kill(pid, 0), { code: 'ESRCH' })
  } finally {
    await rm(fixture, { recursive: true, force: true })
  }
})
