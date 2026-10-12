// Exercise the public runner with disposable command adapters, not a developer build.
import assert from 'node:assert/strict'
import { execFile } from 'node:child_process'
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { promisify } from 'node:util'
import { reefRoot } from './config.ts'

const exec = promisify(execFile)
test('each suite builds what it runs, with no database URL in build processes', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'reef-desktop-command-'))
  try {
    const log = join(dir, 'processes.jsonl')
    // Every build tool records its command; proto generation, the last
    // step before Vitest, fails so the run stops there.
    for (const tool of ['npm', 'cargo'])
      await writeFile(
        join(dir, tool),
        `#!${process.execPath}
import { appendFileSync } from 'node:fs'
appendFileSync(${JSON.stringify(log)}, JSON.stringify({ argv: process.argv, env: process.env }) + '\\n')
if (process.argv.includes('e2e:proto:gen')) process.exit(23)
`,
        { mode: 0o755 },
      )
    for (const [suite, builds, settings] of [
      ['main', [['run', 'build']]],
      ['local-mode', [['run', 'build']]],
      [
        'desktop',
        [
          ['run', 'stage', '--', '--runtime', '/unused'],
          ['run', 'package', '--', '--linux', '--dir'],
          ['exec', '--', 'electron-fuses', 'write', '--app'],
        ],
      ],
      // An installed app needs neither a runtime nor a package.
      ['desktop', [], { E2E_DESKTOP_EXECUTABLE: '/installed/coral-app', E2E_CORAL_BINARY: '' }],
    ] as const) {
      await writeFile(log, '')
      await assert.rejects(
        exec(process.execPath, ['e2e/scripts/run.ts', '--suite', suite], {
          cwd: reefRoot,
          env: {
            PATH: `${dir}:${process.env.PATH}`,
            HOME: dir,
            E2E_HARNESS_PROBES: suite === 'main' ? '1' : '',
            E2E_FIXTURE_HOME: join(dir, 'no-https-setup'),
            E2E_CORAL_BINARY: '/unused',
            E2E_POSTGRES_TEST_URL: 'postgres://secret@localhost/developer',
            CORAL_TEST_POSTGRES_URL: 'postgres://secret@localhost/developer',
            ...settings,
          },
          timeout: 30_000,
        }),
        /npm failed \(23\)/,
      )
      const processes = (await readFile(log, 'utf8'))
        .trim()
        .split('\n')
        .map((line) => JSON.parse(line))
      const commands: string[][] = processes.map((process) => process.argv.slice(2))
      assert.equal(commands.length, builds.length + 1)
      builds.forEach((build, index) =>
        assert.deepEqual(commands[index].slice(0, build.length), build),
      )
      assert.ok(commands.at(-1)!.includes('e2e:proto:gen'))
      for (const process of processes) {
        assert.equal(process.env.E2E_SUITE, suite)
        assert.doesNotMatch(JSON.stringify(process.env), /secret|postgres:\/\//)
      }
    }
  } finally {
    await rm(dir, { recursive: true, force: true })
  }
})

test('help and invalid suite combinations do not build or require fixture provisioning', async () => {
  const { stdout } = await exec(process.execPath, ['e2e/scripts/run.ts', '--help'], {
    cwd: reefRoot,
    env: { PATH: '', HOME: '/missing', E2E_FIXTURE_HOME: '/missing' },
  })
  assert.match(stdout, /--database sqlite\|postgres/)
  await assert.rejects(
    exec(process.execPath, ['e2e/scripts/run.ts', '--suite', 'desktop', '--database', 'postgres'], {
      cwd: reefRoot,
      env: { PATH: '', HOME: '/missing' },
    }),
    /Only the main suite selects a database/,
  )
})
