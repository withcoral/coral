import assert from 'node:assert/strict'
import { execFile } from 'node:child_process'
import { readFile, writeFile } from 'node:fs/promises'
import { join, resolve } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import { promisify } from 'node:util'
import test from 'node:test'
import { LocalPostgres } from './postgres.ts'
import { postgresFixture } from './postgres.fixture.ts'

test('provisioning owns container startup and readiness without Make on PATH', async (context) => {
  const fixture = await postgresFixture(context, { readinessFailures: 1 })
  const lease = await new LocalPostgres(fixture.env).provision()
  const state = await fixture.state()
  assert.deepEqual(state.containers['coral-test-postgres'], {
    running: true,
    image: 'docker.io/library/postgres:17',
    publish: '127.0.0.1::5432',
    port: '55432',
  })
  assert.equal(state.readinessFailures, 0)
  assert.equal(new URL(lease.url).host, '127.0.0.1:55432')
  assert.deepEqual(state.databases, [new URL(lease.url).pathname.slice(1)])
  await lease.close()
  assert.deepEqual((await fixture.state()).databases, [])
})

test('a disposable database is closed once after a failed consumer without removing the reusable container', async (context) => {
  const fixture = await postgresFixture(context)
  const lease = await new LocalPostgres(fixture.env).provision()
  try {
    throw new Error('consumer failed')
  } catch {
    await lease.close()
  }
  await lease.close()
  const state = await fixture.state()
  assert.deepEqual(state.databases, [])
  assert.deepEqual(state.dropped, [new URL(lease.url).pathname.slice(1)])
  assert.equal(state.containers['coral-test-postgres'].running, true)
  assert.equal(fixture.env.CORAL_TEST_POSTGRES_URL, undefined)
})

test('independent leases keep each other and the reusable container alive', async (context) => {
  const fixture = await postgresFixture(context)
  const postgres = new LocalPostgres(fixture.env)
  const first = await postgres.provision()
  const second = await postgres.provision()
  assert.notEqual(first.url, second.url)
  await first.close()
  await first.close()
  assert.deepEqual((await fixture.state()).databases, [new URL(second.url).pathname.slice(1)])
  await second.close()
  const state = await fixture.state()
  assert.deepEqual(state.databases, [])
  assert.equal(state.dropped.length, 2)
  assert.equal(Object.keys(state.containers).length, 1)
})

test('a client error after committed database creation still removes the owned database', async (context) => {
  const fixture = await postgresFixture(context, { creationCommittedThenFailed: true })
  await assert.rejects(
    new LocalPostgres(fixture.env).provision(),
    /creation failed; the disposable database was removed/,
  )
  const state = await fixture.state()
  assert.deepEqual(state.databases, [])
  assert.equal(state.dropped.length, 1)
})

test('a failed database creation cleans a possibly committed database and reports cleanup failure safely', async (context) => {
  const fixture = await postgresFixture(context, {
    creationCommittedThenFailed: true,
    fail: ['dropdb'],
  })
  await assert.rejects(new LocalPostgres(fixture.env).provision(), (error) => {
    assert.match(error.message, /cleanup failed.*coral_test_/)
    assert.doesNotMatch(error.message, /private-command-canary|postgres:\/\//)
    return true
  })
  const state = await fixture.state()
  assert.equal(state.databases.length, 1)
  const drops = state.commands.filter(({ args }) => args[2] === 'dropdb')
  assert.deepEqual(drops[0].args.slice(2), [
    'dropdb',
    '-U',
    'postgres',
    '--if-exists',
    state.databases[0],
  ])
})

test('Make shortcuts share container defaults, overrides, reuse and cleanup with the module', async (context) => {
  const fixture = await postgresFixture(context, {}, ['docker', 'podman'])
  const exec = promisify(execFile)
  const make = (target) =>
    exec(
      '/usr/bin/make',
      [
        '--no-print-directory',
        target,
        'LOCAL_POSTGRES_CONTAINER=chosen-postgres',
        'LOCAL_POSTGRES_IMAGE=fixture:17',
        'LOCAL_POSTGRES_PORT=55440',
      ],
      { cwd: resolve(import.meta.dirname, '..'), env: fixture.env, timeout: 15_000 },
    )
  assert.match(
    (await make('postgres-start')).stdout,
    /Postgres is ready at postgres:\/\/postgres:postgres@127.0.0.1:55440\/postgres/,
  )
  assert.equal(
    (await make('postgres-url')).stdout.trim(),
    'postgres://postgres:postgres@127.0.0.1:55440/postgres',
  )
  await make('postgres-stop')
  assert.equal((await fixture.state()).containers['chosen-postgres'].running, false)
  await make('postgres-start')
  const state = await fixture.state()
  assert.deepEqual(state.containers['chosen-postgres'], {
    running: true,
    image: 'fixture:17',
    publish: '127.0.0.1:55440:5432',
    port: '55440',
  })
  assert.equal(state.commands.filter(({ args }) => args[0] === 'run').length, 1)
  assert.ok(
    state.commands.every(({ engine }) => engine === 'docker'),
    'prefer Docker when both are available',
  )
  await make('postgres-clean')
  assert.deepEqual((await fixture.state()).containers, {})
  await make('postgres-stop')
  await make('postgres-clean')
})

test(
  'readiness has a deadline and cannot create a database in an unready container',
  { timeout: 5000 },
  async (context) => {
    const fixture = await postgresFixture(context, { fail: ['pg_isready'] })
    context.mock.timers.enable({ apis: ['Date'], now: 0 })
    const starting = new LocalPostgres(fixture.env).provision()
    void starting.catch(() => {})
    let polled = false
    for (let attempt = 0; attempt < 150; attempt++) {
      const state = await fixture.state().catch(() => undefined)
      if (state?.commands.some(({ args }) => args[2] === 'pg_isready')) {
        polled = true
        break
      }
      await delay(10)
    }
    assert.equal(polled, true)
    context.mock.timers.setTime(60_001)
    await assert.rejects(starting, /did not become ready within 60 seconds/)
    assert.deepEqual((await fixture.state()).databases, [])
  },
)

test('cleanup distinguishes an absent container from an engine failure without leaking command output', async (context) => {
  const fixture = await postgresFixture(context, { fail: ['ls'] })
  const postgres = new LocalPostgres(fixture.env)
  for (const work of [() => postgres.stop(), () => postgres.clean()]) {
    await assert.rejects(work(), (error) => {
      assert.match(error.message, /failed; check the container engine/)
      assert.doesNotMatch(error.message, /private-command-canary/)
      return true
    })
  }
})

test('explicit engine selection overrides Docker preference and requires a real loopback binding', async (context) => {
  const fixture = await postgresFixture(context, { binding: '0.0.0.0:55432' }, ['docker', 'podman'])
  const postgres = new LocalPostgres({ ...fixture.env, POSTGRES_CONTAINER_ENGINE: 'podman' })
  await assert.rejects(postgres.provision(), /no usable loopback port/)
  const state = await fixture.state()
  assert.ok(state.commands.every(({ engine }) => engine === 'podman'))
  assert.deepEqual(state.databases, [])
})

test('Make dry runs have no container side effects and Rust consumer failures close their database', async (context) => {
  const fixture = await postgresFixture(context)
  const exec = promisify(execFile)
  const cwd = resolve(import.meta.dirname, '..')
  await exec(
    '/usr/bin/make',
    ['-n', 'postgres-start', 'postgres-url', 'postgres-stop', 'postgres-clean', 'postgres-tests'],
    { cwd, env: fixture.env },
  )
  assert.deepEqual((await fixture.state()).commands, [])
  const consumer = join(fixture.directory, 'consumer.mjs')
  const observed = join(fixture.directory, 'consumer.json')
  await writeFile(
    consumer,
    `import { writeFileSync } from 'node:fs'; writeFileSync(${JSON.stringify(observed)}, JSON.stringify({ database: process.env.CORAL_TEST_DATABASE, url: process.env.CORAL_TEST_POSTGRES_URL })); process.exit(17)\n`,
  )
  await assert.rejects(
    exec(
      '/usr/bin/make',
      [
        '--no-print-directory',
        'postgres-tests',
        `RUST_WORKSPACE_TESTS=${process.execPath} ${consumer}`,
      ],
      { cwd, env: fixture.env, timeout: 15_000 },
    ),
    (error) => {
      assert.equal(error.code, 2)
      assert.match(error.stderr, /Error 17/)
      return true
    },
  )
  const result = JSON.parse(await readFile(observed, 'utf8'))
  assert.equal(result.database, 'postgres')
  const state = await fixture.state()
  assert.deepEqual(state.databases, [])
  assert.deepEqual(state.dropped, [new URL(result.url).pathname.slice(1)])
})

test('invalid CLI commands and port overrides fail before touching the container engine', async (context) => {
  const fixture = await postgresFixture(context)
  const exec = promisify(execFile)
  for (const args of [['unknown'], ['start', 'unexpected']]) {
    await assert.rejects(
      exec(process.execPath, [resolve(import.meta.dirname, 'postgres-cli.mjs'), ...args], {
        env: fixture.env,
      }),
      /Usage: node scripts\/postgres-cli.mjs/,
    )
  }
  for (const port of ['-1', '65536', 'not-a-port']) {
    await assert.rejects(
      new LocalPostgres({ ...fixture.env, LOCAL_POSTGRES_PORT: port }).start(),
      /LOCAL_POSTGRES_PORT/,
    )
  }
  assert.deepEqual((await fixture.state()).commands, [])
})

test('failed startup and URL inspection never create a disposable database', async (context) => {
  const fixture = await postgresFixture(context, { fail: ['run'] })
  const postgres = new LocalPostgres(fixture.env)
  await assert.rejects(postgres.url(), /no usable loopback port/)
  await assert.rejects(postgres.provision(), /Postgres startup failed/)
  const state = await fixture.state()
  assert.deepEqual(state.containers, {})
  assert.deepEqual(state.databases, [])
})

test('explicit caller-owned Postgres is validated but never provisioned or dropped', async () => {
  const postgres = new LocalPostgres({ PATH: '' })
  const lease = await postgres.provision('postgres://fixture@localhost/disposable')
  assert.equal(lease.url, 'postgres://fixture@localhost/disposable')
  await lease.close()
  for (const url of ['secret', 'https://secret@example.com/db', 'postgres://localhost/']) {
    await assert.rejects(postgres.provision(url), {
      message: 'Supply a disposable Postgres database URL',
    })
  }
})

test('missing Postgres tooling fails setup rather than passing an unrequested suite', async () => {
  await assert.rejects(new LocalPostgres({ PATH: '' }).provision(), /Postgres startup failed/)
})
