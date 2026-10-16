import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import test from 'node:test'

const script = fileURLToPath(new URL('./check-migrations.sh', import.meta.url))
const name = '0009_workspace_access_control.sql'
const sql = 'CREATE TABLE workspaces (id TEXT PRIMARY KEY);\n'
const postgres = `crates/coral-app/migrations/postgres/${name}`
const sqlite = `crates/coral-app/migrations/sqlite/${name}`
// Before the chains split, one file served both databases.
const shared = `crates/coral-app/migrations/${name}`

// A repository whose `main` holds the given migrations at the given product
// version, with a branch checked out on top of it.
async function repository(t, version, migrations = { [postgres]: sql, [sqlite]: sql }) {
  const root = await mkdtemp(join(tmpdir(), 'coral-check-migrations-'))
  t.after(() => rm(root, { recursive: true, force: true }))
  const git = (...args) => {
    const result = spawnSync('git', args, { cwd: root, encoding: 'utf8' })
    assert.equal(result.status, 0, result.stderr)
  }
  const write = async (path, content) => {
    await mkdir(dirname(join(root, path)), { recursive: true })
    await writeFile(join(root, path), content)
  }
  git('init', '--quiet', '--initial-branch=main')
  git('config', 'user.email', 'dev@example.com')
  git('config', 'user.name', 'Dev')
  await write('Cargo.toml', `[workspace.package]\nversion = "${version}" # x-release-please-version\n`)
  for (const [path, content] of Object.entries(migrations)) await write(path, content)
  git('add', '-A')
  git('commit', '--quiet', '-m', 'base')
  git('switch', '--quiet', '-c', 'change')
  return {
    write,
    remove: (path) => rm(join(root, path)),
    check: () => spawnSync(script, ['main'], { cwd: root, encoding: 'utf8' }),
  }
}

const runs = (database) => `crates/coral-run/migrations/${database}`

const changes = {
  "deletes one database's copy of a migration": (repo) => repo.remove(sqlite),
  "edits one database's copy of a migration": (repo) => repo.write(postgres, 'CREATE TABLE workspaces (id TEXT);\n'),
  'adds a migration below the highest version': async (repo) => {
    await repo.write(`${runs('postgres')}/0001_runs.sql`, 'CREATE TABLE runs (id TEXT PRIMARY KEY);\n')
    await repo.write(`${runs('sqlite')}/0001_runs.sql`, 'CREATE TABLE runs (id TEXT PRIMARY KEY);\n')
  },
  'adds a migration that no database runs': (repo) =>
    repo.write('crates/coral-run/migrations/0010_runs.sql', 'CREATE TABLE runs (id TEXT PRIMARY KEY);\n'),
  'adds a migration for only one database': (repo) =>
    repo.write(`${runs('postgres')}/0010_runs.sql`, 'CREATE TABLE runs (id TEXT PRIMARY KEY);\n'),
}

for (const [name, change] of Object.entries(changes)) {
  test(`a checkout that ${name} fails against a Coral 1.x main`, async (t) => {
    const repo = await repository(t, '1.0.0-beta.11')
    await change(repo)
    const result = repo.check()
    assert.equal(result.status, 1, result.stderr)
    assert.match(result.stderr, /check-migrations: .*(must not change|must use a version above 9|no database runs|is missing)/)
  })

  // The swap replaces Coral 0.x's whole tree, migrations included.
  test(`a checkout that ${name} passes against a Coral 0.x main`, async (t) => {
    const repo = await repository(t, '0.15.4')
    await change(repo)
    const result = repo.check()
    assert.equal(result.status, 0, result.stderr)
    assert.match(result.stderr, /forks from Coral 0\.x/)
  })
}

test('a checkout that adds the next migration for each database passes against a Coral 1.x main', async (t) => {
  const repo = await repository(t, '1.0.0-beta.11')
  await repo.write(`${runs('postgres')}/0010_runs.sql`, 'CREATE TABLE runs (id TEXT PRIMARY KEY);\n')
  await repo.write(`${runs('sqlite')}/0010_runs.sql`, 'CREATE TABLE runs (id BLOB PRIMARY KEY);\n')
  const result = repo.check()
  assert.equal(result.status, 0, result.stderr)
})

test('a checkout that moves a shared migration unchanged into both databases passes', async (t) => {
  const repo = await repository(t, '1.0.0-beta.11', { [shared]: sql })
  await repo.remove(shared)
  await repo.write(postgres, sql)
  await repo.write(sqlite, sql)
  const result = repo.check()
  assert.equal(result.status, 0, result.stderr)
})

test('a checkout that moves a shared migration but changes one copy fails', async (t) => {
  const repo = await repository(t, '1.0.0-beta.11', { [shared]: sql })
  await repo.remove(shared)
  await repo.write(postgres, sql)
  await repo.write(sqlite, 'CREATE TABLE workspaces (id TEXT);\n')
  const result = repo.check()
  assert.equal(result.status, 1, result.stderr)
  assert.match(result.stderr, new RegExp(`must not change[^]*${sqlite}`))
  assert.doesNotMatch(result.stderr, new RegExp(postgres))
})
