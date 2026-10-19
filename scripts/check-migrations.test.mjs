import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import test from 'node:test'

const script = fileURLToPath(new URL('./check-migrations.sh', import.meta.url))
const migration = 'crates/coral-db/migrations/0009_workspace_access_control.sql'
const runs = 'CREATE TABLE runs (id TEXT PRIMARY KEY);\n'

// A repository whose `main` holds `files` at the given product version, with a
// branch checked out on top of it.
async function repository(t, version, files = { [migration]: 'CREATE TABLE workspaces (id TEXT PRIMARY KEY);\n' }) {
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
  for (const [path, content] of Object.entries(files)) await write(path, content)
  git('add', '-A')
  git('commit', '--quiet', '-m', 'base')
  git('switch', '--quiet', '-c', 'change')
  return {
    write,
    remove: (path) => rm(join(root, path)),
    check: () => spawnSync(script, ['main'], { cwd: root, encoding: 'utf8' }),
  }
}

const changes = {
  'deletes a migration': (repo) => repo.remove(migration),
  'edits a migration': (repo) => repo.write(migration, 'CREATE TABLE workspaces (id TEXT);\n'),
  'adds a migration below the highest version': (repo) =>
    repo.write('crates/coral-db/migrations/0001_runs.sql', runs),
  'adds a migration for one database below the highest version': (repo) =>
    repo.write('crates/coral-db/migrations/sqlite/0001_runs.sql', runs),
  'adds a migration to another crate': (repo) => repo.write('crates/coral-run/migrations/0010_runs.sql', runs),
  'adds a migration to a coral-db subfolder the plan skips': (repo) =>
    repo.write('crates/coral-db/migrations/postgress/0010_runs.sql', runs),
}

for (const [name, change] of Object.entries(changes)) {
  test(`a checkout that ${name} fails against a Coral 1.x main`, async (t) => {
    const repo = await repository(t, '1.0.0-beta.11')
    await change(repo)
    const result = repo.check()
    assert.equal(result.status, 1, result.stderr)
    assert.match(result.stderr, /check-migrations: .*(must not change|must use a version above 9|never run)/)
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

const accepted = {
  'adds the next migration': (repo) => repo.write('crates/coral-db/migrations/0010_runs.sql', runs),
  'adds the next migration for each database': async (repo) => {
    await repo.write('crates/coral-db/migrations/postgres/0010_runs.sql', runs)
    await repo.write('crates/coral-db/migrations/sqlite/0010_runs.sql', runs)
  },
}

for (const [name, change] of Object.entries(accepted)) {
  test(`a checkout that ${name} passes against a Coral 1.x main`, async (t) => {
    const repo = await repository(t, '1.0.0-beta.11')
    await change(repo)
    const result = repo.check()
    assert.equal(result.status, 0, result.stderr)
  })
}

test('a checkout that starts a ledger main does not have may begin at version 0', async (t) => {
  const repo = await repository(t, '1.0.0-beta.11', {})
  await repo.write('crates/coral-db/migrations/0000_state_format.sql', runs)
  const result = repo.check()
  assert.equal(result.status, 0, result.stderr)
})
