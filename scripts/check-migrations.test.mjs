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
    git,
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
  'adds a migration with a pgvector column': (repo) =>
    repo.write(
      'crates/coral-db/migrations/postgres/0010_embeddings.sql',
      'ALTER TABLE runs ADD COLUMN embedding halfvec(384);\n',
    ),
  'adds a migration with a pgvector column of no fixed dimension': (repo) =>
    repo.write(
      'crates/coral-db/migrations/postgres/0010_embeddings.sql',
      'ALTER TABLE runs ADD COLUMN embedding vector NOT NULL;\n',
    ),
  'adds a migration that creates pgvector': (repo) =>
    repo.write('crates/coral-db/migrations/postgres/0010_vector.sql', 'CREATE EXTENSION IF NOT EXISTS vector;\n'),
}

for (const [name, change] of Object.entries(changes)) {
  test(`a checkout that ${name} fails against a Coral 1.x main`, async (t) => {
    const repo = await repository(t, '1.0.0-beta.11')
    await change(repo)
    const result = repo.check()
    assert.equal(result.status, 1, result.stderr)
    assert.match(result.stderr, /check-migrations: .*(must not change|must use a version above 9|never run|name pgvector)/)
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
  'adds the next migration with a tsvector column': (repo) =>
    repo.write(
      'crates/coral-db/migrations/postgres/0010_runs.sql',
      "ALTER TABLE runs ADD COLUMN tsv tsvector GENERATED ALWAYS AS (to_tsvector('simple', id)) STORED;\n",
    ),
  'adds the next migration with a text vector named vector': (repo) =>
    repo.write(
      'crates/coral-db/migrations/postgres/0010_runs.sql',
      'CREATE FUNCTION lexemes(vector TSVECTOR) RETURNS BIGINT\n' +
        "LANGUAGE sql IMMUTABLE AS 'SELECT count(*) FROM unnest(vector)';\n",
    ),
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

// The statements and the declarations are tested in
// migration-compatibility.test.mjs. These cases cover what only the script
// does: it runs the check, reads the release tags and skips the SQLite halves.
const next = 'crates/coral-db/migrations/0010_runs.sql'
const drop = 'ALTER TABLE workspaces DROP COLUMN name;\n'

test('a checkout whose migration can break an older release fails', async (t) => {
  const repo = await repository(t, '1.0.0-beta.11')
  await repo.write(next, drop)
  const result = repo.check()
  assert.equal(result.status, 1, result.stderr)
  assert.match(result.stderr, /0010_runs\.sql can break an older Coral/)
  assert.match(result.stderr, /drops a column/)
})

// A release user upgrades from one release to the next. A removal is safe for
// them only when a release has stopped using what it removes and does not
// remove it yet: a release that has the named step.
test('a removal passes only when it names a step that a release has', async (t) => {
  const marker = 'crates/coral-db/migrations/0010_stop_using_workspace_names.sql'
  const removal = 'crates/coral-db/migrations/0011_runs.sql'
  const declared = (step) => `-- rolling-upgrade: unused since step ${step}. No query names the column.\n${drop}`
  const repo = await repository(t, '1.0.0-beta.11')
  repo.git('switch', '--quiet', 'main')
  repo.git('tag', 'v1.0.0')
  await repo.write(marker, '-- Stops using workspaces.name.\n')
  repo.git('add', '-A')
  repo.git('commit', '--quiet', '-m', 'stop using the column')
  repo.git('switch', '--quiet', '-c', 'removal')

  // The marker is on main, and no release has it.
  await repo.write(removal, declared('0010'))
  const unreleased = repo.check()
  assert.equal(unreleased.status, 1, unreleased.stderr)
  assert.match(unreleased.stderr, /names step 0010, which no release has \(the newest release ends at step 0009\)/)

  // A pre-release is not a release that users upgrade through.
  repo.git('tag', 'v1.1.0-beta.1', 'main')
  assert.equal(repo.check().status, 1)

  repo.git('tag', 'v1.1.0', 'main')
  const accepted = repo.check()
  assert.equal(accepted.status, 0, accepted.stderr)

  // The removal's own step, and a step that does not exist.
  for (const step of ['0011', '0099']) {
    await repo.write(removal, declared(step))
    const result = repo.check()
    assert.equal(result.status, 1, result.stderr)
    assert.match(result.stderr, new RegExp(`names step ${step}, which no release has`))
  }
})

test('a table rebuild in the SQLite half of an additive step passes', async (t) => {
  const repo = await repository(t, '1.0.0-beta.11')
  await repo.write('crates/coral-db/migrations/postgres/0010_runs.sql', 'ALTER TABLE workspaces ADD COLUMN owner TEXT;\n')
  await repo.write(
    'crates/coral-db/migrations/sqlite/0010_runs.sql',
    'CREATE TABLE workspaces_new (id TEXT PRIMARY KEY, owner TEXT);\n' +
      'INSERT INTO workspaces_new (id) SELECT id FROM workspaces;\n' +
      'DROP TABLE workspaces;\nALTER TABLE workspaces_new RENAME TO workspaces;\n',
  )
  const result = repo.check()
  assert.equal(result.status, 0, result.stderr)
})
