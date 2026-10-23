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
const workspaces = 'CREATE TABLE workspaces (id TEXT PRIMARY KEY);\n'

// A repository whose `main` holds `files` at the given product version, with a
// branch checked out on top of it.
async function repository(t, version, files = { [migration]: workspaces }) {
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
    // Commits `change` to main after this branch forked from it.
    advanceMain: async (change) => {
      git('switch', '--quiet', 'main')
      await change()
      git('add', '-A')
      git('commit', '--quiet', '-m', 'main')
      git('switch', '--quiet', 'change')
    },
    check: () => spawnSync(script, ['main'], { cwd: root, encoding: 'utf8' }),
  }
}

const changes = {
  'deletes a migration': (repo) => repo.remove(migration),
  'renames a migration above the highest version': async (repo) => {
    await repo.remove(migration)
    await repo.write('crates/coral-db/migrations/0010_workspace_access_control.sql', workspaces)
  },
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

const halves = {
  'crates/coral-db/migrations/postgres/0009_runs.sql': 'crates/coral-db/migrations/postgres/0010_runs.sql',
  'crates/coral-db/migrations/sqlite/0009_runs.sql': 'crates/coral-db/migrations/sqlite/0010_runs.sql',
}
const specificNine = Object.fromEntries(Object.keys(halves).map((half) => [half, runs]))
// A shared step and a database-specific step that both use version 9.
const sharedNine = { [migration]: workspaces, ...specificNine }
const sharedStep = { [migration]: 'crates/coral-db/migrations/0010_workspace_access_control.sql' }

// Each rename deletes a file on main and adds its bytes, or `content`, as
// another. `mainRenames` lands on main after the checkout forks.
const moves = [
  { name: 'moves the shared step off a version two steps share', main: sharedNine, renames: sharedStep, status: 0 },
  {
    name: 'moves the database-specific step off a version two steps share',
    main: sharedNine,
    renames: halves,
    status: 0,
  },
  {
    name: 'moves a step off a shared version with other bytes',
    main: sharedNine,
    renames: sharedStep,
    content: runs,
    status: 1,
  },
  {
    name: "moves a shared step off a shared version into one database's folder",
    main: sharedNine,
    renames: { [migration]: 'crates/coral-db/migrations/sqlite/0010_workspace_access_control.sql' },
    status: 1,
  },
  // Two steps share version 9 and their bytes, and both disappear into one.
  {
    name: 'moves two steps off a shared version into one',
    main: { [migration]: runs, 'crates/coral-db/migrations/0009_runs.sql': runs },
    renames: {
      [migration]: 'crates/coral-db/migrations/0010_runs.sql',
      'crates/coral-db/migrations/0009_runs.sql': 'crates/coral-db/migrations/0010_runs.sql',
    },
    status: 1,
  },
  {
    name: 'moves a step main has already moved',
    main: sharedNine,
    mainRenames: sharedStep,
    renames: { [migration]: 'crates/coral-db/migrations/0011_workspace_access_control.sql' },
    status: 1,
  },
  // The two halves of one step share its name, so they share no version.
  {
    name: 'moves a database-specific step whose version no other step shares',
    main: specificNine,
    renames: halves,
    status: 1,
  },
]

for (const { name, main, mainRenames, renames, content, status } of moves) {
  test(`a checkout that ${name} ${status ? 'fails' : 'passes'} against a Coral 1.x main`, async (t) => {
    const repo = await repository(t, '1.0.0-beta.11', main)
    const rename = async (renames, content) => {
      for (const [from, to] of Object.entries(renames)) {
        await repo.remove(from)
        await repo.write(to, content ?? main[from])
      }
    }
    if (mainRenames) await repo.advanceMain(() => rename(mainRenames))
    await rename(renames, content)
    const result = repo.check()
    assert.equal(result.status, status, result.stderr)
    if (status) assert.match(result.stderr, /must not change/)
  })
}

test('a checkout that starts a ledger main does not have may begin at version 0', async (t) => {
  const repo = await repository(t, '1.0.0-beta.11', {})
  await repo.write('crates/coral-db/migrations/0000_state_format.sql', runs)
  const result = repo.check()
  assert.equal(result.status, 0, result.stderr)
})
