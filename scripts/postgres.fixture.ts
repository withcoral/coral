// Stateful stand-in for the external Docker/Podman command, shared by lifecycle
// and runner tests. It never starts a real container or touches credentials.
import { mkdtemp, readFile, rm, symlink, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import type { TestContext } from 'node:test'

export async function postgresFixture(
  test: TestContext,
  initial: Record<string, unknown> = {},
  engines = ['podman'],
) {
  const directory = await mkdtemp(join(tmpdir(), 'coral-postgres-fixture-'))
  test.after(() => rm(directory, { recursive: true, force: true }))
  const stateFile = join(directory, 'state.json')
  await writeFile(
    stateFile,
    JSON.stringify({ containers: {}, databases: [], dropped: [], commands: [], ...initial }),
  )
  const binary = `#!${process.execPath}
import { readFileSync, writeFileSync } from 'node:fs'
import { basename } from 'node:path'
const state = JSON.parse(readFileSync(process.env.POSTGRES_FIXTURE_STATE, 'utf8'))
const args = process.argv.slice(2)
state.commands.push({ engine: basename(process.argv[1]), args })
const save = () => writeFileSync(process.env.POSTGRES_FIXTURE_STATE, JSON.stringify(state))
const fail = () => { save(); console.error('private-command-canary'); process.exit(1) }
const operation = args[0] === 'exec' ? args[2] : args[0] === 'container' ? args[1] : args[0]
if (state.fail?.includes(operation)) fail()
const name = args[0] === 'exec' ? args[1] : args.at(-1)
switch (operation) {
  case '--version': console.log('fixture engine'); break
  case 'ls': console.log(Object.keys(state.containers).join('\\n')); break
  case 'run': {
    const name = args[args.indexOf('--name') + 1]
    if (state.containers[name]) fail()
    const publish = args[args.indexOf('-p') + 1]
    state.containers[name] = { running: true, image: args.at(-1), publish, port: publish.split(':')[1] || '55432' }
    break
  }
  case 'start':
    if (!state.containers[name]) fail()
    state.containers[name].running = true
    break
  case 'stop':
    if (!state.containers[name]) fail()
    state.containers[name].running = false
    break
  case 'rm': delete state.containers[name]; break
  case 'port': {
    const container = state.containers[args[1]]
    if (!container?.running) fail()
    console.log(state.binding ?? ('127.0.0.1:' + container.port))
    break
  }
  case 'pg_isready':
    if (!state.containers[name]?.running) fail()
    if (state.readinessFailures > 0) { state.readinessFailures--; fail() }
    break
  case 'createdb':
    state.databases.push(args.at(-1))
    // Model a client error after the database was actually created.
    if (state.creationCommittedThenFailed) fail()
    break
  case 'dropdb':
    state.databases = state.databases.filter(database => database !== args.at(-1))
    state.dropped.push(args.at(-1))
    break
  default: fail()
}
save()
`
  for (const engine of engines) await writeFile(join(directory, engine), binary, { mode: 0o755 })
  await symlink(process.execPath, join(directory, 'node'))
  return {
    directory,
    env: { PATH: directory, HOME: directory, POSTGRES_FIXTURE_STATE: stateFile },
    state: async () => JSON.parse(await readFile(stateFile, 'utf8')),
  }
}
