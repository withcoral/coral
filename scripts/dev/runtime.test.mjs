import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import { EventEmitter } from 'node:events'
import { existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { createServer } from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import test from 'node:test'
import {
  assertPortsFree,
  commands,
  configureHerdrPanes,
  configureRexPanes,
  supervise,
  waitForReady,
} from './runtime.mjs'

async function eventually(predicate) {
  for (let i = 0; i < 200; i++) {
    if (predicate()) return
    await delay(10)
  }
  throw new Error('Timed out waiting for fixture process')
}
function directory(t) {
  const path = mkdtempSync(join(tmpdir(), 'coral-dev-process-'))
  t.after(() => rmSync(path, { recursive: true, force: true }))
  return path
}
function node(code) {
  return { command: process.execPath, args: ['-e', code], env: process.env }
}

test('launch specifications isolate state, pass secrets only as environment, and force hosted Reef', () => {
  const root = '/checkout with spaces'
  const secret = 'dummy-filesystem-only-secret'
  const config = {
    input: { WORKOS_CONNECT_CLIENT_SECRET: secret, TAILNET_HOST: 'developer.example.ts.net' },
    webEnv: { CORAL_WEB_AUTH_MODE: 'required', CORAL_ENDPOINT: 'http://127.0.0.1:14555' },
  }
  const specs = commands(root, config, { CORAL_CONFIG_DIR: '/do-not-touch' })
  assert.equal(specs.coral.env.CORAL_CONFIG_DIR, '/checkout with spaces/.dev/state')
  assert.equal(specs.coral.env.WORKOS_CONNECT_CLIENT_SECRET, secret)
  assert.ok(!specs.coral.args.join(' ').includes(secret))
  assert.equal(specs.reef.env.__VITE_ADDITIONAL_SERVER_ALLOWED_HOSTS, 'developer.example.ts.net')
  assert.ok(specs.reef.args.includes('/checkout with spaces/apps/reef'))
  assert.ok(!Object.values(specs.reef.env).includes(secret))
})

test('busy ports fail without killing the existing listener', async (t) => {
  const server = createServer()
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
  t.after(() => new Promise((resolve) => server.close(resolve)))
  await assert.rejects(assertPortsFree([server.address().port]), /already occupied/)
  assert.ok(server.listening)
})

test('readiness waits for every endpoint and is bounded and cancelable', async () => {
  let calls = 0
  await waitForReady(['https://one.test', 'https://two.test'], new AbortController().signal, {
    interval: 1,
    timeout: 1000,
    request: async (_url, options) => {
      assert.equal(options.redirect, 'error')
      assert.ok(options.signal)
      calls++
      return new Response('', { status: calls < 4 ? 503 : 200 })
    },
  })
  assert.ok(calls >= 4)
  await assert.rejects(
    waitForReady(['https://not-ready.test'], new AbortController().signal, {
      interval: 1,
      timeout: 10,
      request: async () => new Response('', { status: 503 }),
    }),
    /startup deadline/,
  )
  const controller = new AbortController()
  controller.abort()
  await assert.rejects(waitForReady([], controller.signal), { name: 'AbortError' })
})

test('Ctrl-C stops both child process groups', { timeout: 10_000 }, async (t) => {
  const root = directory(t)
  const signals = new EventEmitter()
  const specs = ['coral', 'reef'].map((name) =>
    node(`
    const fs = require('node:fs');
    process.on('SIGINT', () => { fs.writeFileSync(${JSON.stringify(join(root, name + '.stopped'))}, 'SIGINT'); process.exit(0) });
    fs.writeFileSync(${JSON.stringify(join(root, name + '.ready'))}, 'ready');
    setInterval(() => {}, 1000);
  `),
  )
  const running = supervise(specs, { cwd: root, signals, grace: 300 })
  try {
    await eventually(() =>
      specs.every((_, i) => existsSync(join(root, ['coral', 'reef'][i] + '.ready'))),
    )
    signals.emit('SIGINT')
    assert.equal(await running, 130)
    assert.equal(readFileSync(join(root, 'coral.stopped'), 'utf8'), 'SIGINT')
    assert.equal(readFileSync(join(root, 'reef.stopped'), 'utf8'), 'SIGINT')
    assert.equal(signals.listenerCount('SIGINT'), 0)
  } finally {
    signals.emit('SIGTERM')
    await running
  }
})

test(
  'one server exiting stops the other and preserves the failure code',
  { timeout: 10_000 },
  async (t) => {
    const root = directory(t)
    const signals = new EventEmitter()
    const ready = join(root, 'ready')
    const stopped = join(root, 'stopped')
    const peer = node(
      `const fs=require('node:fs'); process.on('SIGTERM',()=>{fs.writeFileSync(${JSON.stringify(stopped)},'stopped');process.exit(0)});fs.writeFileSync(${JSON.stringify(ready)},'ready');setInterval(()=>{},1000)`,
    )
    const failure = node(
      `const fs=require('node:fs');setInterval(()=>{if(fs.existsSync(${JSON.stringify(ready)}))process.exit(7)},5)`,
    )
    const running = supervise([peer, failure], { cwd: root, signals, grace: 300 })
    try {
      assert.equal(await running, 7)
      assert.equal(readFileSync(stopped, 'utf8'), 'stopped')
    } finally {
      signals.emit('SIGTERM')
      await running
    }
  },
)

test(
  'shutdown reaches descendants even when their parent does not forward signals',
  { timeout: 10_000 },
  async (t) => {
    const root = directory(t)
    const signals = new EventEmitter()
    const ready = join(root, 'grandchild.ready')
    const stopped = join(root, 'grandchild.stopped')
    const grandchild = `const fs=require('node:fs');process.on('SIGTERM',()=>{fs.writeFileSync(${JSON.stringify(stopped)},'stopped');process.exit(0)});fs.writeFileSync(${JSON.stringify(ready)},'ready');setInterval(()=>{},1000)`
    const parent = node(
      `require('node:child_process').spawn(process.execPath,['-e',${JSON.stringify(grandchild)}],{stdio:'ignore'});process.on('SIGTERM',()=>process.exit(0));setInterval(()=>{},1000)`,
    )
    const running = supervise([parent], { cwd: root, signals, grace: 300 })
    try {
      await eventually(() => existsSync(ready))
      signals.emit('SIGTERM')
      assert.equal(await running, 143)
      assert.equal(readFileSync(stopped, 'utf8'), 'stopped')
    } finally {
      signals.emit('SIGTERM')
      await running
    }
  },
)

test('Herdr refuses outside/agent/split contexts and uses returned IDs without stealing focus', () => {
  assert.throws(
    () => configureHerdrPanes('/repo', () => assert.fail('must not inspect Herdr'), false),
    /inside/,
  )
  for (const [agent, count] of [
    ['pi', 1],
    [undefined, 2],
  ]) {
    const calls = []
    assert.throws(
      () =>
        configureHerdrPanes(
          '/repo',
          (args) => {
            calls.push(args)
            return args[1] === 'current'
              ? { result: { pane: { pane_id: 'actual:left', agent } } }
              : { result: { layout: { panes: Array(count).fill({}) } } }
          },
          true,
        ),
      /ordinary shell/,
    )
    assert.ok(!calls.some((args) => args[1] === 'split'))
  }
  const calls = []
  configureHerdrPanes(
    '/repo with spaces',
    (args) => {
      calls.push(args)
      if (args[1] === 'current') return { result: { pane: { pane_id: 'actual:left' } } }
      if (args[1] === 'layout') return { result: { layout: { panes: [{}] } } }
      if (args[1] === 'split') return { result: { pane: { pane_id: 'returned:right' } } }
      return {}
    },
    true,
  )
  assert.deepEqual(calls[2], [
    'pane',
    'split',
    '--current',
    '--direction',
    'right',
    '--cwd',
    '/repo with spaces',
    '--no-focus',
  ])
  assert.deepEqual(calls[3], ['pane', 'rename', 'actual:left', 'Coral'])
  assert.deepEqual(calls[4], ['pane', 'rename', 'returned:right', 'Reef'])
  assert.deepEqual(calls[5], ['pane', 'run', 'returned:right', "'/repo with spaces/dev' reef"])
})

test('Rex refuses outside/split contexts and uses returned IDs without stealing focus', () => {
  assert.throws(
    () =>
      configureRexPanes('/repo', () => assert.fail('must not inspect Rex'), undefined, 'b:left'),
    /inside/,
  )
  const windowWith = (ids) => ({
    windows: [
      { layers: [{ blocks: [{ block_id: 'b:other' }] }] },
      { layers: [{ blocks: ids.map((block_id) => ({ block_id })) }] },
    ],
  })
  for (const ids of [['b:left', 'b:extra'], []]) {
    const calls = []
    assert.throws(
      () =>
        configureRexPanes(
          '/repo',
          (args) => {
            calls.push(args)
            return windowWith(ids)
          },
          's:1',
          'b:left',
        ),
      /fresh tab/,
    )
    assert.deepEqual(calls, [['session', 'inspect', 's:1', '--json']])
  }
  const calls = []
  configureRexPanes(
    '/repo with spaces',
    (args) => {
      calls.push(args)
      if (args[0] === 'session') return windowWith(['b:left'])
      if (args[0] === 'split') return { block_ids: ['b:right'] }
      return {}
    },
    's:1',
    'b:left',
  )
  assert.deepEqual(calls, [
    ['session', 'inspect', 's:1', '--json'],
    [
      'split',
      '-s',
      's:1',
      '-b',
      'b:left',
      '--split',
      'right',
      '-c',
      '/repo with spaces',
      '--label',
      'Reef',
      '--focus=false',
      '--json',
    ],
    ['block', 'rename', '-s', 's:1', 'b:left', 'Coral'],
    ['send', '-s', 's:1', '-b', 'b:right', "'/repo with spaces/dev' reef"],
    ['send-key', '-s', 's:1', '-b', 'b:right', 'enter'],
  ])
})

test('./dev run --rex refuses outside Rex before any other check', () => {
  const env = { ...process.env }
  delete env.REX_SESSION
  delete env.REX_BLOCK
  const result = spawnSync(
    process.execPath,
    [join(import.meta.dirname, 'cli.mjs'), 'run', '--rex'],
    { encoding: 'utf8', env },
  )
  assert.match(result.stderr, /Run --rex only inside a fresh Rex shell tab/)
  assert.equal(result.status, 1)
})
