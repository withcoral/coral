import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { readdir } from 'node:fs/promises'
import { EventEmitter, once } from 'node:events'
import { test } from 'node:test'

import { createWebRequest, startServer } from './server.js'

const REQUIRED_ENV = {
  CORAL_ENDPOINT: 'http://127.0.0.1:9',
  HOST: '127.0.0.1',
  NODE_ENV: 'production',
  PORT: '0',
  REEF_AUTH_ISSUER: 'http://127.0.0.1:9',
  REEF_AUTH_MODE: 'required',
  REEF_PUBLIC_URL: 'http://127.0.0.1:3000',
  REEF_SESSION_SECRET: '0123456789abcdef0123456789abcdef',
}
const CHILD_ENV = { ...process.env, ...REQUIRED_ENV }
delete CHILD_ENV.FORCE_COLOR

test('probes valid runtime config immediately before listen with a fresh context', async () => {
  const events = []
  const contexts = []
  const warnings = []
  const handler = async (_request, context) => {
    events.push('probe')
    contexts.push(context)
    return new Response(null, { status: 204 })
  }
  const listen = async (_server, port, host) => events.push(`listen:${host}:${port}`)
  const installShutdown = () => events.push('shutdown')

  const originalWarn = console.warn
  console.warn = (message) => warnings.push(message)
  try {
    await startServer({
      env: { REEF_AUTH_MODE: 'disabled' },
      handler,
      installShutdown,
      listen,
      server: {},
    })
  } finally {
    console.warn = originalWarn
  }

  assert.deepEqual(events, ['probe', 'listen:0.0.0.0:3000', 'shutdown'])
  assert.equal(contexts.length, 1)
  assert.match(warnings[0], /authentication is disabled/)
})

test('aborts Fetch requests only for actual Node client disconnects', () => {
  const nodeRequest = Object.assign(new EventEmitter(), {
    complete: false,
    headers: { host: 'reef.test' },
    method: 'GET',
    url: '/',
  })
  const nodeResponse = Object.assign(new EventEmitter(), { writableEnded: false })
  const aborted = createWebRequest(nodeRequest, nodeResponse)
  nodeRequest.emit('aborted')
  assert.equal(aborted.request.signal.aborted, true)
  aborted.release()

  const disconnected = createWebRequest(nodeRequest, nodeResponse)
  nodeResponse.emit('close')
  assert.equal(disconnected.request.signal.aborted, true)
  disconnected.release()

  nodeRequest.complete = true
  nodeResponse.writableEnded = true
  const completed = createWebRequest(nodeRequest, nodeResponse)
  nodeRequest.emit('close')
  nodeResponse.emit('close')
  assert.equal(completed.request.signal.aborted, false)
  completed.release()
})

test('does not listen when the runtime probe throws or returns non-2xx', async () => {
  for (const handler of [
    async () => {
      throw new Error('missing setting')
    },
    async () => new Response(null, { status: 503 }),
  ]) {
    let listened = false
    await assert.rejects(
      startServer({
        handler,
        listen: async () => {
          listened = true
        },
        server: {},
      }),
      /runtime configuration: (missing setting|health probe returned HTTP 503)/,
    )
    assert.equal(listened, false)
  }
})

test('production entry serves static files, delegates SSR, and shuts down cleanly', async () => {
  const child = spawn(process.execPath, ['server.js'], {
    cwd: import.meta.dirname,
    env: CHILD_ENV,
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  let stderr = ''
  child.stderr.setEncoding('utf8').on('data', (chunk) => (stderr += chunk))

  try {
    const port = await listeningPort(child)
    const origin = `http://127.0.0.1:${port}`
    const health = await fetch(`${origin}/healthz`)
    assert.equal(health.status, 200)
    assert.deepEqual(await health.json(), { status: 'ok' })

    const root = await fetch(origin, { redirect: 'manual' })
    assert.equal(root.status, 302)
    assert.match(root.headers.get('location') ?? '', /^\/login/)

    const favicon = await fetch(`${origin}/favicon.svg`)
    assert.equal(favicon.status, 200)
    assert.equal(favicon.headers.get('cache-control'), 'no-cache')
    const traversal = await fetch(`${origin}/assets%2f..%2ffavicon.svg`)
    assert.equal(traversal.status, 200)
    assert.equal(traversal.headers.get('cache-control'), 'no-cache')

    const [asset] = await readdir(new URL('./build/client/assets/', import.meta.url))
    const immutable = await fetch(`${origin}/assets/${asset}`)
    assert.equal(immutable.status, 200)
    assert.equal(immutable.headers.get('cache-control'), 'public, max-age=31536000, immutable')
  } finally {
    const [code, signal] = await stopChild(child)
    assert.equal(stderr, '')
    assert.equal(signal, null)
    assert.equal(code, 0)
  }
})

test('production entry names fatal runtime config and exits without listening', async () => {
  const child = spawn(process.execPath, ['server.js'], {
    cwd: import.meta.dirname,
    env: { ...CHILD_ENV, REEF_PUBLIC_URL: '' },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  const stdout = streamText(child.stdout)
  const stderr = streamText(child.stderr)
  const [code] = await once(child, 'exit')

  assert.notEqual(code, 0)
  assert.doesNotMatch(await stdout, /Reef listening/)
  const error = await stderr
  assert.match(error, /REEF_PUBLIC_URL must be set when auth is required/)
  assert.match(error, /Reef startup failed: runtime configuration: health probe returned HTTP 500/)
})

function listeningPort(child) {
  return new Promise((resolve, reject) => {
    let output = ''
    const timeout = setTimeout(
      () => reject(new Error('server did not listen within 10 seconds')),
      10_000,
    )
    child.stdout.setEncoding('utf8').on('data', (chunk) => {
      output += chunk
      const match = output.match(/Reef listening on http:\/\/127\.0\.0\.1:(\d+)/)
      if (match) {
        clearTimeout(timeout)
        resolve(Number(match[1]))
      }
    })
    child.once('exit', (code) => reject(new Error(`server exited before listening (${code})`)))
  })
}

async function streamText(stream) {
  stream.setEncoding('utf8')
  let output = ''
  for await (const chunk of stream) output += chunk
  return output
}

async function stopChild(child) {
  if (child.exitCode !== null || child.signalCode !== null) {
    return [child.exitCode, child.signalCode]
  }
  child.kill('SIGTERM')
  return once(child, 'exit')
}
