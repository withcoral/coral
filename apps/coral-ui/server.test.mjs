import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { access, readdir } from 'node:fs/promises'
import { EventEmitter, once } from 'node:events'
import { test } from 'node:test'

import { createWebRequest, startServer } from './server.js'

try {
  await Promise.all([
    access(new URL('./build/server/index.js', import.meta.url)),
    access(new URL('./build/client/assets/', import.meta.url)),
  ])
} catch {
  throw new Error(
    'Coral UI production build artifacts are missing; run `npm run build` before `npm run test:server`.',
  )
}

const REQUIRED_ENV = {
  CORAL_ENDPOINT: 'http://127.0.0.1:9',
  HOST: '127.0.0.1',
  NODE_ENV: 'production',
  PORT: '0',
  CORAL_UI_AUTH_ISSUER: 'http://127.0.0.1:9',
  CORAL_UI_AUTH_MODE: 'required',
  CORAL_UI_PUBLIC_URL: 'http://127.0.0.1:3000',
  CORAL_UI_SESSION_SECRET: '0123456789abcdef0123456789abcdef',
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
      env: { CORAL_UI_AUTH_MODE: 'disabled' },
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

test('treats a blank PORT as unset', async () => {
  let configuredPort

  await startServer({
    env: { PORT: '   ', CORAL_UI_AUTH_MODE: 'required' },
    handler: async () => new Response(null, { status: 204 }),
    installShutdown: () => undefined,
    listen: async (_server, port) => {
      configuredPort = port
    },
    server: {},
  })

  assert.equal(configuredPort, 3000)
})

test('aborts Fetch requests only for actual Node client disconnects', () => {
  const nodeRequest = Object.assign(new EventEmitter(), {
    complete: false,
    headers: { host: 'coral-ui.test' },
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
    env: { ...CHILD_ENV, CORAL_UI_PUBLIC_URL: '' },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  const stdout = streamText(child.stdout)
  const stderr = streamText(child.stderr)
  const [code] = await once(child, 'exit')

  assert.notEqual(code, 0)
  assert.doesNotMatch(await stdout, /Coral UI listening/)
  const error = await stderr
  assert.match(error, /CORAL_UI_PUBLIC_URL must be set when auth is required/)
  assert.match(
    error,
    /Coral UI startup failed: runtime configuration: health probe returned HTTP 500/,
  )
})

test('stops waiting for a listening port when the child exits', async () => {
  const child = spawn(process.execPath, ['-e', 'process.exit(2)'], {
    stdio: ['ignore', 'pipe', 'pipe'],
  })

  await assert.rejects(listeningPort(child), /server exited before listening \(2\)/)
})

function listeningPort(child) {
  return new Promise((resolve, reject) => {
    let output = ''
    const cleanup = () => {
      clearTimeout(timeout)
      child.stdout.off('data', onData)
      child.off('exit', onExit)
    }
    const onData = (chunk) => {
      output += chunk
      const match = output.match(/Coral UI listening on http:\/\/127\.0\.0\.1:(\d+)/)
      if (match) {
        cleanup()
        resolve(Number(match[1]))
      }
    }
    const onExit = (code) => {
      cleanup()
      reject(new Error(`server exited before listening (${code})`))
    }
    const timeout = setTimeout(() => {
      cleanup()
      reject(new Error('server did not listen within 10 seconds'))
    }, 10_000)
    child.stdout.setEncoding('utf8').on('data', onData)
    child.once('exit', onExit)
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
