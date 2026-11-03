import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { access, readdir } from 'node:fs/promises'
import { EventEmitter, once } from 'node:events'
import { createServer } from 'node:http'
import { connect } from 'node:net'
import { test } from 'node:test'
import { createServer as createHttp2Server } from 'node:http2'
import { createCipheriv, createHash, randomBytes } from 'node:crypto'

import { createCookie } from 'react-router'

import { createRequestListener, createWebRequest, startServer } from './server.js'

try {
  await Promise.all([
    access(new URL('./server.mjs', import.meta.url)),
    access(new URL('./build/client/assets/', import.meta.url)),
  ])
} catch {
  throw new Error(
    'Coral web production build artifacts are missing; run `npm run build` before `npm run test:server`.',
  )
}

const REQUIRED_ENV = {
  CORAL_ENDPOINT: 'http://127.0.0.1:9',
  HOST: '127.0.0.1',
  NODE_ENV: 'production',
  PORT: '0',
  CORAL_WEB_AUTH_ISSUER: 'http://127.0.0.1:9',
  CORAL_WEB_AUTH_MODE: 'required',
  CORAL_WEB_PUBLIC_URL: 'http://127.0.0.1:3000',
  CORAL_WEB_SESSION_SECRET: '0123456789abcdef0123456789abcdef',
}
const CHILD_ENV = { ...process.env, ...REQUIRED_ENV }
delete CHILD_ENV.FORCE_COLOR
for (const name of Object.keys(CHILD_ENV)) {
  if (name.startsWith('OTEL_')) delete CHILD_ENV[name]
}

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
      env: { CORAL_WEB_AUTH_MODE: 'disabled' },
      handler,
      installShutdown,
      listen,
      server: {},
    })
  } finally {
    console.warn = originalWarn
  }

  assert.deepEqual(events, ['probe', 'listen:127.0.0.1:3000', 'shutdown'])
  assert.equal(contexts.length, 1)
  assert.match(warnings[0], /authentication is disabled/)
})

test('production local Reef rejects non-loopback binds before listening', async () => {
  for (const host of ['0.0.0.0', '::', '192.0.2.1', 'localhost.example']) {
    const child = spawn(process.execPath, ['server.mjs'], {
      cwd: import.meta.dirname,
      env: { ...CHILD_ENV, CORAL_WEB_AUTH_MODE: 'disabled', HOST: host },
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    const stdout = streamText(child.stdout)
    const stderr = streamText(child.stderr)
    const [code] = await once(child, 'exit')
    assert.notEqual(code, 0)
    assert.doesNotMatch(await stdout, /Coral web listening/)
    assert.match(await stderr, /local Reef.*loopback/)
  }
})

test('production local Reef defaults to a usable loopback listener', async () => {
  const child = spawn(process.execPath, ['server.mjs'], {
    cwd: import.meta.dirname,
    env: { ...CHILD_ENV, CORAL_WEB_AUTH_MODE: 'disabled', HOST: '' },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  const stderr = streamText(child.stderr)
  try {
    const port = await listeningPort(child)
    assert.equal((await fetch(`http://127.0.0.1:${port}/healthz`)).status, 200)
    assert.equal((await rawRequest(port, '/healthz', `rebind.attacker.test:${port}`)).status, 421)
  } finally {
    const [code] = await stopChild(child)
    assert.equal(code, 0)
  }
  assert.match(await stderr, /authentication is disabled/)
})

test('local Reef answers only Host headers that name its own loopback listener', async () => {
  const reached = []
  const handler = async (request) => {
    reached.push(request.url)
    return new Response(null, { status: 204 })
  }
  // A target that starts with // or /\ is a path on this listener, never another
  // host: React Router checks a mutation's Origin against the request URL.
  for (const [requireListenerHost, host, target, expected, path = target] of [
    [true, (port) => `127.0.0.1:${port}`, '/', 204],
    [true, (port) => `localhost:${port}`, '/', 204],
    [true, (port) => `LocalHost:${port}`, '/', 204],
    [true, (port) => `rebind.attacker.test:${port}`, '/', 421],
    [true, (port) => `rebind.attacker.test:${port}`, '/favicon.svg', 421],
    [true, () => '127.0.0.1:1', '/', 421],
    [true, () => '127.0.0.1', '/', 421],
    [true, () => undefined, '/', 421],
    [true, (port) => `127.0.0.1:${port}`, 'http://rebind.attacker.test/', 421],
    [false, (port) => `rebind.attacker.test:${port}`, '/', 204],
    [true, (port) => `127.0.0.1:${port}`, '//attacker.test/workspaces', 204],
    [
      true,
      (port) => `127.0.0.1:${port}`,
      '/\\attacker.test/workspaces',
      204,
      '//attacker.test/workspaces',
    ],
    [false, (port) => `127.0.0.1:${port}`, '//attacker.test/workspaces', 204],
  ]) {
    const server = createServer(createRequestListener({ handler, requireListenerHost }))
    server.listen(0, '127.0.0.1')
    await once(server, 'listening')
    const { port } = server.address()
    const label = `${requireListenerHost ? 'guarded' : 'unguarded'} ${host(port) ?? 'no Host'} ${target}`
    reached.length = 0
    try {
      const response = await rawRequest(port, target, host(port))
      assert.equal(response.status, expected, label)
      if (expected === 421) {
        assert.equal(response.body, '', label)
        assert.deepEqual(reached, [], label)
      } else {
        assert.deepEqual(reached, [`http://${host(port).toLowerCase()}${path}`], label)
      }
    } finally {
      await new Promise((resolve) => server.close(resolve))
    }
  }
})

test('treats a blank PORT as unset', async () => {
  let configuredPort

  await startServer({
    env: { PORT: '   ', CORAL_WEB_AUTH_MODE: 'required' },
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
    headers: { host: 'coral-web.test' },
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
  const child = spawn(process.execPath, ['server.mjs'], {
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
    const readiness = await fetch(`${origin}/readyz`)
    assert.equal(readiness.status, 503)
    assert.deepEqual(await readiness.json(), { status: 'unavailable' })
    assert.equal((await fetch(`${origin}/healthz`)).status, 200)

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

test('expired-session data requests return from login to document URLs', async () => {
  // Use the real middleware boundary: React Router passes the raw .data request
  // separately from its normalized application URL. A direct loader test misses it.
  const child = spawn(process.execPath, ['server.mjs'], {
    cwd: import.meta.dirname,
    env: CHILD_ENV,
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  const stderr = streamText(child.stderr)
  try {
    const origin = `http://127.0.0.1:${await listeningPort(child)}`
    for (const cookie of ['', 'coral_web_session=expired-session']) {
      for (const [pathname, returnTo] of [
        ['/.data', '/'],
        ['/workspaces/repro.data', '/workspaces/repro'],
        [
          '/workspaces/repro.data?tab=members&_routes=routes%2Fworkspace',
          '/workspaces/repro?tab=members',
        ],
        ['/workspaces/repro.data?index&tab=members', '/workspaces/repro?tab=members'],
        ['/workspaces/repro/_.data?tab=members', '/workspaces/repro/?tab=members'],
      ]) {
        const response = await fetch(`${origin}${pathname}`, {
          headers: { Cookie: cookie },
          redirect: 'manual',
        })
        assert.equal(response.status, 202, pathname)
        // Single-fetch redirects encode their destination in the turbo-stream
        // string table instead of an HTTP Location header.
        const values = JSON.parse(await response.text())
        const location = values.find(
          (value) => typeof value === 'string' && value.startsWith('/login?'),
        )
        assert.ok(location, `missing login redirect for ${pathname}`)
        assert.equal(new URL(location, origin).searchParams.get('returnTo'), returnTo, pathname)
        assert.equal(response.headers.get('cache-control'), 'private, no-store')
      }
    }
  } finally {
    await stopChild(child)
    await stderr
  }
})

test('rejected Coral sessions clear the cookie and offer sign in instead of a generic error', async () => {
  let grpcStatus = '16'
  const calls = []
  const sessions = new Set()
  const peer = createHttp2Server()
  peer.on('session', (session) => {
    sessions.add(session)
    session.on('close', () => sessions.delete(session))
  })
  peer.on('stream', (stream, headers) => {
    calls.push(headers[':path'])
    stream.on('error', () => undefined)
    stream.resume()
    stream.respond({
      ':status': 200,
      'content-type': 'application/grpc',
      'grpc-status': grpcStatus,
      'grpc-message': 'Rejected by test peer',
    })
    stream.end()
  })
  peer.listen(0, '127.0.0.1')
  await once(peer, 'listening')
  const child = spawn(process.execPath, ['server.mjs'], {
    cwd: import.meta.dirname,
    env: { ...CHILD_ENV, CORAL_ENDPOINT: `http://127.0.0.1:${peer.address().port}` },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  const stderr = streamText(child.stderr)
  try {
    const origin = `http://127.0.0.1:${await listeningPort(child)}`
    const cookie = await validSessionCookie()
    let csrfCookie
    const workspace = '/workspaces/00000000-0000-4000-8000-000000000001'
    // Authorization denials and outages are not session expiry. Keep the session.
    for (const [code, status] of [
      ['7', 403],
      ['14', 503],
    ]) {
      grpcStatus = code
      const response = await fetch(`${origin}${workspace}`, {
        headers: { Cookie: cookie },
        redirect: 'manual',
      })
      assert.equal(response.status, status)
      assert.equal(response.headers.get('location'), null)
      assert.doesNotMatch(response.headers.get('set-cookie') ?? '', /Max-Age=0/)
      csrfCookie = response.headers
        .getSetCookie()
        .find((value) => value.startsWith('coral_web_session_csrf='))
        ?.split(';')[0]
      await response.text()
    }
    assert.ok(csrfCookie)
    const csrf = await createCookie('coral_web_session_csrf', {
      secrets: [REQUIRED_ENV.CORAL_WEB_SESSION_SECRET],
    }).parse(csrfCookie)
    grpcStatus = '16'
    for (const [pathname, returnTo, method] of [
      [`${workspace}/activity`, `${workspace}/activity`, 'GET'],
      [`${workspace}/activity.data`, `${workspace}/activity`, 'GET'],
      [`${workspace}?tab=members`, `${workspace}?tab=members`, 'GET'],
      [
        `${workspace}.data?tab=members&_routes=routes%2Fworkspace`,
        `${workspace}?tab=members`,
        'GET',
      ],
      ['/workspaces', '/workspaces', 'POST'],
      ['/workspaces.data', '/workspaces', 'POST'],
    ]) {
      const dataRequest = new URL(pathname, origin).pathname.endsWith('.data')
      calls.length = 0
      const response = await fetch(`${origin}${pathname}`, {
        method,
        headers: { Cookie: `${cookie}; ${csrfCookie}`, Origin: REQUIRED_ENV.CORAL_WEB_PUBLIC_URL },
        body:
          method === 'POST'
            ? new URLSearchParams({
                csrf: csrf.token,
                intent: 'createWorkspace',
                displayName: 'Do not replay',
                requestId: '00000000-0000-4000-8000-000000000002',
              })
            : undefined,
        redirect: 'manual',
      })
      assert.equal(response.status, dataRequest ? 202 : method === 'POST' ? 303 : 302, pathname)
      assert.match(response.headers.get('set-cookie') ?? '', /coral_web_session=; Max-Age=0/)
      assert.match(response.headers.get('set-cookie') ?? '', /coral_web_session_csrf=; Max-Age=0/)
      assert.equal(response.headers.get('cache-control'), 'private, no-store')
      assert.match(response.headers.get('vary') ?? '', /Cookie/i)
      if (method === 'POST') {
        assert.equal(calls.filter((path) => path.endsWith('/CreateWorkspace')).length, 1)
      }
      const location = dataRequest
        ? JSON.parse(await response.text()).find(
            (value) => typeof value === 'string' && value.startsWith('/login?'),
          )
        : response.headers.get('location')
      assert.ok(location)
      const loginUrl = new URL(location, origin)
      assert.equal(loginUrl.searchParams.get('returnTo'), returnTo)
      assert.equal(loginUrl.searchParams.get('sessionExpired'), '1')
      const login = await fetch(loginUrl, { redirect: 'manual' })
      assert.equal(login.status, 200)
      const html = await login.text()
      assert.match(html, /Your session expired/)
      assert.doesNotMatch(html, /An unexpected error occurred/)
      if (returnTo !== '/') assert.ok(html.includes(`value="${returnTo}"`))
    }
  } finally {
    await stopChild(child)
    await stderr
    for (const session of sessions) session.destroy()
    await new Promise((resolve) => peer.close(resolve))
  }
})

// A browser-valid, unexpired cookie whose token Coral no longer recognizes.
async function validSessionCookie() {
  const secret = REQUIRED_ENV.CORAL_WEB_SESSION_SECRET
  const iv = randomBytes(12)
  const cipher = createCipheriv('aes-256-gcm', createHash('sha256').update(secret).digest(), iv)
  const ciphertext = Buffer.concat([
    cipher.update(
      JSON.stringify({
        accessToken: 'deleted-database-token',
        expiresAt: Math.floor(Date.now() / 1000) + 3600,
        tokenType: 'Bearer',
      }),
    ),
    cipher.final(),
  ])
  const encrypted = [iv, cipher.getAuthTag(), ciphertext]
    .map((part) => part.toString('base64url'))
    .join('.')
  return (
    await createCookie('coral_web_session', { secrets: [secret] }).serialize(encrypted)
  ).split(';')[0]
}

test('proxied logout accepts the runtime public host without trusting forwarded origins', async () => {
  // Exercise the real React Router handler: calling the action directly bypasses
  // the framework origin check that used to return 400 before logout could run.
  for (const publicUrl of ['https://reef.example.test', 'https://reef.example.test:8443']) {
    const child = spawn(process.execPath, ['server.mjs'], {
      cwd: import.meta.dirname,
      env: { ...CHILD_ENV, CORAL_WEB_PUBLIC_URL: publicUrl },
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    const stderr = streamText(child.stderr)
    try {
      const origin = `http://127.0.0.1:${await listeningPort(child)}`
      for (const pathname of ['/logout.data', '/logout']) {
        for (const [submittedOrigin, expectedStatus] of [
          [publicUrl, 403],
          [origin, 403],
          ['https://attacker.example.test', 400],
          ['https://sub.reef.example.test', 400],
          ['https://reef.example.test.attacker.test', 400],
        ]) {
          const response = await fetch(`${origin}${pathname}`, {
            method: 'POST',
            headers: {
              Origin: submittedOrigin,
              'X-Forwarded-Host': new URL(submittedOrigin).host,
              'X-Forwarded-Proto': 'https',
            },
            body: new URLSearchParams({ csrf: 'invalid-token' }),
            redirect: 'manual',
          })
          // A dummy session/token must reach logout's own CSRF rejection, not
          // React Router's generic Bad Request. Other origins remain blocked.
          assert.equal(response.status, expectedStatus, `${pathname} from ${submittedOrigin}`)
          if (expectedStatus === 403) assert.match(await response.text(), /Invalid CSRF token/)
        }
      }
    } finally {
      await stopChild(child)
      await stderr
    }
  }
})

test('Reef readiness follows private gRPC health and bounds a stalled call', async () => {
  // This is a separate native gRPC peer, not a mock of Reef's client module.
  // HealthCheckResponse.status is protobuf field 1 (varint); gRPC adds a 5-byte frame.
  let status = 1
  const sessions = new Set()
  const peer = createHttp2Server()
  peer.on('session', (session) => {
    sessions.add(session)
    session.on('close', () => sessions.delete(session))
  })
  peer.on('stream', (stream, headers) => {
    assert.equal(headers[':path'], '/grpc.health.v1.Health/Check')
    stream.on('error', () => undefined)
    stream.resume()
    stream.respond(
      { ':status': 200, 'content-type': 'application/grpc' },
      { waitForTrailers: true },
    )
    stream.on('wantTrailers', () => stream.sendTrailers({ 'grpc-status': '0' }))
    if (status !== null) stream.end(Buffer.from([0, 0, 0, 0, 2, 8, status]))
  })
  peer.listen(0, '127.0.0.1')
  await once(peer, 'listening')
  const child = spawn(process.execPath, ['server.mjs'], {
    cwd: import.meta.dirname,
    env: { ...CHILD_ENV, CORAL_ENDPOINT: `http://127.0.0.1:${peer.address().port}` },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  let stderr = ''
  child.stderr.setEncoding('utf8').on('data', (chunk) => (stderr += chunk))
  try {
    const origin = `http://127.0.0.1:${await listeningPort(child)}`
    assert.equal((await fetch(`${origin}/readyz`)).status, 200)
    status = 2 // NOT_SERVING
    const notReady = await fetch(`${origin}/readyz`)
    assert.equal(notReady.status, 503)
    assert.deepEqual(await notReady.json(), { status: 'unavailable' })
    status = null // A dependency that accepts HTTP/2 but never completes its RPC.
    const started = performance.now()
    const stalled = await fetch(`${origin}/readyz`, { signal: AbortSignal.timeout(5000) })
    assert.equal(stalled.status, 503)
    assert.ok(performance.now() - started < 5000)
    assert.equal((await fetch(`${origin}/healthz`)).status, 200)
    status = 1 // Recovery requires no process restart or authenticated browser.
    assert.equal((await fetch(`${origin}/readyz`)).status, 200)
  } finally {
    await stopChild(child)
    for (const session of sessions) session.destroy()
    await new Promise((resolve) => peer.close(resolve))
  }
  assert.equal(stderr, '')
})

test('production entry names fatal runtime config and exits without listening', async () => {
  const child = spawn(process.execPath, ['server.mjs'], {
    cwd: import.meta.dirname,
    env: { ...CHILD_ENV, CORAL_WEB_PUBLIC_URL: '' },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  const stdout = streamText(child.stdout)
  const stderr = streamText(child.stderr)
  const [code] = await once(child, 'exit')

  assert.notEqual(code, 0)
  assert.doesNotMatch(await stdout, /Coral web listening/)
  const error = await stderr
  assert.match(error, /CORAL_WEB_PUBLIC_URL must be set when auth is required/)
  assert.match(error, /Coral web startup failed: CORAL_WEB_PUBLIC_URL/)
})

test('one Reef request is one trace, rooted at Reef, that Coral calls join', async () => {
  const collector = await startCollector()
  const coral = await startGrpcPeer()
  const child = startReef({
    CORAL_ENDPOINT: coral.url,
    OTEL_EXPORTER_OTLP_ENDPOINT: collector.url,
    OTEL_SERVICE_NAME: 'reef-under-test',
  })
  const stderr = streamText(child.stderr)
  const workspace = '/workspaces/00000000-0000-4000-8000-000000000001'
  try {
    const origin = `http://127.0.0.1:${await listeningPort(child)}`
    const cookie = await validSessionCookie()
    const callsDuring = async (...fetchArgs) => {
      coral.calls.length = 0
      await (await fetch(...fetchArgs)).text()
      return [...coral.calls]
    }

    const page = await callsDuring(`${origin}${workspace}?marker=query-secret`, {
      headers: { Cookie: cookie, baggage: 'user=baggage-secret' },
    })
    const stream = await callsDuring(`${origin}/activity/count`, {
      headers: { Cookie: cookie, Origin: REQUIRED_ENV.CORAL_WEB_PUBLIC_URL },
      method: 'POST',
    })
    await stopChild(child)

    const spans = exportedSpans(collector.bodies)
    const requests = spans.filter((span) => span.kind === SERVER_SPAN)
    assert.deepEqual(
      requests.map((span) => span.name),
      ['GET /workspaces/:workspace', 'POST /activity/count'],
    )
    const [pageSpan, streamSpan] = requests
    for (const [span, calls] of [
      [pageSpan, page],
      [streamSpan, stream],
    ]) {
      assert.ok(calls.length > 0, span.name)
      for (const call of calls) {
        const client = spans.find((candidate) => candidate.name === call.path.slice(1))
        assert.equal(client.kind, CLIENT_SPAN, call.path)
        assert.equal(call.traceparent, `00-${span.traceId}-${client.spanId}-01`, call.path)
      }
      assert.equal(span.resource['service.name'], 'reef-under-test')
    }
    assert.ok(
      spans.some(
        (span) => span.parentSpanId === pageSpan.spanId && span.name.startsWith('loader '),
      ),
    )
    assert.deepEqual(
      spans
        .flatMap((span) => Object.keys(span.attributes))
        .filter((key) => !SAFE_SPAN_KEYS.includes(key)),
      [],
    )
    assert.ok(stream.some((call) => call.path.endsWith('/WatchActivity')))
    assert.equal(pageSpan.parentSpanId, '')
    assert.doesNotMatch(
      Buffer.concat(collector.bodies).toString('latin1'),
      /query-secret|baggage-secret|coral_web_session|00000000-0000-4000/,
    )
    assert.equal(await stderr, '')
  } finally {
    await stopChild(child)
    await coral.close()
    await collector.close()
  }
})

test('production entry names an invalid sampler and exits without listening', async () => {
  const child = startReef({
    OTEL_EXPORTER_OTLP_ENDPOINT: 'http://127.0.0.1:9',
    OTEL_TRACES_SAMPLER: 'unknown',
  })
  const stdout = streamText(child.stdout)
  const stderr = streamText(child.stderr)
  const [code] = await once(child, 'exit')
  assert.notEqual(code, 0)
  assert.match(await stderr, /trace export: OTEL_TRACES_SAMPLER must be one of/)
  assert.doesNotMatch(await stdout, /Coral web listening/)
})

test('stops waiting for a listening port when the child exits', async () => {
  const child = spawn(process.execPath, ['-e', 'process.exit(2)'], {
    stdio: ['ignore', 'pipe', 'pipe'],
  })

  await assert.rejects(listeningPort(child), /server exited before listening \(2\)/)
})

function startReef(env) {
  return spawn(process.execPath, ['server.mjs'], {
    cwd: import.meta.dirname,
    env: { ...CHILD_ENV, ...env },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
}

/** A native gRPC peer that denies every call and records its trace headers. */
async function startGrpcPeer() {
  const calls = []
  const sessions = new Set()
  const peer = createHttp2Server()
  peer.on('session', (session) => {
    sessions.add(session)
    session.on('close', () => sessions.delete(session))
  })
  peer.on('stream', (stream, headers) => {
    calls.push({ path: headers[':path'], traceparent: headers.traceparent })
    stream.on('error', () => undefined)
    stream.resume()
    stream.respond({ ':status': 200, 'content-type': 'application/grpc', 'grpc-status': '7' })
    stream.end()
  })
  peer.listen(0, '127.0.0.1')
  await once(peer, 'listening')
  return {
    calls,
    url: `http://127.0.0.1:${peer.address().port}`,
    async close() {
      for (const session of sessions) session.destroy()
      await new Promise((resolve) => peer.close(resolve))
    },
  }
}

/** An OTLP/HTTP endpoint that keeps each request body. */
async function startCollector() {
  const bodies = []
  const collector = createServer(async (request, response) => {
    const chunks = []
    for await (const chunk of request) chunks.push(chunk)
    bodies.push(Buffer.concat(chunks))
    response.writeHead(200).end()
  })
  collector.listen(0, '127.0.0.1')
  await once(collector, 'listening')
  return {
    bodies,
    url: `http://127.0.0.1:${collector.address().port}`,
    close: () => new Promise((resolve) => collector.close(resolve)),
  }
}

const SERVER_SPAN = 2
const CLIENT_SPAN = 3
const SAFE_SPAN_KEYS = [
  'error.type',
  'http.request.method',
  'http.response.status_code',
  'http.route',
  'rpc.method',
  'rpc.response.status_code',
  'rpc.system.name',
]

/** Decodes the spans of OTLP `ExportTraceServiceRequest` bodies. */
function exportedSpans(bodies) {
  const spans = []
  for (const body of bodies) {
    for (const resourceSpans of protoFields(body, 1)) {
      const [resource] = protoFields(resourceSpans, 1)
      for (const scopeSpans of protoFields(resourceSpans, 2)) {
        for (const span of protoFields(scopeSpans, 2)) {
          const text = (field) => (protoFields(span, field)[0] ?? Buffer.alloc(0)).toString()
          const hex = (field) => (protoFields(span, field)[0] ?? Buffer.alloc(0)).toString('hex')
          spans.push({
            attributes: protoAttributes(span, 9),
            kind: Number(protoFields(span, 6)[0]),
            name: text(5),
            parentSpanId: hex(4),
            resource: protoAttributes(resource, 1),
            spanId: hex(2),
            traceId: hex(1),
          })
        }
      }
    }
  }
  return spans
}

function protoAttributes(message, field) {
  return Object.fromEntries(
    protoFields(message, field).map((attribute) => {
      const [value] = protoFields(attribute, 2)
      const [text] = protoFields(value, 1)
      return [
        protoFields(attribute, 1)[0].toString(),
        text === undefined ? protoFields(value, 3)[0] : text.toString(),
      ]
    }),
  )
}

/** The values of one field in a protobuf message: bytes, or a BigInt varint. */
function protoFields(message, wanted) {
  const values = []
  let offset = 0
  const varint = () => {
    let value = 0n
    let shift = 0n
    let byte
    do {
      byte = message[offset++]
      value |= BigInt(byte & 0x7f) << shift
      shift += 7n
    } while (byte & 0x80)
    return value
  }
  const bytes = (length) => message.subarray(offset, (offset += length))
  while (offset < message.length) {
    const tag = Number(varint())
    const readers = {
      0: varint,
      1: () => bytes(8),
      2: () => bytes(Number(varint())),
      5: () => bytes(4),
    }
    const value = readers[tag & 7]()
    if (tag >> 3 === wanted) values.push(value)
  }
  return values
}

// fetch() always sends its own Host, and HTTP/1.1 requires one, so write the
// request directly. HTTP/1.0 is the only way to omit Host.
async function rawRequest(port, target, host) {
  const socket = connect(port, '127.0.0.1')
  await once(socket, 'connect')
  // Keep the socket open for writing: Node closes a half-closed connection
  // before an asynchronous handler answers. The server closes it afterwards.
  socket.write(
    host === undefined
      ? `GET ${target} HTTP/1.0\r\n\r\n`
      : `GET ${target} HTTP/1.1\r\nHost: ${host}\r\nConnection: close\r\n\r\n`,
  )
  const text = await streamText(socket)
  const [head, body] = text.split('\r\n\r\n')
  return { status: Number(head.split(' ')[1]), body }
}

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
      const match = output.match(/Coral web listening on http:\/\/127\.0\.0\.1:(\d+)/)
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
