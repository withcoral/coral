import assert from 'node:assert/strict'
import { createServer as createHttpServer, request } from 'node:http'
import { connect, createServer as createHttp2Server, type ServerHttp2Stream } from 'node:http2'
import { test } from 'node:test'

import { startRoundRobinProxy } from './round-robin-proxy.ts'

/** A backend that names itself on both fronts, with gRPC's status in a trailer. */
async function backend(name: string) {
  const grpc = createHttp2Server()
  grpc.on('stream', (stream: ServerHttp2Stream, headers) => {
    stream.respond(
      { ':status': 200, 'content-type': 'application/grpc' },
      { waitForTrailers: true },
    )
    stream.on('wantTrailers', () =>
      stream.sendTrailers({ 'grpc-status': headers[':path'] === '/fail' ? '14' : '0' }),
    )
    stream.end(name)
  })
  const mcp = createHttpServer((incoming, response) => {
    // initialize issues a session, as a 2025-11-25 MCP server does.
    if (incoming.url === '/initialize') response.setHeader('mcp-session-id', `${name}-session`)
    response.end(`${name} ${incoming.headers.host}`)
  })
  const listen = (server: typeof grpc | typeof mcp) =>
    new Promise<number>((resolve) =>
      server.listen(0, '127.0.0.1', () => resolve((server.address() as { port: number }).port)),
    )
  const [grpcPort, mcpPort] = await Promise.all([listen(grpc), listen(mcp)])
  return {
    grpcURL: `http://127.0.0.1:${grpcPort}`,
    mcpURL: `http://127.0.0.1:${mcpPort}`,
    authURL: `http://127.0.0.1:${mcpPort}`,
    close: () =>
      Promise.all([grpc, mcp].map((server) => new Promise((done) => server.close(done)))),
  }
}

test('the proxy balances each request on one connection and records what each answered', async () => {
  const backends = await Promise.all([backend('first'), backend('second')])
  const proxy = await startRoundRobinProxy()
  proxy.serve(backends)
  const session = connect(proxy.grpcURL)
  try {
    const call = (path: string) =>
      new Promise<{ body: string; status?: string }>((resolve, reject) => {
        const stream = session.request({ ':method': 'POST', ':path': path })
        let body = ''
        let status: string | undefined
        stream.setEncoding('utf8').on('data', (chunk) => (body += chunk))
        stream.on('trailers', (trailers) => (status = String(trailers['grpc-status'])))
        stream.on('end', () => resolve({ body, status }))
        stream.on('error', reject)
        stream.end()
      })
    assert.deepEqual(
      (await Promise.all(['/a', '/b', '/c'].map(call))).map(({ body }) => body),
      ['first', 'second', 'first'],
    )
    proxy.pin(1)
    assert.deepEqual(await call('/fail'), { body: 'second', status: '14' })
    assert.equal((await call('/d')).body, 'second')
    proxy.pin(undefined)
    const post = (path: string, headers: Record<string, string> = {}) =>
      new Promise<string>((resolve, reject) => {
        const outgoing = request(
          `${proxy.mcpURL}${path}`,
          { method: 'POST', headers },
          (response) => {
            let body = ''
            response.setEncoding('utf8').on('data', (chunk) => (body += chunk))
            response.on('end', () => resolve(body))
          },
        )
        outgoing.on('error', reject)
        outgoing.end()
      })
    // The Host header stays the proxy's origin.
    assert.equal(await post('/initialize'), `second ${new URL(proxy.mcpURL).host}`)
    // A session's later requests stay on the instance that issued it, pinned or not.
    proxy.pin(0)
    assert.match(await post('/mcp', { 'mcp-session-id': 'second-session' }), /^second /)
    assert.match(await post('/mcp'), /^first /)
    proxy.pin(undefined)
    const grpc = proxy.requests().filter(({ front }) => front === 'grpc')
    assert.deepEqual(new Set(grpc.map(({ connection }) => connection)), new Set([1]))
    assert.deepEqual(
      grpc.map(({ instance, path, grpcStatus }) => [instance, path, grpcStatus]),
      [
        [0, '/a', '0'],
        [1, '/b', '0'],
        [0, '/c', '0'],
        [1, '/fail', '14'],
        [1, '/d', '0'],
      ],
    )
    assert.deepEqual(
      proxy
        .requests()
        .filter(({ front }) => front === 'mcp')
        .map(({ instance, path, status }) => [instance, path, status]),
      [
        [1, '/initialize', 200],
        [1, '/mcp', 200],
        [0, '/mcp', 200],
      ],
    )
  } finally {
    session.close()
    await proxy.stop()
    await Promise.all(backends.map((server) => server.close()))
  }
})
