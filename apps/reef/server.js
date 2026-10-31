import { createReadStream } from 'node:fs'
import { stat } from 'node:fs/promises'
import { createServer } from 'node:http'
import { isIP } from 'node:net'
import { extname, relative, resolve, sep } from 'node:path'
import { pipeline } from 'node:stream/promises'
import { Readable } from 'node:stream'
import { fileURLToPath, pathToFileURL } from 'node:url'

import { createRequestHandler, RouterContextProvider } from 'react-router'

import { createRequestTracing } from './server-tracing.js'

const CLIENT_ROOT = resolve(fileURLToPath(new URL('./build/client/', import.meta.url)))
const CONTENT_TYPES = {
  '.css': 'text/css; charset=utf-8',
  '.ico': 'image/x-icon',
  '.js': 'text/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
}

class StartupFailure extends Error {
  constructor(stage, cause) {
    super(`${stage}: ${cause instanceof Error ? cause.message : String(cause)}`, { cause })
  }
}

export async function probeRuntimeConfig(handler) {
  let response
  try {
    response = await handler(new Request('http://127.0.0.1/healthz'), new RouterContextProvider())
  } catch (cause) {
    throw new StartupFailure('runtime configuration', cause)
  }
  if (!response.ok) {
    throw new StartupFailure(
      'runtime configuration',
      new Error(`health probe returned HTTP ${response.status}`),
    )
  }
}

export async function startServer({
  handler,
  server,
  env = process.env,
  host = env.HOST?.trim() ||
    (env.CORAL_WEB_AUTH_MODE?.trim().toLowerCase() === 'disabled' ? '127.0.0.1' : '0.0.0.0'),
  port = Number(env.PORT?.trim() || 3000),
  listen = listenServer,
  installShutdown = installShutdownHandlers,
}) {
  if (env.CORAL_WEB_AUTH_MODE?.trim().toLowerCase() === 'disabled') {
    const loopback = host === '::1' || (isIP(host) === 4 && host.startsWith('127.'))
    if (!loopback)
      throw new StartupFailure('HTTP listener', new Error('local Reef bind must be a loopback IP'))
    console.warn(
      'WARNING: Coral web authentication is disabled; requests use the implicit local User and normal domain authorization.',
    )
  }
  await probeRuntimeConfig(handler)
  await listen(server, port, host)
  installShutdown(server)
  return server
}

async function listenServer(server, port, host) {
  try {
    await new Promise((resolveListen, reject) => {
      const onError = (error) => reject(error)
      server.once('error', onError)
      server.listen(port, host, () => {
        server.off('error', onError)
        resolveListen()
      })
    })
  } catch (cause) {
    throw new StartupFailure('HTTP listener', cause)
  }
}

async function serveStatic(request, response) {
  if (request.method !== 'GET' && request.method !== 'HEAD') return false

  let pathname
  try {
    pathname = decodeURIComponent(new URL(request.url ?? '/', 'http://localhost').pathname)
  } catch {
    return false
  }
  const file = resolve(CLIENT_ROOT, `.${pathname}`)
  if (!file.startsWith(`${CLIENT_ROOT}${sep}`)) return false

  let metadata
  try {
    metadata = await stat(file)
  } catch (error) {
    if (error?.code === 'ENOENT' || error?.code === 'ENOTDIR') return false
    throw error
  }
  if (!metadata.isFile()) return false

  const clientPath = relative(CLIENT_ROOT, file)
  response.setHeader(
    'Cache-Control',
    clientPath.split(sep)[0] === 'assets' ? 'public, max-age=31536000, immutable' : 'no-cache',
  )
  response.setHeader('Content-Length', metadata.size)
  response.setHeader('Content-Type', CONTENT_TYPES[extname(file)] ?? 'application/octet-stream')
  if (request.method === 'HEAD') response.end()
  else await pipeline(createReadStream(file), response)
  return true
}

// React Router checks a mutation's Origin against this URL. Resolving a target
// such as `//attacker.test/x` against the Host would make the attacker's origin
// the request's own, so an origin-form target is always a path on this host.
function requestUrl(request) {
  const origin = new URL(`http://${request.headers.host ?? 'localhost'}`).origin
  const target = request.url ?? '/'
  return target.startsWith('/') ? new URL(`${origin}${target}`) : new URL(target, origin)
}

export function createWebRequest(request, response) {
  const controller = new AbortController()
  const abort = () => controller.abort()
  const abortIncompleteRequest = () => !request.complete && abort()
  const abortIncompleteResponse = () => !response.writableEnded && abort()
  request.once('aborted', abort)
  request.once('close', abortIncompleteRequest)
  response.once('close', abortIncompleteResponse)

  const headers = new Headers()
  for (const [name, value] of Object.entries(request.headers)) {
    for (const item of Array.isArray(value) ? value : [value]) {
      if (item !== undefined) headers.append(name, item)
    }
  }
  const init = { headers, method: request.method, signal: controller.signal }
  if (request.method !== 'GET' && request.method !== 'HEAD') {
    init.body = Readable.toWeb(request)
    init.duplex = 'half'
  }
  return {
    request: new Request(requestUrl(request), init),
    release() {
      request.off('aborted', abort)
      request.off('close', abortIncompleteRequest)
      response.off('close', abortIncompleteResponse)
    },
  }
}

async function sendWebResponse(request, response, webResponse) {
  response.statusCode = webResponse.status
  for (const [name, value] of webResponse.headers) {
    if (name !== 'set-cookie') response.setHeader(name, value)
  }
  const cookies = webResponse.headers.getSetCookie()
  if (cookies.length) response.setHeader('Set-Cookie', cookies)
  if (!webResponse.body || request.method === 'HEAD') {
    await webResponse.body?.cancel()
    response.end()
  } else {
    await pipeline(Readable.fromWeb(webResponse.body), response)
  }
}

async function handleNodeRequest(request, response, handler, traceRequest) {
  let bridge
  try {
    if (await serveStatic(request, response)) return
    bridge = createWebRequest(request, response)
    const webRequest = bridge.request
    await traceRequest(webRequest, async () => {
      const webResponse = await handler(webRequest, new RouterContextProvider())
      await sendWebResponse(request, response, webResponse)
    })
  } catch (error) {
    console.error('Coral web request failed:', error)
    if (response.headersSent) response.destroy()
    else response.writeHead(500).end('Internal Server Error')
  } finally {
    bridge?.release()
  }
}

// Local mode serves the implicit local User to anything that reaches the port.
// React Router derives the request origin from Host, so a DNS-rebinding page
// would pass its origin check. Answer only requests that name this listener, and
// refuse absolute-form targets, which would replace Host in the request URL.
export function createRequestListener({
  handler,
  requireListenerHost,
  traceRequest = (_request, respond) => respond(),
}) {
  return (request, response) => {
    if (requireListenerHost && !namesLocalListener(request)) {
      response.writeHead(421, { 'Content-Length': 0 }).end()
      return
    }
    void handleNodeRequest(request, response, handler, traceRequest)
  }
}

function namesLocalListener(request) {
  const { localAddress, localPort } = request.socket
  const address = isIP(localAddress) === 6 ? `[${localAddress}]` : localAddress
  const host = request.headers.host?.toLowerCase()
  return (
    request.url?.startsWith('/') === true &&
    (host === `${address}:${localPort}` || host === `localhost:${localPort}`)
  )
}

function installShutdownHandlers(server, afterClose = () => undefined) {
  let closing = false
  const shutdown = () => {
    if (closing) return
    closing = true
    server.close(async (closeError) => {
      try {
        if (closeError) throw closeError
        await afterClose()
      } catch (error) {
        console.error('Coral web shutdown failed:', error)
        process.exitCode = 1
      }
    })
  }
  process.on('SIGTERM', shutdown)
  process.on('SIGINT', shutdown)
}

async function main() {
  process.env.NODE_ENV = 'production'
  let tracing
  try {
    tracing = createRequestTracing()
  } catch (cause) {
    throw new StartupFailure('trace export', cause)
  }
  const build = await import('./build/server/index.js')
  const authMode = process.env.CORAL_WEB_AUTH_MODE?.trim().toLowerCase()
  // Use deployment configuration, never the build machine's public host or
  // untrusted forwarding headers, for React Router's action origin allowlist.
  const publicUrl = authMode === 'required' ? process.env.CORAL_WEB_PUBLIC_URL?.trim() : undefined
  const handler = createRequestHandler(
    {
      ...build,
      allowedActionOrigins: publicUrl ? [new URL(publicUrl).host] : [],
    },
    'production',
  )
  const server = createServer(
    createRequestListener({
      handler,
      requireListenerHost: authMode === 'disabled',
      traceRequest: tracing.traceRequest,
    }),
  )
  await startServer({
    handler,
    installShutdown: (listening) => installShutdownHandlers(listening, tracing.shutdown),
    server,
  })
  const address = server.address()
  const host = typeof address === 'object' && address ? address.address : process.env.HOST
  const port = typeof address === 'object' && address ? address.port : process.env.PORT
  console.log(`Coral web listening on http://${host}:${port}`)
}

if (process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url) {
  main().catch((error) => {
    console.error(
      `Coral web startup failed: ${error instanceof Error ? error.message : String(error)}`,
    )
    process.exitCode = 1
  })
}
