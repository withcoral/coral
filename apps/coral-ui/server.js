import { createReadStream } from 'node:fs'
import { stat } from 'node:fs/promises'
import { createServer } from 'node:http'
import { extname, relative, resolve, sep } from 'node:path'
import { pipeline } from 'node:stream/promises'
import { Readable } from 'node:stream'
import { fileURLToPath, pathToFileURL } from 'node:url'

import { createRequestHandler, RouterContextProvider } from 'react-router'

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
  host = env.HOST?.trim() || '0.0.0.0',
  port = Number(env.PORT?.trim() || 3000),
  listen = listenServer,
  installShutdown = installShutdownHandlers,
}) {
  if (env.CORAL_UI_AUTH_MODE?.trim().toLowerCase() === 'disabled') {
    console.warn(
      'WARNING: Coral UI authentication is disabled; all clients have full local access.',
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
    request: new Request(
      new URL(request.url ?? '/', `http://${request.headers.host ?? 'localhost'}`),
      init,
    ),
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

async function handleNodeRequest(request, response, handler) {
  let bridge
  try {
    if (await serveStatic(request, response)) return
    bridge = createWebRequest(request, response)
    const webResponse = await handler(bridge.request, new RouterContextProvider())
    await sendWebResponse(request, response, webResponse)
  } catch (error) {
    console.error('Coral UI request failed:', error)
    if (response.headersSent) response.destroy()
    else response.writeHead(500).end('Internal Server Error')
  } finally {
    bridge?.release()
  }
}

function installShutdownHandlers(server) {
  let closing = false
  const shutdown = () => {
    if (closing) return
    closing = true
    server.close((error) => {
      if (error) {
        console.error('Coral UI shutdown failed:', error)
        process.exitCode = 1
      }
    })
  }
  process.on('SIGTERM', shutdown)
  process.on('SIGINT', shutdown)
}

async function main() {
  process.env.NODE_ENV = 'production'
  const build = await import('./build/server/index.js')
  const handler = createRequestHandler(build, 'production')
  const server = createServer((request, response) => {
    void handleNodeRequest(request, response, handler)
  })
  await startServer({ handler, server })
  const address = server.address()
  const host = typeof address === 'object' && address ? address.address : process.env.HOST
  const port = typeof address === 'object' && address ? address.port : process.env.PORT
  console.log(`Coral UI listening on http://${host}:${port}`)
}

if (process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url) {
  main().catch((error) => {
    console.error(
      `Coral UI startup failed: ${error instanceof Error ? error.message : String(error)}`,
    )
    process.exitCode = 1
  })
}
