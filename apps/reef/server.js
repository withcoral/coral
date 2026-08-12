import { createReadStream } from 'node:fs'
import { stat } from 'node:fs/promises'
import { createServer } from 'node:http'
import { extname, resolve, sep } from 'node:path'
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
  port = env.PORT ? Number(env.PORT) : 3000,
  listen = listenServer,
}) {
  if (env.REEF_AUTH_MODE?.trim().toLowerCase() === 'disabled') {
    console.warn('WARNING: Reef authentication is disabled; all clients have full local access.')
  }
  await probeRuntimeConfig(handler)
  await listen(server, port, host)
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

  response.setHeader(
    'Cache-Control',
    pathname.startsWith('/assets/') ? 'public, max-age=31536000, immutable' : 'no-cache',
  )
  response.setHeader('Content-Length', metadata.size)
  response.setHeader('Content-Type', CONTENT_TYPES[extname(file)] ?? 'application/octet-stream')
  if (request.method === 'HEAD') response.end()
  else await pipeline(createReadStream(file), response)
  return true
}

function toWebRequest(request) {
  const headers = new Headers()
  for (const [name, value] of Object.entries(request.headers)) {
    for (const item of Array.isArray(value) ? value : [value]) {
      if (item !== undefined) headers.append(name, item)
    }
  }
  const init = { headers, method: request.method }
  if (request.method !== 'GET' && request.method !== 'HEAD') {
    init.body = Readable.toWeb(request)
    init.duplex = 'half'
  }
  return new Request(
    new URL(request.url ?? '/', `http://${request.headers.host ?? 'localhost'}`),
    init,
  )
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
  try {
    if (await serveStatic(request, response)) return
    const webResponse = await handler(toWebRequest(request), new RouterContextProvider())
    await sendWebResponse(request, response, webResponse)
  } catch (error) {
    console.error('Reef request failed:', error)
    if (response.headersSent) response.destroy()
    else response.writeHead(500).end('Internal Server Error')
  }
}

function installShutdown(server) {
  let closing = false
  const shutdown = () => {
    if (closing) return
    closing = true
    server.close((error) => {
      if (error) {
        console.error('Reef shutdown failed:', error)
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
  installShutdown(server)
  await startServer({ handler, server })
  const address = server.address()
  const host = typeof address === 'object' && address ? address.address : process.env.HOST
  const port = typeof address === 'object' && address ? address.port : process.env.PORT
  console.log(`Reef listening on http://${host}:${port}`)
}

if (process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url) {
  main().catch((error) => {
    console.error(`Reef startup failed: ${error instanceof Error ? error.message : String(error)}`)
    process.exitCode = 1
  })
}
