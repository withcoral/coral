import { createReadStream } from 'node:fs'
import { stat } from 'node:fs/promises'
import { createServer, type Server } from 'node:http'
import { extname, join, resolve, sep } from 'node:path'
import { app } from 'electron'
import { repoRoot } from './sidecar'

export interface ReefRendererServer {
  root: string
  url: string
  stop(): Promise<void>
}

const MIME_TYPES: Record<string, string> = {
  '.css': 'text/css; charset=utf-8',
  '.html': 'text/html; charset=utf-8',
  '.ico': 'image/x-icon',
  '.js': 'text/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.map': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
}

function rendererRoot(): string {
  return app.isPackaged ? join(process.resourcesPath, 'reef') : resolve(repoRoot(), 'reef', 'build', 'client')
}

function isInside(root: string, candidate: string): boolean {
  const normalizedRoot = root.endsWith(sep) ? root : `${root}${sep}`
  return candidate === root || candidate.startsWith(normalizedRoot)
}

function acceptsHtml(request: import('node:http').IncomingMessage): boolean {
  return request.headers.accept?.includes('text/html') ?? false
}

function requestPath(url: string | undefined): string | null {
  try {
    return decodeURIComponent(new URL(url ?? '/', 'http://127.0.0.1').pathname)
  } catch {
    return null
  }
}

async function resolveRequestFile(root: string, request: import('node:http').IncomingMessage): Promise<string | null> {
  const pathname = requestPath(request.url)
  if (!pathname) return null

  const relativePath = pathname === '/' ? 'index.html' : pathname.replace(/^\/+/, '')
  const candidate = resolve(root, relativePath)
  if (!isInside(root, candidate)) return null

  try {
    const candidateStat = await stat(candidate)
    if (candidateStat.isFile()) return candidate
  } catch {
    // Fall through to the SPA fallback.
  }

  if (request.method === 'GET' || request.method === 'HEAD') {
    if (!extname(pathname) || acceptsHtml(request)) return join(root, 'index.html')
  }

  return null
}

function sendNotFound(response: import('node:http').ServerResponse) {
  response.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' })
  response.end('Not found')
}

function sendFile(
  request: import('node:http').IncomingMessage,
  response: import('node:http').ServerResponse,
  filePath: string,
) {
  response.writeHead(200, {
    'Cache-Control': 'no-store',
    'Content-Type': MIME_TYPES[extname(filePath)] ?? 'application/octet-stream',
  })

  if (request.method === 'HEAD') {
    response.end()
    return
  }

  createReadStream(filePath)
    .on('error', (error) => {
      if (response.headersSent) response.destroy(error)
      else sendNotFound(response)
    })
    .pipe(response)
}

function closeServer(server: Server): Promise<void> {
  return new Promise((resolveClose, rejectClose) => {
    server.close((error) => {
      if (error) rejectClose(error)
      else resolveClose()
    })
  })
}

export function startReefRendererServer(): Promise<ReefRendererServer> {
  const root = rendererRoot()
  const server = createServer((request, response) => {
    if (request.method !== 'GET' && request.method !== 'HEAD') {
      response.writeHead(405, { Allow: 'GET, HEAD' })
      response.end()
      return
    }

    void resolveRequestFile(root, request)
      .then((filePath) => {
        if (!filePath) {
          sendNotFound(response)
          return
        }
        sendFile(request, response, filePath)
      })
      .catch((error: unknown) => {
        console.error('[reef-renderer] failed to serve renderer asset', error)
        response.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' })
        response.end('Internal server error')
      })
  })

  return new Promise((resolveStart, rejectStart) => {
    server.once('error', rejectStart)
    server.listen(0, '127.0.0.1', () => {
      server.off('error', rejectStart)
      const address = server.address()
      if (!address || typeof address === 'string') {
        rejectStart(new Error('Reef renderer server did not bind to a TCP port.'))
        return
      }
      resolveStart({
        root,
        url: `http://127.0.0.1:${address.port}/`,
        stop: () => closeServer(server),
      })
    })
  })
}
