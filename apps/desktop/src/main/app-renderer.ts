import { randomBytes } from 'node:crypto'
import { readFile, stat } from 'node:fs/promises'
import { extname, join, resolve, sep } from 'node:path'
import { pathToFileURL } from 'node:url'
import { app, net, protocol } from 'electron'
import { RouterContextProvider, createRequestHandler, type ServerBuild } from 'react-router'
import { repoRoot } from './sidecar'

// The renderer is served over a custom, non-network scheme instead of a TCP
// loopback server, so no local socket exposes the app assets to other
// processes. Registered as `standard` + `secure` so it gets a real origin and
// the fetch/streaming APIs the renderer relies on.
export const APP_SCHEME = 'coral-app'
export const APP_ORIGIN = `${APP_SCHEME}://app`
export const APP_ENTRY_URL = `${APP_ORIGIN}/`

// gRPC-web requests are proxied to the loopback sidecar under this same-origin
// path, so the strict CSP ('self') covers them and no CORS layer is involved.
// In dev the Vite server proxies this same prefix to the sidecar, so the client
// stays same-origin there too — hence the export.
export const GRPC_PATH_PREFIX = '/__coral__'
export const APP_GRPC_BASE = `${APP_ORIGIN}${GRPC_PATH_PREFIX}`

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

// Must be called before the app `ready` event.
export function registerAppSchemePrivileges(): void {
  protocol.registerSchemesAsPrivileged([
    {
      scheme: APP_SCHEME,
      privileges: { standard: true, secure: true, supportFetchAPI: true, stream: true, corsEnabled: true },
    },
  ])
}

function rendererRoot(): string {
  return app.isPackaged ? join(process.resourcesPath, 'app') : resolve(repoRoot(), 'apps', 'reef', 'build', 'client')
}

function serverBuildEntry(): string {
  return app.isPackaged
    ? join(app.getAppPath(), 'out', 'reef-server', 'index.js')
    : resolve(repoRoot(), 'apps', 'reef', 'build', 'server', 'index.js')
}

function isInside(root: string, candidate: string): boolean {
  const normalizedRoot = root.endsWith(sep) ? root : `${root}${sep}`
  return candidate === root || candidate.startsWith(normalizedRoot)
}

function contentType(filePath: string): string {
  return MIME_TYPES[extname(filePath)] ?? 'application/octet-stream'
}

function contentSecurityPolicy(nonce: string): string {
  return [
    "default-src 'none'",
    `script-src 'self' 'nonce-${nonce}'`,
    "style-src 'self' 'unsafe-inline'",
    "img-src 'self' data:",
    "font-src 'self'",
    // The sidecar is reached via the same-origin gRPC proxy path, so 'self' is
    // sufficient — no loopback host needs to be allowlisted here.
    "connect-src 'self'",
    "base-uri 'none'",
    "form-action 'none'",
    "frame-ancestors 'none'",
  ].join('; ')
}

function requestPathname(url: string): string | null {
  try {
    return decodeURIComponent(new URL(url).pathname)
  } catch {
    return null
  }
}

async function assetResponse(filePath: string, headOnly: boolean): Promise<Response> {
  const headers = { 'Cache-Control': 'no-store', 'Content-Type': contentType(filePath) }
  // HEAD probes only need headers — skip the disk read.
  if (headOnly) return new Response(null, { headers })
  return new Response(await readFile(filePath), { headers })
}

async function htmlResponse(filePath: string, headOnly: boolean): Promise<Response> {
  const nonce = randomBytes(16).toString('base64')
  const headers = {
    'Cache-Control': 'no-store',
    'Content-Type': 'text/html; charset=utf-8',
    'Content-Security-Policy': contentSecurityPolicy(nonce),
  }
  if (headOnly) return new Response(null, { headers })
  // Tag every script (the inline theme bootstrap and the RR bundle entries) with
  // the per-response nonce so the strict CSP admits them without 'unsafe-inline'.
  // Match only real script-tag openings (`<script>` or `<script ...>`), never a
  // literal like `<scripting` or `<script` inside text.
  const html = (await readFile(filePath, 'utf8')).replace(/<script(?=[\s>])/g, `<script nonce="${nonce}"`)
  return new Response(html, { headers })
}

function serveFile(filePath: string, headOnly: boolean): Promise<Response> {
  return extname(filePath) === '.html'
    ? htmlResponse(filePath, headOnly)
    : assetResponse(filePath, headOnly)
}

function notFound(): Response {
  return new Response('Not found', {
    status: 404,
    headers: { 'Content-Type': 'text/plain; charset=utf-8' },
  })
}

function methodNotAllowed(): Response {
  return new Response('Method not allowed', { status: 405, headers: { Allow: 'GET, HEAD, POST' } })
}

async function resolveFile(root: string, pathname: string): Promise<string | null> {
  const relativePath = pathname === '/' ? 'index.html' : pathname.replace(/^\/+/, '')
  const candidate = resolve(root, relativePath)
  if (!isInside(root, candidate)) return null

  try {
    if ((await stat(candidate)).isFile()) return candidate
  } catch {
    // Fall through to the React Router server handler.
  }
  return null
}

async function indexHtmlExists(root: string): Promise<boolean> {
  try {
    return (await stat(join(root, 'index.html'))).isFile()
  } catch {
    return false
  }
}

async function serverBuildExists(): Promise<boolean> {
  try {
    return (await stat(serverBuildEntry())).isFile()
  } catch {
    return false
  }
}

function isStaticAssetPath(pathname: string): boolean {
  return (
    pathname !== '/' &&
    pathname !== '/index.html' &&
    Boolean(extname(pathname)) &&
    !pathname.endsWith('.data')
  )
}

function acceptsHtmlFallback(request: Request, pathname: string): boolean {
  const accept = request.headers.get('accept') ?? ''
  return pathname === '/' || !extname(pathname) || accept.includes('text/html')
}

type ReactRouterHandler = ReturnType<typeof createRequestHandler>

let reactRouterHandler: ReactRouterHandler | null = null

async function loadReactRouterHandler(): Promise<ReactRouterHandler> {
  if (reactRouterHandler) return reactRouterHandler

  const serverBuild = (await import(pathToFileURL(serverBuildEntry()).href)) as ServerBuild
  reactRouterHandler = createRequestHandler(serverBuild, app.isPackaged ? 'production' : 'development')
  return reactRouterHandler
}

async function refreshServerSidecarEndpoint(resolveSidecarBaseUrl: () => Promise<string>): Promise<void> {
  process.env.CORAL_ENDPOINT = await resolveSidecarBaseUrl()
}

async function reactRouterResponse(
  request: Request,
  resolveSidecarBaseUrl: () => Promise<string>,
): Promise<Response> {
  try {
    await refreshServerSidecarEndpoint(resolveSidecarBaseUrl)
  } catch (error) {
    console.error('[app-renderer] sidecar unavailable for server render', error)
  }

  const handler = await loadReactRouterHandler()
  const response = await handler(request, new RouterContextProvider() as never)
  return secureDocumentResponse(response, request.method === 'HEAD')
}

async function secureDocumentResponse(response: Response, headOnly: boolean): Promise<Response> {
  const headers = new Headers(response.headers)
  const contentType = headers.get('Content-Type') ?? headers.get('content-type') ?? ''
  if (!contentType.includes('text/html')) return response

  const nonce = randomBytes(16).toString('base64')
  headers.set('Content-Security-Policy', contentSecurityPolicy(nonce))
  headers.set('Cache-Control', 'no-store')
  if (headOnly) {
    return new Response(null, {
      headers,
      status: response.status,
      statusText: response.statusText,
    })
  }

  const html = (await response.text()).replace(/<script(?=[\s>])/g, `<script nonce="${nonce}"`)
  return new Response(html, {
    headers,
    status: response.status,
    statusText: response.statusText,
  })
}

async function proxyToSidecar(
  request: Request,
  resolveSidecarBaseUrl: () => Promise<string>,
): Promise<Response> {
  let baseUrl: string
  try {
    baseUrl = await resolveSidecarBaseUrl()
  } catch (error) {
    console.error('[app-renderer] sidecar unavailable for proxy', error)
    return new Response('Sidecar unavailable', {
      status: 502,
      headers: { 'Content-Type': 'text/plain; charset=utf-8' },
    })
  }

  // Forward the raw (still-encoded) path + query so the sidecar receives the URL
  // exactly as sent — decoding here would corrupt encoded ?/#/%xx in REST paths.
  const requestUrl = new URL(request.url)
  const suffix = requestUrl.pathname.slice(GRPC_PATH_PREFIX.length) // keeps the leading '/'
  const target = `${baseUrl.replace(/\/$/, '')}${suffix}${requestUrl.search}`

  // gRPC-web is unary or server-streaming only (never client-streaming), so the
  // request body is a single small message — buffer it to avoid streaming-body
  // constraints. The response may stream and is forwarded as-is (grpc-web encodes
  // trailers in the body, so nothing extra is needed).
  const body =
    request.method === 'GET' || request.method === 'HEAD' ? undefined : await request.arrayBuffer()

  // A main-process fetch is not subject to CORS, so the sidecar needs no CORS
  // layer for this path.
  try {
    return await net.fetch(target, { method: request.method, headers: request.headers, body })
  } catch (error) {
    // The sidecar can die between resolve and fetch — return a controlled 502
    // instead of surfacing a raw network failure to the renderer.
    console.error('[app-renderer] sidecar proxy request failed', error)
    return new Response('Sidecar request failed', {
      status: 502,
      headers: { 'Content-Type': 'text/plain; charset=utf-8' },
    })
  }
}

export function registerAppProtocol(resolveSidecarBaseUrl: () => Promise<string>): void {
  const root = rendererRoot()

  protocol.handle(APP_SCHEME, async (request) => {
    const pathname = requestPathname(request.url)
    if (pathname === null) return notFound()

    // Same-origin gRPC-web proxy to the loopback sidecar.
    if (pathname === GRPC_PATH_PREFIX || pathname.startsWith(`${GRPC_PATH_PREFIX}/`)) {
      return proxyToSidecar(request, resolveSidecarBaseUrl)
    }

    const headOnly = request.method === 'HEAD'

    try {
      if (request.method === 'GET' || headOnly) {
        const filePath = await resolveFile(root, pathname)
        if (filePath && isStaticAssetPath(pathname)) {
          return serveFile(filePath, headOnly)
        }

        // Asset-like misses should stay 404s. Extensionless document/data
        // requests fall through to the React Router server build.
        if (extname(pathname) && !pathname.endsWith('.data')) {
          return notFound()
        }
      } else if (request.method !== 'POST') {
        return methodNotAllowed()
      }

      // React Router owns document, data, action, and route-discovery requests.
      if (app.isPackaged || (await serverBuildExists())) {
        return reactRouterResponse(request, resolveSidecarBaseUrl)
      }

      // Legacy static fallback for local builds produced before the server
      // bundle exists.
      if (acceptsHtmlFallback(request, pathname) && (await indexHtmlExists(root))) {
        return serveFile(join(root, 'index.html'), headOnly)
      }

      return notFound()
    } catch (error) {
      console.error('[app-renderer] failed to serve renderer asset', error)
      return new Response('Internal server error', {
        status: 500,
        headers: { 'Content-Type': 'text/plain; charset=utf-8' },
      })
    }
  })
}
