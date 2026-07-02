import { randomBytes } from 'node:crypto'
import { readFile, stat } from 'node:fs/promises'
import { extname, join, resolve, sep } from 'node:path'
import { app, protocol } from 'electron'
import { repoRoot } from './sidecar'

// The renderer is served over a custom, non-network scheme instead of a TCP
// loopback server, so no local socket exposes the app assets to other
// processes. Registered as `standard` + `secure` so it gets a real origin and
// the fetch/streaming APIs the SPA relies on.
export const APP_SCHEME = 'coral-app'
export const APP_ORIGIN = `${APP_SCHEME}://app`
export const APP_ENTRY_URL = `${APP_ORIGIN}/`

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
    // No renderer code reaches the sidecar yet; keep this locked to 'self' and
    // widen it to the actual sidecar origin when the API client is wired.
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

async function resolveFile(root: string, pathname: string): Promise<string | null> {
  const relativePath = pathname === '/' ? 'index.html' : pathname.replace(/^\/+/, '')
  const candidate = resolve(root, relativePath)
  if (!isInside(root, candidate)) return null

  try {
    if ((await stat(candidate)).isFile()) return candidate
  } catch {
    // Fall through to the SPA fallback.
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

export function registerAppProtocol(): void {
  const root = rendererRoot()

  protocol.handle(APP_SCHEME, async (request) => {
    const headOnly = request.method === 'HEAD'
    if (request.method !== 'GET' && !headOnly) {
      return new Response('Method not allowed', { status: 405, headers: { Allow: 'GET, HEAD' } })
    }

    const pathname = requestPathname(request.url)
    if (pathname === null) return notFound()

    try {
      const filePath = await resolveFile(root, pathname)
      if (filePath) {
        return serveFile(filePath, headOnly)
      }

      // SPA fallback: serve index.html for extensionless or html-accepting GETs,
      // but only when it actually exists (otherwise 404 rather than a broken 200).
      const accept = request.headers.get('accept') ?? ''
      if ((!extname(pathname) || accept.includes('text/html')) && (await indexHtmlExists(root))) {
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
