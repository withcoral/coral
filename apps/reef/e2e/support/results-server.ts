import { createReadStream } from 'node:fs'
import { realpath, stat } from 'node:fs/promises'
import { createServer } from 'node:http'
import { extname, join, resolve, sep } from 'node:path'
import { pipeline } from 'node:stream/promises'

import { renderLanding } from './artifacts.ts'
import { playbackScriptHash } from './playback.ts'
import { viewerRenderer } from './viewer-renderer.ts'

const mime: Record<string, string> = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json',
  '.png': 'image/png',
  '.webm': 'video/webm',
  '.mp4': 'video/mp4',
  '.zip': 'application/zip',
}

export async function resultsServer(root: string, port = 0, externalOrigin?: string) {
  let externalHost: string | undefined
  if (externalOrigin !== undefined) {
    // Require one canonical HTTP(S) origin, not credentials, a path, or a host pattern.
    let url: URL
    try {
      url = new URL(externalOrigin)
    } catch {
      throw new Error('Invalid E2E_RESULTS_ORIGIN: expected a canonical HTTP(S) origin')
    }
    if (
      !['http:', 'https:'].includes(url.protocol) ||
      /[*,]/.test(url.host) ||
      (externalOrigin !== url.origin && externalOrigin !== `${url.origin}/`)
    ) {
      throw new Error('Invalid E2E_RESULTS_ORIGIN: expected a canonical HTTP(S) origin')
    }
    externalHost = url.host
  }
  const base = await realpath(root)
  await viewerRenderer()
  const landingStyles = new Map<string, string>()
  const server = createServer(async (request, response) => {
    response.setHeader('Cache-Control', 'no-store')
    response.setHeader('X-Content-Type-Options', 'nosniff')
    response.setHeader(
      'Content-Security-Policy',
      `default-src 'none'; script-src '${playbackScriptHash}'; style-src 'self'; font-src data:; img-src 'self' data:; media-src 'self'; base-uri 'none'; frame-ancestors 'none'`,
    )
    // Prevent DNS rebinding; an explicit proxy host is not authentication.
    const host = request.headers.host
    if (
      host !== `127.0.0.1:${(server.address() as { port: number }).port}` &&
      (externalHost === undefined || host !== externalHost)
    ) {
      response.writeHead(403).end()
      return
    }
    if (request.method !== 'GET' && request.method !== 'HEAD') {
      response.writeHead(405).end()
      return
    }
    try {
      const pathname = decodeURIComponent(new URL(request.url!, 'http://127.0.0.1').pathname)
      if (landingStyles.has(pathname)) {
        response.setHeader('Content-Type', mime['.css'])
        response.end(request.method === 'HEAD' ? undefined : landingStyles.get(pathname))
        return
      }
      if (pathname === '/') {
        response.setHeader('Content-Type', mime['.html'])
        const page = await renderLanding(base)
        landingStyles.set(`/${page.stylesheet}`, page.css)
        response.end(request.method === 'HEAD' ? undefined : page.html)
        return
      }
      let path = await realpath(resolve(base, `.${pathname}`))
      if (!path.startsWith(base + sep)) throw new Error('Outside results root')
      if ((await stat(path)).isDirectory()) {
        path = await realpath(join(path, 'index.html'))
        if (!path.startsWith(base + sep)) throw new Error('Outside results root')
      }
      const file = await stat(path)
      if (!file.isFile()) throw new Error('Not a file')
      const contentType = mime[extname(path)] || 'application/octet-stream'
      response.setHeader('Content-Type', contentType)
      if (contentType === 'application/octet-stream')
        response.setHeader('Content-Disposition', 'attachment')
      response.setHeader('Accept-Ranges', 'bytes')
      let start = 0
      let end = file.size - 1
      if (request.headers.range) {
        const range = /^bytes=(\d+)-(\d*)$/.exec(request.headers.range)
        start = Number(range?.[1])
        end = range?.[2] ? Number(range[2]) : end
        if (
          !range ||
          !Number.isSafeInteger(start) ||
          !Number.isSafeInteger(end) ||
          start > end ||
          start < 0 ||
          start >= file.size
        ) {
          response.writeHead(416, { 'Content-Range': `bytes */${file.size}` }).end()
          return
        }
        end = Math.min(end, file.size - 1)
        response.statusCode = 206
        response.setHeader('Content-Range', `bytes ${start}-${end}/${file.size}`)
      }
      response.setHeader('Content-Length', Math.max(0, end - start + 1))
      if (request.method === 'HEAD' || file.size === 0) response.end()
      else await pipeline(createReadStream(path, { start, end }), response)
    } catch {
      if (!response.headersSent) response.writeHead(404).end('Not found')
      else response.destroy()
    }
  })
  await new Promise<void>((onListen, reject) => {
    server.once('error', reject)
    server.listen(port, '127.0.0.1', onListen)
  })
  return server
}
