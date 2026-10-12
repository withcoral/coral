import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { readFile } from 'node:fs/promises'
import { createServer } from 'node:http'
import { createServer as https } from 'node:https'
import { once } from 'node:events'
import { chromium } from 'playwright'

// Fixed ports belong only to this container. Product fixtures still use
// ephemeral ports. Both native Coral and Chromium exercise normal verification.
const directory = process.env.E2E_TLS_DIRECTORY!
const servers = [createServer((_req, res) => res.end('sidecar'))]
for (const name of ['trusted', 'untrusted']) {
  servers.push(
    https(
      {
        cert: await readFile(`${directory}/${name}.pem`),
        key: await readFile(`${directory}/${name}.key`),
      },
      (_req, res) => res.end('sidecar'),
    ),
  )
}
let browser: Awaited<ReturnType<typeof chromium.launch>> | undefined
try {
  for (const [index, port] of [5000, 5443, 5444].entries()) {
    servers[index].listen(port, '0.0.0.0')
    await once(servers[index], 'listening')
  }
  const child = spawn('/opt/reel/network-tests', ['--ignored', '--nocapture'], { stdio: 'inherit' })
  const [code] = await once(child, 'exit')
  assert.equal(code, 0, 'Coral must enforce scheme, DNS and TLS on real private hostnames')
  browser = await chromium.launch()
  const page = await browser.newPage()
  for (const origin of ['http://sidecar:5000', 'https://sidecar:5443']) {
    await page.goto(origin)
    assert.equal(await page.textContent('body'), 'sidecar')
  }
  await assert.rejects(page.goto('https://sidecar:5444'), /ERR_CERT_AUTHORITY_INVALID/)
  console.log(
    'Container network acceptance: private HTTP, verified HTTPS and untrusted certificate rejection passed',
  )
} finally {
  await browser?.close()
  for (const server of servers) {
    server.closeAllConnections()
    await new Promise<void>((resolve) => server.close(() => resolve()))
  }
}
