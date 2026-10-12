import assert from 'node:assert/strict'
import { once } from 'node:events'
import { createServer } from 'node:http'
import { test, type TestContext } from 'node:test'
import { chromium } from 'playwright'

import { collectDataResponses } from './data-responses.ts'
import { bounded } from './stack.ts'

async function fixture(context: TestContext, disconnect = false) {
  const requests: { method?: string; cookie?: string; body: string }[] = []
  let consentCookie: string | undefined
  const server = createServer(async (request, response) => {
    if (request.url === '/signin.data') {
      let body = ''
      for await (const chunk of request) body += chunk
      requests.push({ method: request.method, cookie: request.headers.cookie, body })
      if (disconnect) {
        response.destroy()
        return
      }
      response.writeHead(202, {
        'Content-Type': 'text/plain',
        'Set-Cookie': 'callback=fixture; Path=/; HttpOnly',
      })
      response.write('/consent\n')
      // The redirect is usable before the full payload has arrived.
      setTimeout(() => response.end('sensitive-tail'), 50)
      return
    }
    if (request.url === '/consent') {
      consentCookie = request.headers.cookie
      response.end('<h1>Consent</h1>')
      return
    }
    response.writeHead(200, {
      'Content-Type': 'text/html',
      'Set-Cookie': 'session=fixture; Path=/; HttpOnly',
    })
    response.end(`
      <button onclick="signIn()">Sign in</button>
      <script>
        async function signIn() {
          const response = await fetch('/signin.data', { method: 'POST', body: 'sign-in' });
          const first = await response.body.getReader().read();
          location.href = new TextDecoder().decode(first.value).split('\\n')[0];
        }
      </script>
    `)
  })
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  context.after(async () => {
    server.closeAllConnections()
    await new Promise<void>((resolve) => server.close(() => resolve()))
  })
  const browser = await chromium.launch()
  context.after(() => browser.close())
  const page = await browser.newPage()
  const origin = `http://127.0.0.1:${(server.address() as { port: number }).port}`
  await page.goto(origin)
  return { page, origin, requests, consentCookie: () => consentCookie }
}

test('captures the complete streamed payload before OAuth navigation, preserving cookies and POST', async (context) => {
  const { page, origin, requests, consentCookie } = await fixture(context)
  const responses = await collectDataResponses(page)
  await page.getByRole('button', { name: 'Sign in' }).click()
  await page.waitForURL(`${origin}/consent`)
  assert.deepEqual(await bounded(responses(), 1000, 'Response capture'), [
    '/consent\nsensitive-tail',
  ])
  assert.deepEqual(requests, [{ method: 'POST', cookie: 'session=fixture', body: 'sign-in' }])
  assert.match(consentCookie() ?? '', /callback=fixture/)
})

test('a failed response capture rejects instead of passing with missing secret evidence', async (context) => {
  const { page, requests } = await fixture(context, true)
  const responses = await collectDataResponses(page)
  await page.evaluate(() => fetch('/signin.data', { method: 'POST' }).catch(() => 'request failed'))
  await assert.rejects(responses())
  assert.equal(requests.length, 1)
})
