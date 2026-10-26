import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { readFile } from 'node:fs/promises'
import { createServer } from 'node:http'
import { test } from 'node:test'
import { chromium } from 'playwright'

const approvalDirectory = new URL(
  '../../../crates/coral-app/src/auth/authorization_server/authorize/',
  import.meta.url,
)

test('standalone OAuth page works without scripts under its pinned CSP in both themes', async () => {
  const css = await readFile(new URL('approval.css', approvalDirectory), 'utf8')
  const hash = createHash('sha256').update(css).digest('base64')
  const policy = `default-src 'none'; base-uri 'none'; frame-ancestors 'none'; style-src 'sha256-${hash}'`
  const template = await readFile(new URL('approval.html', approvalDirectory), 'utf8')
  const submissions: { body: string; origin?: string }[] = []
  const server = createServer(async (request, response) => {
    if (request.method === 'POST') {
      let body = ''
      for await (const chunk of request) body += chunk
      submissions.push({ body, origin: request.headers.origin })
      response.end('Submitted')
      return
    }
    const loopback = request.url?.includes('loopback')
    const values: Record<string, string> = {
      styles: css,
      client_name: 'Example client',
      client_identity: 'client.example.test',
      redirect_label: 'Returns to',
      redirect_destination: loopback ? '127.0.0.1:4111' : 'client.example.test:443',
      ticket: 'test-only-ticket',
      access_scope: 'this workspace',
      decision_title: 'Allow access?',
      form_action: '/oauth/authorize',
      account: '',
      choices: '',
    }
    // Rust owns real substitution and escaping; this fixture supplies known
    // values solely to exercise the hand-authored document in a real browser.
    const html = template.replace(/@@(.*?)@@/g, (_, slot: string) => values[slot])
    response.writeHead(200, {
      'Content-Type': 'text/html',
      'Content-Security-Policy': policy,
      'Referrer-Policy': 'same-origin',
    })
    response.end(html)
  })
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const origin = `http://127.0.0.1:${(server.address() as { port: number }).port}`
  const browser = await chromium.launch()
  try {
    const backgrounds: string[] = []
    for (const colorScheme of ['light', 'dark'] as const) {
      for (const loopback of [false, true]) {
        const page = await browser.newPage({
          colorScheme,
          javaScriptEnabled: false,
          viewport: { width: colorScheme === 'dark' ? 390 : 1280, height: 900 },
        })
        const errors: string[] = []
        const requests: string[] = []
        page.on('console', (message) => {
          if (message.text().includes('Content Security Policy')) errors.push(message.text())
        })
        page.on('request', (request) => requests.push(request.url()))
        await page.goto(`${origin}/oauth/authorize${loopback ? '?loopback' : ''}`)
        await page.getByRole('heading', { name: 'Connect Example client to Coral' }).waitFor()
        assert.equal(await page.getByRole('alert').count(), 0)
        await page
          .getByText(loopback ? '127.0.0.1:4111' : 'client.example.test:443', {
            exact: true,
          })
          .waitFor()
        assert.equal(await page.locator('script, [style]').count(), 0)
        // Branding must stay embedded: no logo, font, or stylesheet requests.
        assert.deepEqual(requests, [page.url()])
        assert.equal(await page.locator('button').first().getAttribute('value'), 'cancel')
        assert.equal(
          await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth),
          true,
        )
        assert.equal(
          await page.locator('main').evaluate((element) => getComputedStyle(element).display),
          'grid',
        )
        if (!loopback)
          backgrounds.push(
            await page
              .locator('body')
              .evaluate((element) => getComputedStyle(element).backgroundColor),
          )
        const decision = loopback
          ? { label: 'Connect', value: 'continue' }
          : { label: 'Cancel', value: 'cancel' }
        await page.getByRole('button', { name: decision.label, exact: true }).click()
        await page.getByText('Submitted', { exact: true }).waitFor()
        assert.deepEqual(submissions.at(-1), {
          body: `ticket=test-only-ticket&decision=${decision.value}`,
          origin,
        })
        assert.deepEqual(errors, [])
        assert.ok(requests.every((url) => url.startsWith(origin + '/')))
        await page.close()
      }
    }
    assert.notEqual(backgrounds[0], backgrounds[1])
  } finally {
    await browser.close()
    server.closeAllConnections()
    await new Promise<void>((resolve) => server.close(() => resolve()))
  }
})

// Fills a template's slots the way Rust does, without escaping.
const fill = (template: string, values: Record<string, string>) =>
  template.replace(/@@(.*?)@@/g, (_, slot: string) => values[slot])

// Rust renders these cards and their Connect buttons from escaped names; the
// fixture mirrors that markup.
const option = (id: string, name: string, meta: string) =>
  `<label class="choice"><input type="radio" name="workspace" value="${id}" required>` +
  `<span class="choice-text"><span class="choice-name"><bdi>${name}</bdi></span>` +
  `<span class="choice-meta">${meta}</span></span></label>` +
  `<button class="connect connect-for" type="submit" name="decision" value="continue">` +
  `Connect to <bdi>${name}</bdi></button>`

test('the workspace approval page and its empty state work without scripts in both themes', async () => {
  const css = await readFile(new URL('approval.css', approvalDirectory), 'utf8')
  const hash = createHash('sha256').update(css).digest('base64')
  const policy = `default-src 'none'; base-uri 'none'; frame-ancestors 'none'; style-src 'sha256-${hash}'`
  const approval = await readFile(new URL('approval.html', approvalDirectory), 'utf8')
  const empty = await readFile(new URL('no_workspace.html', approvalDirectory), 'utf8')
  const values: Record<string, string> = {
    styles: css,
    client_name: 'Example client',
    client_identity: 'client.example.test',
    redirect_label: 'Returns to',
    redirect_destination: 'client.example.test:443',
    ticket: 'test-only-ticket',
    access_scope: 'the workspace you choose',
    decision_title: 'Choose a workspace',
    form_action: '/oauth/workspace',
    account: '<p class="account"><span>Signed in as <bdi>person@example.test</bdi></span></p>',
    choices:
      option('alpha-id', 'Alpha', 'GitHub, Linear') +
      option(
        'beta-id',
        'A workspace with a very long name that wraps on a phone',
        'No providers yet',
      ),
    coral_link: '<p class="actions"><a href="https://coral.example.test">Open Coral</a></p>',
  }
  const submissions: string[] = []
  const server = createServer(async (request, response) => {
    if (request.method === 'POST') {
      let body = ''
      for await (const chunk of request) body += chunk
      submissions.push(body)
      response.end('Submitted')
      return
    }
    const template = request.url?.includes('empty') ? empty : approval
    response.writeHead(200, { 'Content-Type': 'text/html', 'Content-Security-Policy': policy })
    response.end(fill(template, values))
  })
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const origin = `http://127.0.0.1:${(server.address() as { port: number }).port}`
  const browser = await chromium.launch()
  try {
    for (const colorScheme of ['light', 'dark'] as const) {
      const page = await browser.newPage({
        colorScheme,
        javaScriptEnabled: false,
        viewport: { width: colorScheme === 'dark' ? 390 : 1280, height: 900 },
      })
      const errors: string[] = []
      page.on('console', (message) => {
        if (message.text().includes('Content Security Policy')) errors.push(message.text())
      })
      await page.goto(`${origin}/picker`)
      await page.getByRole('heading', { name: 'Connect Example client to Coral' }).waitFor()
      await page.getByRole('heading', { name: 'Choose a workspace' }).waitFor()
      await page.getByText('client.example.test:443', { exact: true }).waitFor()
      await page.getByText('person@example.test', { exact: true }).waitFor()
      assert.equal(await page.getByRole('radio', { checked: true }).count(), 0)
      assert.equal(await page.locator('button').first().getAttribute('value'), 'cancel')
      // With nothing picked, the browser holds Connect back but lets Cancel
      // through: only Cancel's submission arrives.
      const before = submissions.length
      await page.getByRole('button', { name: 'Connect', exact: true }).click()
      await page.getByRole('button', { name: 'Cancel', exact: true }).click()
      await page.getByText('Submitted', { exact: true }).waitFor()
      assert.deepEqual(submissions.slice(before), ['ticket=test-only-ticket&decision=cancel'])
      await page.goto(`${origin}/picker`)
      assert.equal(
        await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth),
        true,
      )
      await page.getByRole('radio', { name: /very long name/ }).check()
      // The one Connect button left names the picked workspace.
      assert.deepEqual(await page.getByRole('button', { name: /^Connect/ }).allTextContents(), [
        'Connect to A workspace with a very long name that wraps on a phone',
      ])
      await page.getByRole('button', { name: /^Connect to A workspace/ }).click()
      await page.getByText('Submitted', { exact: true }).waitFor()
      assert.equal(
        submissions.at(-1),
        'ticket=test-only-ticket&workspace=beta-id&decision=continue',
      )

      await page.goto(`${origin}/empty`)
      await page.getByRole('heading', { name: 'No workspace to choose' }).waitFor()
      await page.getByRole('link', { name: 'Open Coral' }).waitFor()
      assert.deepEqual(errors, [])
      await page.close()
    }
  } finally {
    await browser.close()
    server.closeAllConnections()
    await new Promise<void>((resolve) => server.close(() => resolve()))
  }
})
