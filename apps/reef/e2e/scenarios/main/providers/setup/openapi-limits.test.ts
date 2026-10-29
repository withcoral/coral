import { createServer } from 'node:http'
import { expect, test } from 'vitest'
import { withOwnerSession } from '../../../../support/actors.ts'
import { listen, openApiDocument } from '../../../../support/provider-fixtures.ts'
import {
  acceptTools,
  addProviderFromFile,
  chooseFile,
  confirmConfiguration,
  createWorkspace,
  openCustomSource,
  setNamespace,
  waitForProviderStatus,
  waitForPublication,
} from '../../../../support/providers.ts'

test('OpenAPI acquisition can finish after the ordinary management deadline', async (context) => {
  const source = openApiDocument('Slow document', { lookup: {} })
  let downloads = 0
  const upstream = await listen(
    context,
    createServer((request, response) => {
      // This document endpoint rejects the automatic MCP probe immediately.
      // Only OpenAPI acquisition is slow; it must outlive the ordinary deadline.
      if (request.method !== 'GET') {
        response.writeHead(405).end()
        return
      }
      downloads++
      // Coral permits 30 seconds for source acquisition. Reef must not cancel it at 10 seconds.
      const timer = setTimeout(() => {
        response.writeHead(200, { 'content-type': 'application/json' })
        response.end(source)
      }, 11_000)
      response.once('close', () => clearTimeout(timer))
    }),
  )
  await withOwnerSession(context, async ({ page, step, actor }) => {
    const workspace = await createWorkspace(
      actor.accessToken,
      `Slow OpenAPI ${crypto.randomUUID()}`,
    )
    await step('Read a source within Coral’s acquisition deadline', async () => {
      await page.goto(`/workspaces/${workspace}/providers/new`)
      await openCustomSource(page)
      await page.getByLabel('OpenAPI URL', { exact: true }).fill(`${upstream.origin}/document.json`)
      await page.getByRole('button', { name: 'Read', exact: true }).click()
      await page.getByLabel('Display name', { exact: true }).waitFor({ timeout: 20_000 })
      expect(await page.getByLabel('Display name', { exact: true }).inputValue()).toBe(
        'Slow document',
      )
      expect(downloads).toBe(1)
    })
  })
})

test('Large OpenAPI documents work by upload and URL; oversized documents explain the limit', async (context) => {
  const document = Buffer.alloc(40 * 1024 * 1024, ' ')
  openApiDocument('Large document', { lookup: { summary: 'Look up a record' } }).copy(document)
  const oversized = Buffer.alloc(64 * 1024 * 1024 + 1, ' ')
  const upstream = await listen(
    context,
    createServer((request, response) => {
      response.writeHead(200, { 'content-type': 'application/json' })
      response.end(request.url === '/oversized.json' ? oversized : document)
    }),
  )
  await withOwnerSession(context, async ({ page, step, actor }) => {
    // Each step moves tens of MiB, which can outlast the harness's action timeout.
    page.setDefaultTimeout(20_000)
    page.setDefaultNavigationTimeout(20_000)
    let workspace = ''
    await step('Upload a document larger than the previous 32 MiB limit', async () => {
      workspace = await createWorkspace(actor.accessToken, `Large OpenAPI ${crypto.randomUUID()}`)
      const uploaded = await addProviderFromFile(page, workspace, document, 'uploaded')
      await waitForProviderStatus(page, { path: uploaded }, 'Ready', { available: 1, total: 1 })
    })
    await step('Add the same large document by URL', async () => {
      await page.goto(`/workspaces/${workspace}/providers/new`)
      await openCustomSource(page)
      await page.getByLabel('OpenAPI URL', { exact: true }).fill(`${upstream.origin}/large.json`)
      await page.getByRole('button', { name: 'Read', exact: true }).click()
      // The uploaded provider already has the document's title.
      await page.getByLabel('Display name', { exact: true }).fill('Downloaded document')
      await confirmConfiguration(page)
      await acceptTools(page)
      await setNamespace(page, 'downloaded')
      await page.getByRole('button', { name: 'Add provider', exact: true }).click()
      await waitForPublication(page)
      const downloaded = new URL(page.url()).pathname
      await waitForProviderStatus(page, { path: downloaded }, 'Ready', { available: 1, total: 1 })
    })
    await step('An oversized URL explains the document limit, not a network failure', async () => {
      await page.goto(`/workspaces/${workspace}/providers/new`)
      await openCustomSource(page)
      await page
        .getByLabel('OpenAPI URL', { exact: true })
        .fill(`${upstream.origin}/oversized.json`)
      await page.getByRole('button', { name: 'Read', exact: true }).click()
      await page
        .getByRole('form', { name: 'New source', exact: true })
        .getByText('This OpenAPI document is larger than 64 MiB. Select a smaller document.', {
          exact: true,
        })
        .waitFor()
      expect(new URL(page.url()).searchParams.has('setup')).toBe(false)
    })
    await step('The file picker enforces the same 64 MiB limit', async () => {
      await page.goto(`/workspaces/${workspace}/providers/new`)
      await chooseFile(page, 'oversized.json', oversized)
      await page.getByRole('button', { name: 'Read', exact: true }).click()
      await page.getByText('Choose a file up to 64 MiB.', { exact: true }).waitFor()
      expect(new URL(page.url()).searchParams.has('setup')).toBe(false)
    })
  })
}, 120_000)
