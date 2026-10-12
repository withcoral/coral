import { createServer } from 'node:http'
import type { Page } from 'playwright'
import { expect, test } from 'vitest'
import { withOwnerSession } from '../../../../support/actors.ts'
import { listen, openApiDocument } from '../../../../support/provider-fixtures.ts'
import { createWorkspace, openCustomSource } from '../../../../support/providers.ts'

async function dropFile(page: Page, name: string, contents: string, size?: number) {
  const transfer = await page.evaluateHandle(
    (file) => {
      const data = new DataTransfer()
      data.items.add(
        new File([file.size ? new Uint8Array(file.size) : file.contents], file.name, {
          type: 'application/json',
        }),
      )
      return data
    },
    { name, contents, size },
  )
  try {
    await page
      .getByText('Drop an OpenAPI document here, up to 64 MiB.', { exact: true })
      .dispatchEvent('drop', { dataTransfer: transfer })
  } finally {
    await transfer.dispose()
  }
}

async function sourceForm(page: Page) {
  return page.getByRole('form', { name: 'New source', exact: true }).evaluate((element) => {
    const form = new FormData(element as HTMLFormElement)
    const file = form.get('document')
    return {
      source: form.get('source'),
      file: file instanceof File ? { name: file.name, size: file.size } : null,
    }
  })
}

test('OpenAPI drops submit the native document file and source choices stay consistent', async (context) => {
  const document = openApiDocument('Dropped records', {
    listRecords: { summary: 'Read café records' },
  }).toString()
  const remote = openApiDocument('URL records', { listRemote: {} })
  const upstream = await listen(
    context,
    createServer((_request, response) => {
      response.writeHead(200, { 'content-type': 'application/json' })
      response.end(remote)
    }),
  )

  await withOwnerSession(context, async ({ page, step, actor }) => {
    const workspace = await createWorkspace(actor.accessToken, `Source drop ${crypto.randomUUID()}`)
    const path = `/workspaces/${workspace}/providers/new`
    await step('Switch between a dropped file, the picker, and a URL', async () => {
      await page.goto(path)
      await openCustomSource(page)
      await dropFile(page, 'discarded.json', document)
      await page.getByRole('status').filter({ hasText: 'discarded.json' }).waitFor()
      expect((await sourceForm(page)).source).toBe('file')

      const choosing = page.waitForEvent('filechooser')
      await page.getByRole('button', { name: 'Choose a file', exact: true }).click()
      await (
        await choosing
      ).setFiles({
        name: 'chosen.json',
        mimeType: 'application/json',
        buffer: Buffer.from(document),
      })
      await page.getByRole('status').filter({ hasText: 'chosen.json' }).waitFor()
      expect((await sourceForm(page)).source).toBe('file')
      await page.getByLabel('OpenAPI URL', { exact: true }).fill(`${upstream.origin}/document.json`)
      // A URL read sends no document part, not even an empty one.
      expect(await sourceForm(page)).toEqual({ source: 'url', file: null })
    })

    await step('Dropped files retain the OpenAPI size validation', async () => {
      await dropFile(page, 'oversized.json', '', 64 * 1024 * 1024 + 1)
      await page.getByRole('button', { name: 'Read', exact: true }).click()
      await page.getByText('Choose a file up to 64 MiB.', { exact: true }).waitFor()
      expect(
        await page.getByLabel('OpenAPI file', { exact: true }).getAttribute('aria-invalid'),
      ).toBe('true')
      expect(new URL(page.url()).searchParams.has('setup')).toBe(false)
    })

    await step('A replacement drop sends the exact document multipart field', async () => {
      await dropFile(page, 'dropped.json', document)
      await page.getByRole('status').filter({ hasText: 'dropped.json' }).waitFor()
      // Chromium's request inspector omits uploaded file bytes. Inspect the
      // native form value, then verify that the real server reads this document.
      const submitted = await page
        .getByRole('form', { name: 'New source', exact: true })
        .evaluate(async (element) => {
          const data = new FormData(element as HTMLFormElement)
          const file = data.get('document') as File
          return {
            source: data.get('source'),
            count: data.getAll('document').length,
            name: file.name,
            type: file.type,
            size: file.size,
            text: await file.text(),
          }
        })
      expect(submitted).toEqual({
        source: 'file',
        count: 1,
        name: 'dropped.json',
        type: 'application/json',
        size: Buffer.byteLength(document),
        text: document,
      })
      const posting = page.waitForRequest(
        (request) =>
          request.method() === 'POST' &&
          request.url().includes('/providers/new') &&
          (request.headers()['content-type'] ?? '').startsWith('multipart/form-data'),
      )
      await page.getByRole('button', { name: 'Read', exact: true }).click()
      expect((await posting).headers()['content-type']).toContain('boundary=')
      await page.getByLabel('Display name', { exact: true }).waitFor()
      expect(await page.getByLabel('Display name', { exact: true }).inputValue()).toBe(
        'Dropped records',
      )
    })

    await step('A URL still reads after a file selection is left behind', async () => {
      await page.goto(path)
      await openCustomSource(page)
      await dropFile(page, 'discarded.json', document)
      await page.getByLabel('OpenAPI URL', { exact: true }).fill(`${upstream.origin}/document.json`)
      await page.getByRole('button', { name: 'Read', exact: true }).click()
      await page.getByLabel('Display name', { exact: true }).waitFor()
      expect(await page.getByLabel('Display name', { exact: true }).inputValue()).toBe(
        'URL records',
      )
    })
  })
})
