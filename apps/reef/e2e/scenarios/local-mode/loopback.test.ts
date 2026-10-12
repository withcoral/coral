import { createServer } from 'node:http'
import { expect, test } from 'vitest'
import { withBrowserSession } from '../../support/browser-session.ts'
import { loopbackFixtures } from '../../support/config.ts'
import { listen, openApiDocument } from '../../support/provider-fixtures.ts'
import { createWorkspace, openCustomSource } from '../../support/providers.ts'

test('Local provider setup can read an explicitly selected loopback service', async (context) => {
  let reads = 0
  const upstream = await listen(
    context,
    createServer((_request, response) => {
      reads++
      response.setHeader('content-type', 'application/json')
      response.end(openApiDocument('Local service', { lookup: {} }))
    }),
    loopbackFixtures,
  )
  await withBrowserSession(context, async ({ page, step }) => {
    const workspace = await createWorkspace(undefined, `Local service ${crypto.randomUUID()}`)
    await step('Read a loopback document under local destination policy', async () => {
      await page.goto(`/workspaces/${workspace}/providers/new`)
      await openCustomSource(page)
      await page.getByLabel('OpenAPI URL', { exact: true }).fill(upstream.origin)
      await page.getByRole('button', { name: 'Read', exact: true }).click()
      await page.getByLabel('Display name', { exact: true }).waitFor()
      expect(await page.getByLabel('Display name', { exact: true }).inputValue()).toBe(
        'Local service',
      )
      expect(reads).toBe(1)
    })
  })
})
