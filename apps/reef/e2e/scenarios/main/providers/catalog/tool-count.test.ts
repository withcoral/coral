import { expect, test } from 'vitest'
import { withOwnerSession } from '../../../../support/actors.ts'
import { bearerMcpServer } from '../../../../support/provider-fixtures.ts'
import {
  confirmConfiguration,
  createWorkspace,
  openCustomSource,
  readTools,
} from '../../../../support/providers.ts'

test('An unread MCP catalog stays unknown; a discovered empty catalog shows zero', async (context) => {
  const token = `empty-catalog-${crypto.randomUUID()}`
  const upstream = await bearerMcpServer(context, token, false, true)
  await withOwnerSession(context, async ({ page, step, actor }) => {
    const workspace = await createWorkspace(actor.accessToken, `Empty MCP ${crypto.randomUUID()}`)
    await step('Keep the count unknown until discovery succeeds', async () => {
      await page.goto(`/workspaces/${workspace}/providers/new`)
      await openCustomSource(page)
      await page.getByLabel('MCP server URL', { exact: true }).fill(`${upstream.origin}/mcp`)
      await page.getByRole('button', { name: 'Connect', exact: true }).click()
      await page.getByLabel('Namespace', { exact: true }).waitFor()
      expect(await page.getByText('No tools found', { exact: true }).count()).toBe(0)
      expect(upstream.counts.discovery).toBe(0)
      await confirmConfiguration(page, 'Bearer token')
      await page
        .getByRole('button', { name: 'Enter a token to read the tools', exact: true })
        .waitFor()
      expect(await page.getByText('No tools found', { exact: true }).count()).toBe(0)
    })
    await step('Show the known empty result, including after reload', async () => {
      await readTools(page, token)
      await page.getByRole('heading', { name: 'Tools', exact: true }).waitFor()
      const count = page.getByText('0 tools', { exact: true })
      await count.waitFor()
      expect(await page.getByText('Not counted yet', { exact: true }).count()).toBe(0)
      expect(upstream.counts.discovery).toBe(1)
      await page.reload()
      await count.waitFor()
      expect(upstream.counts.discovery).toBe(1)
      expect(upstream.counts.calls).toBe(0)
    })
  })
})
