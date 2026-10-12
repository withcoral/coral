import { expect, test } from 'vitest'
import { withOwnerSession } from '../../../../support/actors.ts'
import { bearerMcpServer } from '../../../../support/provider-fixtures.ts'
import { createWorkspace } from '../../../../support/providers.ts'

test('An instance recipe reads its filled URL through Reef and Coral', async (context) => {
  const upstream = await bearerMcpServer(context, '', true)
  await withOwnerSession(context, async ({ page, step, actor }) => {
    const workspace = await createWorkspace(
      actor.accessToken,
      `Recipe input ${crypto.randomUUID()}`,
    )
    await step('Enter the instance address before reading its source', async () => {
      await page.goto(`/workspaces/${workspace}/providers/new?recipe=n8n`)
      await page.getByLabel('n8n instance', { exact: true }).fill(`${upstream.origin}/`)
      expect(await page.getByLabel('MCP server URL', { exact: true }).inputValue()).toBe(
        `${upstream.origin}/mcp-server/http`,
      )
      await page.getByRole('button', { name: 'Connect', exact: true }).click()
      await page.getByLabel('Display name', { exact: true }).waitFor()
      const server = page.getByRole('group', { name: 'MCP server URL', exact: true })
      expect(await server.getByRole('textbox').inputValue()).toBe(
        `${upstream.origin}/mcp-server/http`,
      )
      expect(upstream.counts.discovery).toBeGreaterThan(0)
      expect(upstream.counts.credentialRequests).toBe(0)
    })
  })
})
