import { expect, type TestContext } from 'vitest'

import { copiedBy } from '../../../support/clipboard.ts'
import { breadcrumb } from '../../../support/providers.ts'
import { withOwner } from '../../../support/topology.ts'

/**
 * A new workspace's overview opens with no dialog over it. Its quick start's prompt
 * names the workspace and its MCP URL.
 */
export async function workspaceQuickStart(context: TestContext) {
  await withOwner(context, async ({ page, target, step }) => {
    const name = `Quick start "${crypto.randomUUID()}"`
    const nav = page.getByRole('navigation', { name: 'Coral', exact: true })
    let id = ''
    await step('Create a workspace and land on its overview', async () => {
      await page.goto('/')
      // Hydration first: a click that lands before it opens nothing.
      await page.getByRole('button', { name: /^Open account menu for / }).waitFor()
      await nav.getByRole('button', { name: 'Create workspace', exact: true }).click()
      await page.getByLabel('Workspace name', { exact: true }).fill(name)
      await page
        .getByRole('dialog')
        .getByRole('button', { name: 'Create workspace', exact: true })
        .click()
      await page.waitForURL((url) => /^\/workspaces\/[0-9a-f-]{36}$/.test(url.pathname))
      await breadcrumb(page, name).waitFor()
      id = new URL(page.url()).pathname.split('/')[2]
    })
    await step('No dialog opens over the overview', async () => {
      // A dialog over the overview would take the click, so Add provider opening proves none is.
      await page
        .getByRole('main')
        .getByRole('button', { name: 'Add provider', exact: true })
        .click()
      await page.getByRole('dialog', { name: 'Add a provider', exact: true }).waitFor()
      await page.keyboard.press('Escape')
      await page.getByRole('dialog').waitFor({ state: 'detached' })
    })
    await step('Copy prompt copies the live workspace into the prompt', async () => {
      const card = page.getByRole('region', { name: 'Connect your MCP client', exact: true })
      await card.getByRole('tab', { name: 'Ask your agent', exact: true }).click()
      const copied = await copiedBy(
        page,
        card.getByRole('button', { name: 'Copy prompt', exact: true }),
      )
      expect(copied).toContain(
        `Help me connect to my Coral workspace ${JSON.stringify(name)} and add a provider.`,
      )
      // The prompt's wording and clamp belong to the AskYourAgent stories; this checks the live URL.
      expect(copied).toContain(`${target.mcpURL}/workspaces/${id}/mcp`)
    })
  })
}
