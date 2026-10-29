import { expect, type TestContext } from 'vitest'

import { copiedBy } from '../../../support/clipboard.ts'
import { breadcrumb, onboardingDialog } from '../../../support/providers.ts'
import { withOwner } from '../../../support/topology.ts'

/**
 * A new workspace's overview opens onboarding once for its creator. Its prompt
 * names the workspace and its MCP URL; Copy prompt keeps the dialog open. After Dismiss, a reload or a later visit does not open it.
 */
export async function workspaceOnboarding(context: TestContext) {
  await withOwner(context, async ({ page, target, step }) => {
    const name = `Onboarding "${crypto.randomUUID()}"`
    const nav = page.getByRole('navigation', { name: 'Coral', exact: true })
    const dialog = onboardingDialog(page)
    const copy = dialog.getByRole('button', { name: 'Copy prompt', exact: true })
    let id = ''
    await step('Create a workspace and land on its overview with onboarding open', async () => {
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
      // The modal hides the page behind it, so the breadcrumb shows after Dismiss.
      await dialog.waitFor()
      id = new URL(page.url()).pathname.split('/')[2]
    })
    await step(
      'Copy prompt copies the live workspace into the prompt and keeps it open',
      async () => {
        const copied = await copiedBy(page, copy)
        expect(copied).toContain(
          `Help me connect to my Coral workspace ${JSON.stringify(name)} and add a provider.`,
        )
        // The prompt's wording and clamp belong to the Onboarding story; this checks the live URL.
        expect(copied).toContain(`${target.mcpURL}/workspaces/${id}/mcp`)
        await dialog.getByRole('button', { name: 'Copy prompt: copied', exact: true }).waitFor()
        expect(await dialog.isVisible()).toBe(true)
      },
    )
    await step('Dismiss closes onboarding', async () => {
      await dialog.getByRole('button', { name: 'Dismiss', exact: true }).click()
      await dialog.waitFor({ state: 'detached' })
      await breadcrumb(page, name).waitFor()
    })
    // A dialog over the overview would take the click, so Add provider opening proves none is.
    const overviewIsFree = async () => {
      await page.getByRole('button', { name: /^Open account menu for / }).waitFor()
      await page
        .getByRole('main')
        .getByRole('button', { name: 'Add provider', exact: true })
        .click()
      await page.getByRole('dialog', { name: 'Add a provider', exact: true }).waitFor()
      expect(await dialog.count()).toBe(0)
      await page.keyboard.press('Escape')
      await page.getByRole('dialog').waitFor({ state: 'detached' })
    }
    await step('A reload does not open onboarding again', async () => {
      await page.reload()
      await breadcrumb(page, name).waitFor()
      await overviewIsFree()
    })
    await step('A later visit does not open onboarding again', async () => {
      await page.goto(`/workspaces/${id}/agents`)
      await page.getByRole('heading', { name: 'Agents', exact: true, level: 1 }).waitFor()
      await nav
        .getByRole('group', { name, exact: true })
        .getByRole('link', { name: 'Overview', exact: true })
        .click()
      await page.waitForURL((url) => url.pathname === `/workspaces/${id}`)
      await overviewIsFree()
    })
  })
}
