import { expect, test } from 'vitest'
import { withOwnerSession } from '../../../support/actors.ts'
import { breadcrumb, createWorkspace } from '../../../support/providers.ts'

test('The shell remembers navigation preferences and recovers from a missing page', async (context) => {
  await withOwnerSession(context, async ({ page, target, step, actor }) => {
    const name = `Navigation ${crypto.randomUUID()}`
    const workspace = await createWorkspace(actor.accessToken, name)
    await step('Open Coral on a workspace Activity', async () => {
      expect((await page.request.get('/readyz')).status()).toBe(200)
      if (target.mcpURL)
        expect((await page.request.get(`${target.mcpURL}/readyz`)).status()).toBe(200)
      await page.goto('/')
      await breadcrumb(page, 'Activity').waitFor()
      // Other scenarios share this Owner, so home may open any of their Workspaces.
      expect(new URL(page.url()).pathname).toMatch(/^\/workspaces\/[^/]+\/activity$/)
      await page.getByRole('navigation', { name: 'Coral', exact: true }).waitFor()
      await page.getByRole('button', { name: 'Collapse sidebar', exact: true }).waitFor()
    })
    await step('Open Activity from the workspace in the sidebar', async () => {
      expect(await page.getByRole('link', { name: 'Needs your action', exact: true }).count()).toBe(
        0,
      )
      await page.goto(`/workspaces/${workspace}`)
      await page
        .getByRole('navigation', { name: 'Coral', exact: true })
        .getByRole('group', { name, exact: true })
        .getByRole('link', { name: 'Activity', exact: true })
        .click()
      await breadcrumb(page, 'Activity').waitFor()
      await breadcrumb(page, name).waitFor()
      expect(new URL(page.url()).pathname).toBe(`/workspaces/${workspace}/activity`)
      await page.reload()
      await breadcrumb(page, 'Activity').waitFor()
      await page.getByRole('heading', { name: 'Your recent activity', exact: true }).waitFor()
    })
    await step('Collapse the sidebar', async () => {
      await page.getByRole('button', { name: 'Collapse sidebar', exact: true }).click()
      await page.getByRole('button', { name: 'Expand sidebar', exact: true }).waitFor()
    })
    await step('Reload and keep the collapsed sidebar', async () => {
      await page.reload()
      await page.getByRole('button', { name: 'Expand sidebar', exact: true }).waitFor()
    })
    await step('Expand the sidebar again', async () => {
      await page.getByRole('button', { name: 'Expand sidebar', exact: true }).click()
      await page.getByRole('button', { name: 'Collapse sidebar', exact: true }).waitFor()
    })
    await step('Open a missing page and see the error', async () => {
      const response = await page.goto('/reel-missing-page')
      expect(response?.status()).toBe(404)
      await page.getByRole('heading', { name: '404', exact: true }).waitFor()
      await page.getByText('This page does not exist. Check the URL.', { exact: true }).waitFor()
    })
    await step('Return to Coral', async () => {
      await page.goto('/')
      await page.getByRole('navigation', { name: 'Coral', exact: true }).waitFor()
      await page.getByRole('button', { name: 'Collapse sidebar', exact: true }).waitFor()
    })
  })
})
