import { expect, test } from 'vitest'
import { withBrowserSession } from '../../support/browser-session.ts'
import { breadcrumb, createWorkspace } from '../../support/providers.ts'

test('A fresh local browser opens Reef without a sign-in ceremony', async (context) => {
  await withBrowserSession(context, async ({ page, target, step }) => {
    await step('Open the ready local application without credentials', async () => {
      expect(target.authentication).toBeUndefined()
      expect((await page.request.get('/readyz')).status()).toBe(200)
      // Home opens a Workspace's Activity, so make sure one exists.
      await createWorkspace(undefined, `Local entry ${crypto.randomUUID()}`)
      await page.goto('/')
      await breadcrumb(page, 'Activity').waitFor()
      expect(new URL(page.url()).pathname).toMatch(/^\/workspaces\/[^/]+\/activity$/)
      await page
        .getByRole('button', { name: 'Open account menu for Local user', exact: true })
        .click()
      expect(await page.getByRole('menuitem', { name: 'Sign out', exact: true }).count()).toBe(0)
      expect(new URL(page.url()).origin).toBe(target.baseURL)
    })
  })
})
