import { expect, test } from 'vitest'
import { withBrowserSession } from '../../support/browser-session.ts'
import { createWorkspace } from '../../support/providers.ts'
import { openSettings, sidebarNav } from '../../support/settings.ts'

// Nobody else can sign in to a local deployment, so Reef offers no people to manage.
test('A local deployment offers no people management', async (context) => {
  await withBrowserSession(context, async ({ page, step }) => {
    const name = `workspaces/${await createWorkspace(undefined, `Local people ${crypto.randomUUID()}`)}`
    await step('Show no Members destination in the workspace or the organization', async () => {
      await page.goto(`/${name}`)
      await page.getByRole('heading', { name: 'Providers', exact: true, level: 2 }).waitFor()
      expect(await page.getByRole('link', { name: 'Members', exact: true }).count()).toBe(0)
      expect(await page.getByRole('heading', { name: 'Members', exact: true }).count()).toBe(0)
      // The local user owns the organization, and still has no members to manage.
      await openSettings(page, 'Workspaces')
      expect(
        await sidebarNav(page).getByRole('link', { name: 'Members', exact: true }).count(),
      ).toBe(0)
    })
    await step('Serve no people pages', async () => {
      for (const path of [`/${name}/members`, '/organization/members'])
        expect((await page.goto(path))?.status(), path).toBe(404)
    })
  })
})
