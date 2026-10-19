import { expect, test } from 'vitest'
import { withOwnerSession } from '../../../../support/actors.ts'
import { openApiDocument } from '../../../../support/provider-fixtures.ts'
import {
  breadcrumb,
  createWorkspace,
  openProviderSettings,
  publishWorkspaceProvider,
} from '../../../../support/providers.ts'

test('Workspace operations preserve deep links, settings, and independent filters', async (context) => {
  await withOwnerSession(context, async ({ page, step, actor }) => {
    let installation = ''
    const table = page.getByRole('table', { name: 'Operations', exact: true })
    const filter = page.getByRole('searchbox', { name: 'Filter operations', exact: true })
    await step('Open the workspace operations table', async () => {
      const workspace = await createWorkspace(actor.accessToken, `Catalog ${crypto.randomUUID()}`)
      await publishWorkspaceProvider(
        actor.accessToken,
        `workspaces/${workspace}`,
        openApiDocument('Catalog', {
          works: { summary: 'Read the forecast' },
          broken: { summary: 'Has no responses', broken: true },
        }),
        'catalog',
      )
      await page.goto(`/workspaces/${workspace}/providers`)
      await table.waitFor()
      // Coral's own provider lists first, so find this provider by its row.
      const action = await table
        .getByRole('row')
        .filter({ has: page.getByRole('link', { name: 'works', exact: true }) })
        .locator('form')
        .getAttribute('action')
      if (!action) throw new Error('Missing installation action')
      installation = action
      await publishWorkspaceProvider(
        actor.accessToken,
        `workspaces/${workspace}`,
        // The second provider is taller than the table's port, so scrolling
        // can bring its heading, or the first catalog's below Coral, to the top.
        openApiDocument('Second catalog', {
          another: { summary: 'Another operation' },
          ...Object.fromEntries(Array.from({ length: 24 }, (_, index) => [`more${index}`, {}])),
        }),
        'other',
      )
      // The last provider is shorter than the port, so only room after the
      // table lets a rail click bring it up under the head.
      await publishWorkspaceProvider(
        actor.accessToken,
        `workspaces/${workspace}`,
        openApiDocument('Tail', { last: {} }),
        'tail',
      )
      await page.goto(installation)
      await table.waitFor()
    })
    await step('Open and reload an operation details drawer', async () => {
      await page.goto(`${installation}/ops/broken`)
      await page
        .getByRole('dialog', { name: 'broken', exact: true })
        .getByText('Unavailable', { exact: true })
        .waitFor()
      await page.getByRole('dialog', { name: 'broken', exact: true }).waitFor()
      await page.reload()
      await page.getByRole('dialog', { name: 'broken', exact: true }).waitFor()
      expect(new URL(page.url()).pathname).toBe(`${installation}/ops/broken`)
      const drawer = page.getByRole('dialog', { name: 'broken', exact: true })
      await drawer.waitFor()
      await drawer.getByRole('button', { name: 'Close', exact: true }).click()
      await page.waitForURL((url) => url.pathname === installation)
      await table.getByRole('link', { name: 'works', exact: true }).click()
      await page.waitForURL((url) => url.pathname === `${installation}/ops/works`)
      await page.getByRole('dialog', { name: 'works', exact: true }).waitFor()
      await page.keyboard.press('Escape')
      await page.waitForURL((url) => url.pathname === installation)
    })
    await step('Preserve settings through browser history and on a narrow screen', async () => {
      const settings = breadcrumb(page, 'Settings')
      await openProviderSettings(page)
      expect(new URL(page.url()).pathname).toBe(`${installation}/settings`)
      await page.getByRole('heading', { level: 2, name: 'Catalog', exact: true }).waitFor()
      // Settings is a page of its own, so the table is gone while it shows.
      expect(await table.count()).toBe(0)
      await page.goBack()
      await settings.waitFor({ state: 'hidden' })
      await table.waitFor()
      await page.goForward()
      await settings.waitFor()
      await page.setViewportSize({ width: 390, height: 844 })
      await page.goto(`${installation}/ops/works`)
      const drawer = page.getByRole('dialog', { name: 'works', exact: true })
      await drawer.waitFor()
      // The drawer slides in from the edge, then rests against it.
      await expect
        .poll(async () => {
          const bounds = await drawer.boundingBox()
          return bounds && Math.round(bounds.x + bounds.width)
        })
        .toBe(390)
      await drawer.getByRole('button', { name: 'Close', exact: true }).click()
      await page.waitForURL((url) => url.pathname === installation)
      await table.waitFor()
      // The filter matches the name the row shows, not its description.
      await filter.fill('orks')
      await table.getByRole('checkbox', { name: 'Select works', exact: true }).waitFor()
    })
    await step('Keep the selection and filter when the rail moves between providers', async () => {
      await page.setViewportSize({ width: 1440, height: 1000 })
      await filter.fill('')
      await page.getByRole('checkbox', { name: 'Select works', exact: true }).check()
      const rail = page.getByRole('navigation', { name: 'installed providers', exact: true })
      // The entry, not the gear beside it, whose name ends in " settings".
      const railEntry = (name: string) =>
        rail.getByRole('link', { name: new RegExp(`\\b${name}\\b(?!.* settings$)`) })
      await railEntry('Second catalog').click()
      await page.waitForURL((url) => !url.pathname.startsWith(installation))
      // Every provider shares one table, so the other provider's rows are already there.
      await table.getByRole('checkbox', { name: 'Select another', exact: true }).waitFor()
      expect(await filter.inputValue()).toBe('')
      expect(
        await page.getByRole('checkbox', { name: 'Select works', exact: true }).isChecked(),
      ).toBe(true)
      await railEntry('Catalog').click()
      await page.waitForURL((url) => url.pathname === installation)
      expect(await railEntry('Catalog').getAttribute('aria-current')).toBe('page')
    })
    await step('A rail click brings even a short last provider up under the head', async () => {
      // The port is shorter than the table, but taller than the last provider.
      await page.setViewportSize({ width: 1440, height: 600 })
      const rail = page.getByRole('navigation', { name: 'installed providers', exact: true })
      await rail.getByRole('link', { name: /\bTail\b(?!.* settings$)/ }).click()
      await page.waitForURL((url) => !url.pathname.startsWith(installation))
      // The group, unlike its sticky heading, sits where the scroll left it.
      await expect
        .poll(() =>
          table.evaluate((element) => {
            const head = element.querySelector('[role="row"]')!.getBoundingClientRect().bottom
            const group = element.querySelector('[role="rowgroup"][aria-label="Tail"]')
            return group !== null && Math.abs(group.getBoundingClientRect().top - head) <= 2
          }),
        )
        .toBe(true)
      await page.setViewportSize({ width: 1440, height: 1000 })
      await page.goto(installation)
      await table.waitFor()
    })
    await step('Scrolling to another provider addresses it', async () => {
      // A short table cannot scroll, so the viewport shrinks until it does.
      await page.setViewportSize({ width: 1440, height: 420 })
      const wheel = () => page.mouse.wheel(0, 400)
      await table.hover()
      await wheel()
      await wheel()
      await page.waitForURL((url) => !url.pathname.startsWith(installation))
      await page.setViewportSize({ width: 1440, height: 1000 })
      await page.goto(installation)
      await table.waitFor()
    })
    await step('Reject an unknown operation URL', async () => {
      const response = await page.goto(`${installation}/ops/notAnOperation`)
      expect(response?.status()).toBe(404)
      expect(await page.getByRole('dialog').count()).toBe(0)
    })
  })
})
