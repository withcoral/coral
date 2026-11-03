import { expect, test } from 'vitest'
import { withOwnerSession } from '../../../../support/actors.ts'
import { openApiDocument } from '../../../../support/provider-fixtures.ts'
import {
  breadcrumb,
  createWorkspace,
  openProviderSettings,
  publishWorkspaceProvider,
} from '../../../../support/providers.ts'

test('Workspace tools preserve deep links, settings, and independent filters', async (context) => {
  await withOwnerSession(context, async ({ page, step, actor }) => {
    let installation = ''
    let providersPage = ''
    const table = page.getByRole('table', { name: 'Tools', exact: true })
    const filter = page.getByRole('searchbox', { name: 'Filter tools', exact: true })
    await step('Open the workspace tools table', async () => {
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
      providersPage = `/workspaces/${workspace}/providers`
      await page.goto(providersPage)
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
        // The second provider is taller than the table's port, so its tab scrolls.
        openApiDocument('Second catalog', {
          another: { summary: 'Another tool' },
          ...Object.fromEntries(Array.from({ length: 24 }, (_, index) => [`more${index}`, {}])),
        }),
        'other',
      )
      await page.goto(installation)
      await table.waitFor()
    })
    await step('Open and reload a tool details drawer', async () => {
      await page.goto(`${installation}/tools/broken`)
      await page
        .getByRole('dialog', { name: 'broken', exact: true })
        .getByText('Unavailable', { exact: true })
        .waitFor()
      await page.getByRole('dialog', { name: 'broken', exact: true }).waitFor()
      await page.reload()
      await page.getByRole('dialog', { name: 'broken', exact: true }).waitFor()
      expect(new URL(page.url()).pathname).toBe(`${installation}/tools/broken`)
      const drawer = page.getByRole('dialog', { name: 'broken', exact: true })
      await drawer.waitFor()
      await drawer.getByRole('button', { name: 'Close', exact: true }).click()
      await page.waitForURL((url) => url.pathname === installation)
      await table.getByRole('link', { name: 'works', exact: true }).click()
      await page.waitForURL((url) => url.pathname === `${installation}/tools/works`)
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
      await page.goto(`${installation}/tools/works`)
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
    await step('Keep the selection when you change tabs', async () => {
      await page.setViewportSize({ width: 1440, height: 1000 })
      await filter.fill('')
      await page.getByRole('checkbox', { name: 'Select works', exact: true }).check()
      const rail = page.getByRole('navigation', { name: 'installed providers', exact: true })
      // The entry, not the gear beside it, whose name ends in " settings".
      const railEntry = (name: string) =>
        rail.getByRole('link', { name: new RegExp(`\\b${name}\\b(?!.* settings$)`) })
      const group = (name: string) => table.getByRole('rowgroup', { name, exact: true })
      await railEntry('Second catalog').click()
      await page.waitForURL((url) => !url.pathname.startsWith(installation))
      // A provider's tab shows only that provider.
      await table.getByRole('checkbox', { name: 'Select another', exact: true }).waitFor()
      expect(await group('Catalog').count()).toBe(0)
      await expect.poll(() => railEntry('Second catalog').getAttribute('aria-current')).toBe('page')
      await railEntry('Catalog').click()
      await page.waitForURL((url) => url.pathname === installation)
      await expect.poll(() => railEntry('Catalog').getAttribute('aria-current')).toBe('page')
      expect(
        await page.getByRole('checkbox', { name: 'Select works', exact: true }).isChecked(),
      ).toBe(true)
      // All providers is the bare list address, and shows every provider.
      await railEntry('All providers').click()
      await page.waitForURL((url) => url.pathname === providersPage)
      await expect.poll(() => railEntry('All providers').getAttribute('aria-current')).toBe('page')
      await group('Second catalog').waitFor()
      expect(await group('Catalog').count()).toBe(1)
    })
    await step('Open a tool from All providers and stay on All providers', async () => {
      await page.setViewportSize({ width: 1440, height: 1000 })
      await page.goto(providersPage)
      await table.waitFor()
      await table.getByRole('link', { name: 'works', exact: true }).click()
      const drawer = page.getByRole('dialog', { name: 'works', exact: true })
      await drawer.waitFor()
      // Closing returns to the tab the drawer opened over, every provider's list.
      await drawer.getByRole('button', { name: 'Close', exact: true }).click()
      await page.waitForURL((url) => url.pathname === providersPage)
      await table.getByRole('rowgroup', { name: 'Second catalog', exact: true }).waitFor()
      await table.getByRole('rowgroup', { name: 'Catalog', exact: true }).waitFor()
    })
    await step('A new tab opens at its top', async () => {
      // A short viewport lets the table scroll.
      await page.setViewportSize({ width: 1440, height: 600 })
      await page.goto(providersPage)
      await table.waitFor()
      await table.evaluate((element) => {
        element.scrollTop = 300
      })
      await expect.poll(() => table.evaluate((element) => element.scrollTop)).toBeGreaterThan(0)
      await page
        .getByRole('navigation', { name: 'installed providers', exact: true })
        .getByRole('link', { name: /\bSecond catalog\b(?!.* settings$)/ })
        .click()
      await page.waitForURL((url) => url.pathname !== providersPage)
      await expect.poll(() => table.evaluate((element) => element.scrollTop)).toBe(0)
      await page.setViewportSize({ width: 1440, height: 1000 })
    })
    await step('Reject an unknown tool URL', async () => {
      const response = await page.goto(`${installation}/tools/notAnTool`)
      expect(response?.status()).toBe(404)
      expect(await page.getByRole('dialog').count()).toBe(0)
    })
  })
})
