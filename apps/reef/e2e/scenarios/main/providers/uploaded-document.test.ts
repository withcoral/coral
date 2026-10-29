import { expect, test } from 'vitest'
import { withOwnerSession } from '../../../support/actors.ts'
import { openApiDocument } from '../../../support/provider-fixtures.ts'
import {
  addProviderFromFile,
  createWorkspace,
  openTool,
  setProviderEnabled,
  waitForProviderStatus,
} from '../../../support/providers.ts'

test('Add and manage a Workspace Provider from an uploaded document', async (context) => {
  await withOwnerSession(context, async ({ page, step, actor }) => {
    let workspace = ''
    let providerPath = ''
    await step('Add a Provider from an uploaded document', async () => {
      workspace = await createWorkspace(
        actor.accessToken,
        `Workspace Provider ${crypto.randomUUID()}`,
      )
      providerPath = await addProviderFromFile(
        page,
        workspace,
        openApiDocument('Weather', {
          getForecast: { summary: 'Get a forecast', tags: ['forecast'] },
          listStations: { summary: 'List stations', tags: ['stations'] },
        }),
        'weather',
      )
    })
    await step('The table shows its Tools under coral.providers.weather', async () => {
      let drawer = await openTool(page, 'getForecast')
      await drawer.getByText('coral.providers.weather.getForecast', { exact: true }).waitFor()
      expect(await drawer.getByText('Unavailable', { exact: true }).count()).toBe(0)
      await page.keyboard.press('Escape')
      await drawer.waitFor({ state: 'hidden' })
      drawer = await openTool(page, 'listStations')
      await drawer.getByText('coral.providers.weather.listStations', { exact: true }).waitFor()
      // The open drawer hides the table; close it before reading the heading.
      await page.keyboard.press('Escape')
      await drawer.waitFor({ state: 'hidden' })
      // The provider's heading counts its tools; none is Unavailable.
      await waitForProviderStatus(page, 'Weather', 'Ready', { available: 2, total: 2 })
    })
    await step('A disabled provider shows as disabled, and enabling restores it', async () => {
      // Reef offers no control for this; an operator changes it through Coral.
      await setProviderEnabled(actor.accessToken, providerPath, false)
      await page.goto(providerPath)
      await waitForProviderStatus(page, 'Weather', 'Provider disabled')
      await page.goto(`${providerPath}/settings`)
      await page.getByText('Disabled', { exact: true }).waitFor()
      await setProviderEnabled(actor.accessToken, providerPath, true)
      await page.goto(providerPath)
      await waitForProviderStatus(page, 'Weather', 'Ready', { available: 2, total: 2 })
    })
    await step('A stale rename from a second tab is refused and keeps the draft', async () => {
      await page.goto(`${providerPath}/settings`)
      const other = await page.context().newPage()
      try {
        await other.goto(`${providerPath}/settings`)
        await other.getByRole('button', { name: 'Edit provider name', exact: true }).click()
        await other.getByRole('textbox', { name: 'Provider name', exact: true }).fill('Stations')
        await other.getByRole('button', { name: 'Save', exact: true }).click()
        await other.getByRole('heading', { level: 2, name: 'Stations', exact: true }).waitFor()
      } finally {
        await other.close()
      }
      // This tab still holds the version from before the second tab's rename.
      await page.getByRole('button', { name: 'Edit provider name', exact: true }).click()
      const name = page.getByRole('textbox', { name: 'Provider name', exact: true })
      await name.fill('Forecasts')
      await name.press('Enter')
      await page
        .getByText(
          'This provider changed. Reload the page. Review the changes before you try again.',
          { exact: true },
        )
        .waitFor()
      expect(await name.inputValue()).toBe('Forecasts')
      await page.reload()
      await page.getByRole('heading', { level: 2, name: 'Stations', exact: true }).waitFor()
    })
    await step('Remove provider makes its former route unavailable', async () => {
      await page.goto(`${providerPath}/settings`)
      await page.getByRole('button', { name: 'Remove provider', exact: true }).click()
      await page
        .getByRole('dialog')
        .getByRole('button', { name: 'Remove provider', exact: true })
        .click()
      await page.waitForURL((url) => url.pathname === `/workspaces/${workspace}/providers`)
      // Every workspace keeps its built-in provider; only the removed one leaves.
      await page.getByRole('rowgroup', { name: 'Coral', exact: true }).waitFor()
      expect(await page.getByRole('rowgroup', { name: 'Stations', exact: true }).count()).toBe(0)
      expect((await page.request.get(providerPath)).status()).toBe(404)
    })
  })
})
