import { expect, test } from 'vitest'
import { withOwnerSession } from '../../../../support/actors.ts'
import { openApiDocument } from '../../../../support/provider-fixtures.ts'
import {
  addProviderFromFile,
  breadcrumb,
  createWorkspace,
  openProviderPolicy,
} from '../../../../support/providers.ts'

test('Select exact operations and apply one bounded policy change', async (context) => {
  await withOwnerSession(context, async ({ page, step, actor }) => {
    let providerPath = ''
    await step('Open the provider’s policy', async () => {
      const workspace = await createWorkspace(actor.accessToken, `Policy ${crypto.randomUUID()}`)
      providerPath = await addProviderFromFile(
        page,
        workspace,
        openApiDocument('Policy', {
          listEntries: { summary: 'List entries' },
          createEntry: { summary: 'Create entry' },
          deleteEntry: { summary: 'Delete entry' },
        }),
        'ledger',
      )
      await openProviderPolicy(page, providerPath)
      expect(await page.getByText('Needs approval · Default', { exact: true }).count()).toBe(3)
    })
    await step(
      'An atomic selection changes two exact operations, not the whole provider',
      async () => {
        await page
          .getByRole('checkbox', { name: 'coral.providers.ledger.listEntries', exact: true })
          .check()
        await page
          .getByRole('checkbox', { name: 'coral.providers.ledger.createEntry', exact: true })
          .check()
        await page.getByRole('radio', { name: 'Always allow', exact: true }).check()
        await page.getByRole('button', { name: 'Save selected operations', exact: true }).click()
        await page.getByText('Always allow · Exact rule', { exact: true }).first().waitFor()
        expect(await page.getByText('Always allow · Exact rule', { exact: true }).count()).toBe(2)
        expect(await page.getByText('Needs approval · Default', { exact: true }).count()).toBe(1)
      },
    )
    await step('Exact operation policy remains separate from its Call Path', async () => {
      await page.goto(`${providerPath}/policy?operation=deleteEntry`)
      await breadcrumb(page, 'Operation policy').waitFor()
      expect(await page.getByRole('checkbox').count()).toBe(1)
      await page
        .getByRole('checkbox', { name: 'coral.providers.ledger.deleteEntry', exact: true })
        .check()
      await page.getByRole('radio', { name: 'Disabled', exact: true }).check()
      await page.getByRole('button', { name: 'Save selected operations', exact: true }).click()
      await page.getByText('Disabled · Exact rule', { exact: true }).waitFor()
    })
  })
})
