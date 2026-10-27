import { expect, test } from 'vitest'
import { withOwnerSession } from '../../../../support/actors.ts'
import { openApiDocument } from '../../../../support/provider-fixtures.ts'
import {
  acceptTools,
  accountQuestion,
  addProviderFromFile,
  chooseAccount,
  chooseFile,
  collectDataResponses,
  confirmConfiguration,
  createWorkspace,
  openProviderSettings,
  waitForProviderStatus,
  waitForPublication,
} from '../../../../support/providers.ts'

test('Secret entry with one shared account, then each person’s own account', async (context) => {
  const secret = `fixture-secret-${crypto.randomUUID()}`
  await withOwnerSession(context, async ({ page, step, actor }) => {
    const responses = await collectDataResponses(page)
    let workspace = ''
    let ledgerPath = ''
    const document = openApiDocument(
      'Ledger',
      { listEntries: { summary: 'List entries' } },
      // GitHub-shaped input deliberately omits authentication metadata.
      {},
    )
    await step('One shared account is Ready as soon as it is added', async () => {
      workspace = await createWorkspace(actor.accessToken, `Secret ${crypto.randomUUID()}`)
      ledgerPath = await addProviderFromFile(page, workspace, document, 'ledger', {
        secret,
        authentication: 'Bearer token',
        account: 'shared',
      })
      await waitForProviderStatus(page, { path: ledgerPath }, 'Ready', { available: 1, total: 1 })
      await openProviderSettings(page)
      await page.getByText('People and agents use this shared account.', { exact: true }).waitFor()
      await page
        .getByRole('region', { name: 'One shared account', exact: true })
        .getByText('Connected', { exact: true })
        .waitFor()
    })
    await step('A bearer document with each person’s own account asks for no secret', async () => {
      await page.goto(`/workspaces/${workspace}/providers/new`)
      await chooseFile(page, 'ledger_theirs.json', document)
      await page.getByRole('button', { name: 'Read', exact: true }).click()
      // The first provider already has the document's title.
      await page.getByLabel('Display name', { exact: true }).fill('Their ledger')
      // The suggested namespace, ledger, belongs to the first provider.
      await page.getByLabel('Namespace', { exact: true }).fill('ledger_theirs')
      await confirmConfiguration(page, 'Bearer token')
      await acceptTools(page)
      await accountQuestion(page).waitFor()
      expect(await page.getByLabel('Token', { exact: true }).count()).toBe(0)
      await chooseAccount(page, 'own-later')
      await page.getByRole('button', { name: 'Add provider', exact: true }).click()
      await waitForPublication(page)
      // You cannot call it before you connect your own account.
      const theirs = { path: new URL(page.url()).pathname }
      await waitForProviderStatus(page, theirs, 'Connect your account')
      // Both list the same tool; open the new provider's own row.
      await page
        .locator(`#provider-group-${theirs.path.split('/').at(-1)}`)
        .getByRole('link', { name: 'listEntries', exact: true })
        .click()
      await page
        .getByRole('dialog', { name: 'listEntries', exact: true })
        .getByText('coral.providers.ledger_theirs.listEntries', { exact: true })
        .waitFor()
      await page.goto(`${theirs.path}/settings`)
      await page
        .getByText(
          'Each person connects their own account. Workspace owners connect accounts for agents.',
          { exact: true },
        )
        .waitFor()
    })
    await step('The secret appears in no response', async () => {
      expect((await page.content()).includes(secret)).toBe(false)
      for (const body of await responses()) expect(body).not.toContain(secret)
    })
    await step('Remove provider removes the one with a shared account in one step', async () => {
      await page.goto(`${ledgerPath}/settings`)
      await page.getByRole('button', { name: 'Remove provider', exact: true }).click()
      // The confirmation names the Call Root it releases.
      await page
        .getByRole('dialog')
        .getByRole('listitem')
        .filter({ hasText: 'Every tool under coral.providers.ledger' })
        .waitFor()
      await page
        .getByRole('dialog')
        .getByRole('button', { name: 'Remove provider', exact: true })
        .click()
      await page.waitForURL((url) => url.pathname === `/workspaces/${workspace}/providers`)
      const rail = page.getByRole('navigation', { name: 'installed providers', exact: true })
      await expect.poll(() => rail.getByRole('link', { name: / settings$/ }).count()).toBe(1)
      // The provider left is the one where each caller uses their own account.
      await openProviderSettings(page)
      await page
        .getByText(
          'Each person connects their own account. Workspace owners connect accounts for agents.',
          { exact: true },
        )
        .waitFor()
    })
  })
})
