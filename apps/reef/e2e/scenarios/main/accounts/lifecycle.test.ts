import { expect, test } from 'vitest'
import {
  ProviderAccountScope,
  ProviderAccountState,
} from '../../../generated/coral/management/v1/account_pb.ts'
import { withOwnerSession } from '../../../support/actors.ts'
import { managementClients } from '../../../support/management.ts'
import { openApiDocument } from '../../../support/provider-fixtures.ts'
import {
  addProviderFromFile,
  collectDataResponses,
  createWorkspace,
  openProviderSettings,
  openYourAccount,
} from '../../../support/providers.ts'

test('Connect, reconnect and disconnect retain one exact caller account', async (context) => {
  const secret = `account-secret-${crypto.randomUUID()}`
  await withOwnerSession(context, async ({ page, step, actor }) => {
    const responses = await collectDataResponses(page)
    const api = managementClients(actor.accessToken)
    let providerPath = ''
    let parent = ''
    let accountId = ''
    let connectedAt = ''
    let accountPage = ''
    const inspect = () =>
      api.accounts.getProviderAccount({
        parent,
        provider: providerPath.slice(1),
        scope: ProviderAccountScope.USER,
      })
    await step('Add a provider with individual accounts', async () => {
      const workspace = await createWorkspace(actor.accessToken, `Accounts ${crypto.randomUUID()}`)
      parent = `workspaces/${workspace}`
      providerPath = await addProviderFromFile(
        page,
        workspace,
        openApiDocument('Accounts', { listEntries: { summary: 'List entries' } }, {}),
        'accounts',
        { authentication: 'Bearer token', account: 'own-later' },
      )
      await openProviderSettings(page)
      await openYourAccount(page)
      await page.getByRole('button', { name: 'Connect Accounts', exact: true }).waitFor()
      await page.getByText('No connected account.', { exact: true }).waitFor()
      expect((await inspect()).account).toBeUndefined()
      accountPage = new URL(page.url()).pathname
    })
    await step('Connect one account without a separate selection', async () => {
      await page.getByLabel('Token', { exact: true }).fill(secret)
      await page.getByRole('button', { name: 'Connect Accounts', exact: true }).click()
      await page.getByText('Ready', { exact: true }).waitFor()
      expect(new URL(page.url()).pathname).toBe(accountPage)
      const account = (await inspect()).account!
      expect(account.state).toBe(ProviderAccountState.READY)
      expect(account.connectedTime).toBeDefined()
      expect(account.configuredByUser).not.toBe('')
      accountId = account.id
      connectedAt = JSON.stringify(account.connectedTime, (_, value) =>
        typeof value === 'bigint' ? String(value) : value,
      )
      await page.getByText('Ready', { exact: true }).waitFor()
      await page
        .getByText('Upstream account name and identity are unavailable.', { exact: true })
        .waitFor()
      // Your own account page does not name you as its configurer.
      expect(await page.getByText('Configured by Coral user', { exact: false }).count()).toBe(0)
      expect(await page.getByRole('group', { name: /^Account / }).count()).toBe(1)
      expect(
        await page.getByRole('button', { name: 'Connect Accounts', exact: true }).count(),
      ).toBe(0)
      expect(
        await page.getByRole('button', { name: /Use this|Save account setting/ }).count(),
      ).toBe(0)
    })
    await step(
      'Explicit reconnect retains the Account ID and records the successful connection',
      async () => {
        await page.getByRole('button', { name: 'Reconnect', exact: true }).click()
        await page.getByLabel('Token', { exact: true }).fill(`${secret}-replacement`)
        // Reconnect goes through the authorization page, then lands back here.
        await page.getByRole('button', { name: 'Connect account', exact: true }).click()
        await page.waitForURL((url) => url.pathname === accountPage)
        const account = (await inspect()).account!
        expect(account.id).toBe(accountId)
        expect(account.state).toBe(ProviderAccountState.READY)
        expect(
          JSON.stringify(account.connectedTime, (_, value) =>
            typeof value === 'bigint' ? String(value) : value,
          ),
        ).not.toBe(connectedAt)
        await page.getByText('Ready', { exact: true }).waitFor()
      },
    )
    await step('Disconnect keeps the account but does not present it as connected', async () => {
      await page.getByRole('button', { name: 'Disconnect', exact: true }).click()
      await page
        .getByRole('dialog')
        .getByRole('button', { name: 'Disconnect', exact: true })
        .click()
      await page.getByText('Disconnected', { exact: true }).waitFor()
      await page.getByText('No connected account.', { exact: true }).waitFor()
      const account = (await inspect()).account!
      expect(account.id).toBe(accountId)
      expect(account.state).toBe(ProviderAccountState.DISCONNECTED)
    })
    await step(
      'Connect again uses the same scoped account, not a spare inventory item',
      async () => {
        await page.getByLabel('Token', { exact: true }).fill(`${secret}-again`)
        await page.getByRole('button', { name: 'Connect Accounts', exact: true }).click()
        await page.getByText('Ready', { exact: true }).waitFor()
        expect(new URL(page.url()).pathname).toBe(accountPage)
        expect((await inspect()).account?.id).toBe(accountId)
        await page.getByText('Ready', { exact: true }).waitFor()
        expect(await page.getByRole('group', { name: /^Account / }).count()).toBe(1)
      },
    )
    await step('No credential is echoed into browser documents or route data', async () => {
      expect(await page.content()).not.toContain(secret)
      for (const body of await responses()) expect(body).not.toContain(secret)
    })
  })
})
