import { expect, type TestContext } from 'vitest'
import {
  ProviderAccountScope,
  ProviderAccountState,
} from '../../../generated/coral/management/v1/account_pb.ts'
import { managementClients } from '../../../support/management.ts'
import { openApiDocument } from '../../../support/provider-fixtures.ts'
import {
  addProviderFromFile,
  createWorkspace,
  observeRun,
  programRunner,
  waitForYourAccount,
} from '../../../support/providers.ts'
import { withOwner } from '../../../support/topology.ts'

/** Connect your account from Accounts to continue a waiting run, or decline on the run. */
export async function connectionRequests(context: TestContext) {
  await withOwner(context, async ({ page, step, target, accessToken }) => {
    let workspace = ''
    let providerPath = ''
    let accountId = ''
    let selectedRun = ''
    let run: ReturnType<typeof programRunner>
    const api = managementClients(accessToken)
    const waitForRequest = async (
      program = 'return await coral.providers.connections.listEntries({});',
    ) => {
      const accepted = await run.exec(program)
      selectedRun = `workspaces/${workspace}/runs/${accepted.structuredContent.run_id}`
      await expect
        .poll(
          async () =>
            (await api.accounts.listUserAccounts({ parent: `workspaces/${workspace}` })).accounts
              .map((row) => row.pendingConnection)
              .filter(Boolean).length,
        )
        .toBe(1)
      return accepted
    }
    const openRun = () =>
      page.goto(`/workspaces/${workspace}/activity?record=${encodeURIComponent(selectedRun)}`)
    const inspect = () =>
      api.accounts.getProviderAccount({
        parent: `workspaces/${workspace}`,
        provider: providerPath.slice(1),
        scope: ProviderAccountScope.USER,
      })
    await step(
      'A waiting run asks you to connect on the run and on your Accounts page',
      async () => {
        workspace = await createWorkspace(accessToken, `Connections ${crypto.randomUUID()}`)
        providerPath = await addProviderFromFile(
          page,
          workspace,
          openApiDocument(
            'Connections',
            { listEntries: { summary: 'List entries', method: 'post' } },
            {},
          ),
          'connections',
          { authentication: 'Bearer token', account: 'own-later' },
        )
        run = programRunner(page, target, workspace, { waitMs: 100 })
        await waitForRequest()
        await run.endTask('success')
        await openRun()
        const review = page.getByRole('dialog', { name: 'Review action', exact: true })
        await review.getByRole('button', { name: 'Connect Connections', exact: true }).waitFor()
        await review.getByRole('button', { name: 'Decline', exact: true }).waitFor()
        // Accounts sits in the workspace navigation.
        await page.goto(`/workspaces/${workspace}`)
        await page.getByRole('link', { name: 'Accounts', exact: true }).click()
        await page.getByRole('heading', { name: 'Accounts', exact: true, level: 1 }).waitFor()
        await page.getByText('A connection request is waiting.', { exact: true }).waitFor()
        await page.getByText('Not connected', { exact: true }).waitFor()
      },
    )
    await step('Connecting from Accounts continues the waiting run', async () => {
      await page.getByRole('button', { name: 'Connect Connections', exact: true }).click()
      // A secret connects in a dialog on Accounts, and the account lands back here.
      const dialog = page.getByRole('dialog', { name: 'Connect Connections', exact: true })
      await dialog.getByLabel('Token', { exact: true }).fill('connection-secret-canary')
      await dialog.getByRole('button', { name: 'Connect account', exact: true }).click()
      await page.waitForURL((url) => url.pathname === `/workspaces/${workspace}/accounts`)
      await page.getByRole('heading', { name: 'Accounts', exact: true, level: 1 }).waitFor()
      await page.getByText('Connected', { exact: true }).waitFor()
      expect(
        await page.getByText('A connection request is waiting.', { exact: true }).count(),
      ).toBe(0)
      expect(await page.content()).not.toContain('connection-secret-canary')
      accountId = (await inspect()).account!.id
      // A click anywhere in the row, not only on its name, opens your account.
      await page
        .getByRole('table', { name: 'Accounts', exact: true })
        .getByRole('row')
        .filter({ hasText: 'Connections' })
        .getByRole('cell')
        .filter({ hasText: 'Connected' })
        .click()
      await waitForYourAccount(page)
      await page
        .getByRole('complementary', { name: 'Details', exact: true })
        .getByText(accountId, { exact: true })
        .waitFor()
      // The same run continues to its approval instead of failing the connection.
      await openRun()
      await page.getByRole('link', { name: /^Review .*connections\.listEntries$/ }).click()
      await page.getByRole('button', { name: 'Approve this call', exact: true }).waitFor()
    })
    await step(
      'Decline on the run fails the waiting call and leaves the account alone',
      async () => {
        await page.goto(`/workspaces/${workspace}/accounts`)
        await page.getByRole('button', { name: 'Disconnect', exact: true }).click()
        await page
          .getByRole('dialog')
          .getByRole('button', { name: 'Disconnect', exact: true })
          .click()
        await page.getByText('Not connected', { exact: true }).waitFor()
        run = programRunner(page, target, workspace, { waitMs: 100 })
        const accepted = await waitForRequest(
          'try { await coral.providers.connections.listEntries({}); } catch (error) { return error.code; }',
        )
        await openRun()
        await page
          .getByRole('dialog', { name: 'Review action', exact: true })
          .getByRole('button', { name: 'Decline', exact: true })
          .click()
        const settled = await observeRun(programRunner(page, target, workspace), accepted)
        expect(settled.update.final_value).toEqual({
          kind: 'value',
          value: 'account_connection_declined',
        })
        await expect
          .poll(
            async () =>
              (await api.accounts.listUserAccounts({ parent: `workspaces/${workspace}` })).accounts
                .map((row) => row.pendingConnection)
                .filter(Boolean).length,
          )
          .toBe(0)
        const account = (await inspect()).account!
        expect(account.id).toBe(accountId)
        expect(account.state).toBe(ProviderAccountState.DISCONNECTED)
      },
    )
  })
}
