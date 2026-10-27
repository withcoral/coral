import { expect, type TestContext } from 'vitest'
import {
  ProviderAccountScope,
  ProviderAccountState,
} from '../../../../generated/coral/management/v1/account_pb.ts'
import { managementClients } from '../../../../support/management.ts'
import { bearerMcpServer } from '../../../../support/provider-fixtures.ts'
import {
  addedProviders,
  acceptTools,
  chooseAccount,
  closeProviderSettings,
  collectDataResponses,
  confirmConfiguration,
  createWorkspace,
  openCustomSource,
  openTool,
  openProviderSettings,
  programRunner,
  readTools,
  waitForProviderStatus,
  waitForPublication,
} from '../../../../support/providers.ts'
import { withOwner } from '../../../../support/topology.ts'

/** Add an MCP server that needs a bearer token before it lists its Tools. */
export async function bearerDiscovery(context: TestContext) {
  const token = `fixture-bearer-${crypto.randomUUID()}`
  const upstream = await bearerMcpServer(context, token)
  await withOwner(context, async ({ page, target, step, accessToken }) => {
    const responses = await collectDataResponses(page)
    let workspace = ''
    let provider = ''
    let sharedAccount = ''
    await step('Add the server with its token', async () => {
      workspace = await createWorkspace(accessToken, `Bearer MCP ${crypto.randomUUID()}`)
      await page.goto(`/workspaces/${workspace}/providers/new`)
      await openCustomSource(page)
      await page.getByLabel('MCP server URL', { exact: true }).fill(`${upstream.origin}/mcp`)
      expect(await page.getByLabel('Access token', { exact: true }).count()).toBe(0)
      await page.getByRole('button', { name: 'Connect', exact: true }).click()
      await page.getByLabel('Namespace', { exact: true }).fill('greetings')
      await confirmConfiguration(page, 'Bearer token')
      await readTools(page, token)
      await acceptTools(page)
      // Explicit retention promotes the discovery grant; no second secret entry is needed.
      await chooseAccount(page, 'shared')
      expect(await page.getByLabel('Token', { exact: true }).count()).toBe(0)
      await page.getByText('One shared account', { exact: true }).waitFor()
      await page.getByRole('button', { name: 'Add provider', exact: true }).click()
      await waitForPublication(page)
      expect(upstream.counts.discovery).toBe(1)
      expect(upstream.counts.calls).toBe(0)
    })
    await step('The Provider is Ready with the shared account', async () => {
      await waitForProviderStatus(page, { path: new URL(page.url()).pathname }, 'Ready', {
        available: 1,
        total: 1,
      })
      await openProviderSettings(page)
      // The card's title is the setting in every deployment; its sentence is not.
      await page
        .getByRole('region', { name: 'One shared account', exact: true })
        .getByText('Connected', { exact: true })
        .waitFor()
      const api = managementClients(accessToken)
      const providers = addedProviders(
        (await api.providers.listProviders({ parent: `workspaces/${workspace}` })).providers,
      )
      const shared = (
        await api.accounts.getProviderAccount({
          parent: `workspaces/${workspace}`,
          provider: providers[0].name,
          scope: ProviderAccountScope.SHARED,
        })
      ).account!
      provider = providers[0].name
      sharedAccount = shared.id
      expect(shared.state).toBe(ProviderAccountState.READY)
      expect(shared.connectedTime).toBeDefined()
      expect(shared.configuredByUser).not.toBe('')
      expect(
        (
          await api.accounts.getProviderAccount({
            parent: `workspaces/${workspace}`,
            provider: providers[0].name,
            scope: ProviderAccountScope.USER,
          })
        ).account,
      ).toBeUndefined()
      await closeProviderSettings(page)
      const drawer = await openTool(page, 'hello')
      await drawer.getByText('coral.providers.greetings.hello', { exact: true }).waitFor()
    })
    await step('A Program run uses the credential', async () => {
      const run = programRunner(page, target, workspace)
      const result = await run('return await coral.providers.greetings.hello({});')
      expect(result).toContain('succeeded')
      expect(result).toContain('Hello from the bearer fixture')
      expect(result).not.toContain(token)
      expect(upstream.counts.discovery).toBe(1)
      expect(upstream.counts.calls).toBe(1)
    })
    await step(
      'Inspect and disconnect the singular shared account without selecting a replacement',
      async () => {
        await page.goto(`/${provider}/accounts`)
        await page.getByRole('heading', { name: 'Shared account', exact: true }).waitFor()
        await page
          .getByText('Upstream account name and identity are unavailable.', { exact: true })
          .waitFor()
        await page.getByText('Configured by Coral user:', { exact: false }).waitFor()
        expect(await page.getByRole('group', { name: /^Account / }).count()).toBe(1)
        expect(
          await page
            .getByRole('button', { name: /Use this|Connect another|Save account setting/ })
            .count(),
        ).toBe(0)
        await page.getByRole('button', { name: 'Disconnect', exact: true }).click()
        await page
          .getByRole('dialog')
          .getByRole('button', { name: 'Disconnect', exact: true })
          .click()
        await page.getByText('Disconnected', { exact: true }).waitFor()
        const account = (
          await managementClients(accessToken).accounts.getProviderAccount({
            parent: `workspaces/${workspace}`,
            provider,
            scope: ProviderAccountScope.SHARED,
          })
        ).account!
        expect(account.id).toBe(sharedAccount)
        expect(account.state).toBe(ProviderAccountState.DISCONNECTED)
        const result = await programRunner(
          page,
          target,
          workspace,
        )('return await coral.providers.greetings.hello({});')
        expect(result).not.toContain('Hello from the bearer fixture')
        expect(upstream.counts.calls).toBe(1)
      },
    )
    await step('The token appears in no page or view model', async () => {
      expect((await page.content()).includes(token)).toBe(false)
      for (const body of await responses()) expect(body).not.toContain(token)
    })
  })
}

/** Workspace discovery credentials retain the exact authorization scope chosen for them. */
export async function discoveryCredentialScope(
  choice: 'caller' | 'replaced',
  context: TestContext,
) {
  const token = `discovery-${crypto.randomUUID()}`
  const replacementToken = `replacement-${crypto.randomUUID()}`
  const upstream = await bearerMcpServer(context, token)
  await withOwner(context, async ({ page, step, accessToken }) => {
    const api = managementClients(accessToken)
    const responses = await collectDataResponses(page)
    const workspace = await createWorkspace(accessToken, `Discovery account ${crypto.randomUUID()}`)
    const parent = `workspaces/${workspace}`
    let candidate = ''
    await step('Review an explicit account choice after protected discovery', async () => {
      await page.goto(`/${parent}/providers/new`)
      await openCustomSource(page)
      await page.getByLabel('MCP server URL', { exact: true }).fill(`${upstream.origin}/mcp`)
      await page.getByRole('button', { name: 'Connect', exact: true }).click()
      await page.getByLabel('Namespace', { exact: true }).fill('reviewed')
      await confirmConfiguration(page, 'Bearer token')
      await readTools(page, token)
      await acceptTools(page)
      await chooseAccount(page, choice === 'caller' ? 'own-later' : 'shared')
      const setupName = `${parent}/providerSetups/${new URL(page.url()).searchParams.get('setup')}`
      const setup = await api.setups.getProviderSetup({ name: setupName })
      candidate = setup.provisionalGrantId
      expect(candidate).not.toBe('')
      if (choice === 'replaced') {
        // A second authorization must not silently replace the account the owner reviewed.
        const replacement = await api.setups.authorizeProviderSetup({
          name: setupName,
          configurationRevision: setup.configuration!.revision,
          credentialValues: { token: replacementToken },
        })
        expect(replacement.provisionalGrantId).not.toBe(candidate)
        expect(replacement.configuration?.revision).toBe(setup.configuration?.revision)
      }
    })
    await step(
      'Publication honors the reviewed account rather than the latest candidate',
      async () => {
        await page.getByRole('button', { name: 'Add provider', exact: true }).click()
        if (choice === 'replaced') {
          await page.getByText('Not added', { exact: true }).waitFor()
          expect((await api.providers.listProviders({ parent })).providers).toHaveLength(1)
        } else {
          await waitForPublication(page)
          const listed = await api.providers.listProviders({ parent })
          expect(listed.providers).toHaveLength(2)
          // Choosing individual accounts never silently promotes discovery credentials.
          // Explicit optional User retention has its own journey.
          for (const scope of [ProviderAccountScope.SHARED, ProviderAccountScope.USER]) {
            expect(
              (
                await api.accounts.getProviderAccount({
                  parent,
                  provider: addedProviders(listed.providers)[0].name,
                  scope,
                })
              ).account,
            ).toBeUndefined()
          }
        }
        expect(upstream.counts.discovery).toBe(1)
        expect(upstream.counts.calls).toBe(0)
        for (const body of await responses()) {
          expect(body).not.toContain(token)
          expect(body).not.toContain(replacementToken)
        }
      },
    )
  })
}
