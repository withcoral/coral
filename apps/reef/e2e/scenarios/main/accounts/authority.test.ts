import { expect, test } from 'vitest'
import { WorkspaceRole } from '../../../generated/coral/management/v1/workspace_pb.ts'
import { CredentialRoute_Kind } from '../../../generated/coral/management/v1/provider_pb.ts'
import {
  ProviderAccountScope,
  ProviderAccountState,
} from '../../../generated/coral/management/v1/account_pb.ts'
import { createActors } from '../../../support/actors.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'
import { managementClients } from '../../../support/management.ts'
import { openApiDocument } from '../../../support/provider-fixtures.ts'
import { breadcrumb, waitForNoAccount, waitForYourAccount } from '../../../support/providers.ts'

test('Account inspection preserves User privacy and exact Agent targeting', async (context) => {
  const { owner, alice, bob } = await createActors(context)
  const api = managementClients(owner.accessToken)
  const workspace = await api.workspaces.createWorkspace({
    displayName: 'Account authority',
    requestId: crypto.randomUUID(),
  })
  for (const actor of [alice, bob])
    await api.memberships.createWorkspaceMembership({
      parent: workspace.name,
      email: actor.email,
      role: WorkspaceRole.MEMBER,
      requestId: crypto.randomUUID(),
    })
  const created = await api.agents.createAgent({
    parent: workspace.name,
    displayName: 'Report bot',
    requestId: crypto.randomUUID(),
  })
  const agent = created.agent!
  const setup = await api.setups.createProviderSetup({
    parent: workspace.name,
    input: {
      case: 'openapiDocument',
      value: openApiDocument(
        'Accounts',
        { listEntries: { summary: 'List entries' } },
        { security: 'bearer' },
      ),
    },
  })
  await api.setups.confirmProviderSetupConfiguration({
    name: setup.name,
    expectedRevision: setup.configuration!.revision,
    configuration: setup.configuration!.suggested,
  })
  const publication = await api.providers.publishProvider({
    parent: workspace.name,
    setup: setup.name,
    requestId: crypto.randomUUID(),
    namespace: 'accounts',
    credentialRoute: { kind: CredentialRoute_Kind.CALLER },
  })
  const providerPath = `/${publication.provider}`
  const userPath = `/${workspace.name}/accounts/${publication.provider.split('/').at(-1)}`
  const agentPath = `${providerPath}/accounts?scope=agent&agent=${encodeURIComponent(agent.name.split('/').at(-1)!)}`
  const inspect = (accessToken: string) =>
    managementClients(accessToken).accounts.getProviderAccount({
      parent: workspace.name,
      provider: publication.provider,
      scope: ProviderAccountScope.USER,
    })
  let privateAuthorizationPath = ''
  let aliceAccount = ''
  await withBrowserSession(
    context,
    async ({ page, step }) => {
      await step('Alice connects her account without Owner privileges', async () => {
        await page.goto(userPath)
        await page.getByLabel('Token', { exact: true }).fill('alice-account-secret-canary')
        const [entered] = await Promise.all([
          page.waitForResponse((response) =>
            (response.headers().location ?? '').includes('/account-authorizations/'),
          ),
          page.getByRole('button', { name: 'Connect', exact: true }).click(),
        ])
        // The connection returns to the account page, which shows the stored account.
        await waitForYourAccount(page)
        privateAuthorizationPath = new URL(entered.headers().location, page.url()).pathname
        const account = (await inspect(alice.accessToken)).account!
        aliceAccount = account.id
        expect(account.state).toBe(ProviderAccountState.READY)
        expect(account.configuredByUser).toBe(alice.userId)
        expect(account.connectedTime).toBeDefined()
      })
      await step('A Member cannot provision an Agent or inspect shared accounts', async () => {
        expect((await page.request.get(agentPath)).status()).toBe(403)
        expect((await page.request.get(`${providerPath}/accounts?scope=shared`)).status()).toBe(403)
      })
      await step('Account mutations require same-origin CSRF-protected POSTs', async () => {
        const form = { intent: 'disconnect', account: aliceAccount }
        expect(
          (
            await page.request.post(userPath, {
              headers: { Origin: new URL(page.url()).origin },
              form,
            })
          ).status(),
        ).toBe(403)
        expect(
          (
            await page.request.post(userPath, {
              headers: { Origin: 'https://untrusted.example' },
              form,
            })
          ).status(),
        ).toBe(400) // React Router rejects cross-origin mutations before the action runs.
        expect((await inspect(alice.accessToken)).account?.state).toBe(ProviderAccountState.READY)
      })
    },
    { actor: alice.label, storageState: alice.storageState },
  )

  await withBrowserSession(
    context,
    async ({ page, step }) => {
      await step(
        'Workspace ownership does not expose Alice’s account or authorization',
        async () => {
          await page.goto(userPath)
          await waitForNoAccount(page)
          expect(await page.content()).not.toContain(aliceAccount)
          expect((await page.request.get(privateAuthorizationPath)).status()).toBe(404)
          expect((await inspect(owner.accessToken)).account).toBeUndefined()
          const csrf = await page.locator('input[name="csrf"]').first().inputValue()
          // The page acts only on your own account; a submitted account never retargets it.
          const response = await page.request.post(userPath, {
            headers: { Origin: new URL(page.url()).origin },
            form: { csrf, intent: 'disconnect', account: aliceAccount },
          })
          expect(response.status()).toBe(404)
        },
      )
      await step('An exact Agent URL provisions only that Agent’s account', async () => {
        await page.goto(agentPath)
        await breadcrumb(page).getByRole('link', { name: 'Report bot', exact: true }).waitFor()
        await page.getByRole('button', { name: 'Connect account', exact: true }).click()
        await page.getByLabel('Token', { exact: true }).fill('agent-account-secret-canary')
        await page.getByRole('button', { name: 'Connect account', exact: true }).click()
        await page.getByText('Account connected.', { exact: true }).waitFor()
        await page.goto(agentPath)
        await page.getByText('Ready', { exact: true }).waitFor()
        const account = (
          await api.accounts.getProviderAccount({
            parent: workspace.name,
            provider: publication.provider,
            scope: ProviderAccountScope.AGENT,
            agent: agent.name,
          })
        ).account!
        expect(account.state).toBe(ProviderAccountState.READY)
        expect(account.id).not.toBe(aliceAccount)
        expect(account.configuredByUser).toBe(owner.userId)
        await page.getByText(`Configured by Coral user: ${owner.userId}`, { exact: true }).waitFor()
        await page
          .getByText('Upstream account name and identity are unavailable.', { exact: true })
          .waitFor()
        expect((await inspect(owner.accessToken)).account).toBeUndefined()
      })
    },
    { actor: owner.label, storageState: owner.storageState },
  )

  await withBrowserSession(
    context,
    async ({ page, step }) => {
      await step('Bob cannot inspect or replace Alice’s account', async () => {
        expect((await page.request.get(privateAuthorizationPath)).status()).toBe(404)
        await page.goto(userPath)
        await waitForNoAccount(page)
        const csrf = await page.locator('input[name="csrf"]').first().inputValue()
        const response = await page.request.post(userPath, {
          headers: { Origin: new URL(page.url()).origin },
          form: {
            csrf,
            requestId: crypto.randomUUID(),
            intent: 'disconnect',
            account: aliceAccount,
          },
        })
        expect(response.status()).toBe(404)
        expect((await inspect(bob.accessToken)).account).toBeUndefined()
        expect((await inspect(alice.accessToken)).account?.state).toBe(ProviderAccountState.READY)
      })
    },
    { actor: bob.label, storageState: bob.storageState },
  )
})
