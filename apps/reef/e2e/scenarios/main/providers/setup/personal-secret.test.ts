import { expect, test } from 'vitest'
import { authenticatedTarget, loginActor, mcpToken } from '../../../../support/actors.ts'
import {
  ProviderAccountScope,
  ProviderAccountState,
} from '../../../../generated/coral/management/v1/account_pb.ts'
import { withBrowserSession } from '../../../../support/browser-session.ts'
import { managementClients } from '../../../../support/management.ts'
import { bearerMcpServer } from '../../../../support/provider-fixtures.ts'
import {
  acceptTools,
  chooseAccount,
  collectDataResponses,
  confirmConfiguration,
  createWorkspace,
  openCustomSource,
  programRunner,
  readTools,
  waitForPublication,
} from '../../../../support/providers.ts'

test.for(['initial', 'different'] as const)(
  'Connect a personal secret during Add: %s authorization',
  async (choice, context) => {
    const actor = await loginActor(context, authenticatedTarget().authentication.owner)
    const discoveryToken = `discovery-${crypto.randomUUID()}`
    const personalToken = `personal-${crypto.randomUUID()}`
    const upstream = await bearerMcpServer(context, discoveryToken, choice === 'initial')
    const api = managementClients(actor.accessToken)
    await withBrowserSession(
      context,
      async ({ page, target, step }) => {
        const responses = await collectDataResponses(page)
        let workspace = ''
        let provider = ''
        let grant = ''
        await step('Connect your own secret without creating a shared account', async () => {
          workspace = await createWorkspace(
            actor.accessToken,
            `Personal secret ${crypto.randomUUID()}`,
          )
          await page.goto(`/workspaces/${workspace}/providers/new`)
          await openCustomSource(page)
          await page.getByLabel('MCP server URL', { exact: true }).fill(`${upstream.origin}/mcp`)
          await page.getByRole('button', { name: 'Connect', exact: true }).click()
          await page.getByLabel('Namespace', { exact: true }).fill('personalSecret')
          await confirmConfiguration(page, 'Bearer token')
          if (choice === 'different') {
            await readTools(page, discoveryToken)
          }
          await acceptTools(page)
          // The same outcome whether or not discovery left a sign-in in hand:
          // your own account is the one you connect here, never the one that
          // read the tools.
          await chooseAccount(page, 'own-connect')
          const setupName = `workspaces/${workspace}/providerSetups/${new URL(page.url()).searchParams.get('setup')}`
          const discovery = await api.setups.getProviderSetup({ name: setupName })
          expect(await page.getByLabel('Token', { exact: true }).count()).toBe(0)
          await page.getByRole('button', { name: 'Connect your account', exact: true }).click()
          const dialog = page.getByRole('dialog', { name: 'Connect your account', exact: true })
          await dialog.getByLabel('Token', { exact: true }).fill(personalToken)
          await dialog.getByRole('button', { name: 'Save your account', exact: true }).click()
          await page.getByRole('button', { name: 'Add provider', exact: true }).waitFor()
          grant = new URL(page.url()).searchParams.get('accountGrant')!
          expect(grant).toBeTruthy()
          expect(grant).not.toBe(discovery.provisionalGrantId)
          await page.getByRole('button', { name: 'Add provider', exact: true }).click()
          await waitForPublication(page)
          provider = new URL(page.url()).pathname.slice(1)
          const inspect = (scope: ProviderAccountScope) =>
            api.accounts.getProviderAccount({ parent: `workspaces/${workspace}`, provider, scope })
          const account = (await inspect(ProviderAccountScope.USER)).account!
          expect(account.id).toBe(grant)
          expect(account.state).toBe(ProviderAccountState.READY)
          expect((await inspect(ProviderAccountScope.SHARED)).account).toBeUndefined()
          expect((await api.setups.getProviderSetup({ name: setupName })).authorized).toBe(false)
          expect(upstream.counts.discovery).toBe(1)
        })
        await step('Only the personal credential authorizes the real upstream call', async () => {
          upstream.acceptToken(personalToken)
          const run = programRunner(page, target, workspace, {
            accessToken: await mcpToken(page, workspace),
          })
          let result = await run.exec('return await coral.providers.personalSecret.hello({});')
          for (
            let pages = 0;
            pages < 10 &&
            (result.structuredContent.state === 'running' || result.structuredContent.has_more);
            pages++
          )
            result = await run.wait(result.structuredContent)
          expect(result.structuredContent.state).toBe('succeeded')
          expect(JSON.stringify(result)).toContain('Hello from the bearer fixture')
          expect(upstream.counts.calls).toBe(1)
          expect(upstream.counts.discovery).toBe(1)
          for (const body of [...(await responses()), JSON.stringify(result)]) {
            expect(body).not.toContain(discoveryToken)
            expect(body).not.toContain(personalToken)
          }
        })
      },
      { actor: actor.label, storageState: actor.storageState },
    )
  },
)
