import { expect, type TestContext } from 'vitest'
import { toJsonString } from '@bufbuild/protobuf'
import {
  ProviderAccountSchema,
  ProviderAccountScope,
  ProviderAccountState,
} from '../../../generated/coral/management/v1/account_pb.ts'
import { inspectRunEvents, managementClients } from '../../../support/management.ts'
import { oauthMcpServer } from '../../../support/provider-fixtures.ts'
import {
  acceptTools,
  breadcrumb,
  chooseAccount,
  collectDataResponses,
  confirmConfiguration,
  createWorkspace,
  openCustomSource,
  programRunner,
  signInToReadTools,
  waitForPublication,
} from '../../../support/providers.ts'
import { copyNewAgentKey } from '../../../support/agent-key.ts'
import { withOwner } from '../../../support/topology.ts'

/** The Owner provisions one Agent's own Account; that Agent's real bearer discovers and executes. */
export async function agentAccount(context: TestContext) {
  const access = `owner-account-${crypto.randomUUID()}`
  const agentAccess = `agent-account-${crypto.randomUUID()}`
  const upstream = await oauthMcpServer(context, access, { replacementAccess: agentAccess })
  await withOwner(context, async ({ page, target, step, accessToken, approveConsent }) => {
    const api = managementClients(accessToken)
    const responses = await collectDataResponses(page)
    let workspace = ''
    let provider = ''
    let agent = ''
    let agentPath = ''
    let credential = ''
    let ownerAccount = ''
    let run: ReturnType<typeof programRunner>
    const inspect = (scope: ProviderAccountScope, selectedAgent = '') =>
      api.accounts.getProviderAccount({
        parent: `workspaces/${workspace}`,
        provider,
        scope,
        agent: selectedAgent,
      })
    await step(
      'Keep the owner account separate and create an Agent without provisioning',
      async () => {
        workspace = await createWorkspace(accessToken, `Agent account ${crypto.randomUUID()}`)
        await page.goto(`/workspaces/${workspace}/providers/new`)
        await openCustomSource(page)
        await page.getByLabel('MCP server URL', { exact: true }).fill(upstream.mcpUrl)
        await page.getByRole('button', { name: 'Connect', exact: true }).click()
        await page.getByLabel('Namespace', { exact: true }).fill('automation')
        await confirmConfiguration(page)
        await signInToReadTools(page)
        await approveConsent()
        await acceptTools(page)
        await chooseAccount(page, 'own')
        await page.getByRole('button', { name: 'Add provider', exact: true }).click()
        await waitForPublication(page)
        provider = new URL(page.url()).pathname.slice(1)
        ownerAccount = toJsonString(
          ProviderAccountSchema,
          (await inspect(ProviderAccountScope.USER)).account!,
        )
        await page.goto(`/workspaces/${workspace}/agents`)
        await page.getByRole('button', { name: 'Create agent', exact: true }).click()
        await page.getByLabel('Agent name', { exact: true }).fill('Account worker')
        await page
          .getByRole('dialog')
          .getByRole('button', { name: 'Create agent', exact: true })
          .click()
        credential = await copyNewAgentKey(page)
        await page.getByRole('link', { name: 'Account worker', exact: true }).click()
        await breadcrumb(page, 'Account worker').waitFor()
        agentPath = new URL(page.url()).pathname
        agent = decodeURIComponent(agentPath.slice(1))
        run = programRunner(page, target, workspace, { accessToken: credential })
        expect((await inspect(ProviderAccountScope.AGENT, agent)).account).toBeUndefined()
      },
    )
    await step(
      'Unready Agent discovery and stale execution never copy the owner account or request sign-in',
      async () => {
        expect((await run.describe('coral.providers.automation.lookup')).isError).toBe(true)
        const stale = await run.exec('return await coral.providers.automation.lookup({});')
        expect(stale.structuredContent.state).toBe('failed')
        expect(upstream.counts.calls).toBe(0)
        expect(
          (
            await api.accounts.listUserAccounts({ parent: `workspaces/${workspace}` })
          ).accounts.filter((row) => row.pendingConnection),
        ).toHaveLength(0)
        await page
          .getByRole('button', { name: 'Connect account for automation', exact: true })
          .waitFor()
      },
    )
    await step(
      "The owner authorizes this Agent from the agent's page through the real callback",
      async () => {
        await page
          .getByRole('button', { name: 'Connect account for automation', exact: true })
          .click()
        const dialog = page.getByRole('dialog')
        await dialog
          .getByText('Connect this account for the agent. Your personal accounts stay the same.', {
            exact: true,
          })
          .waitFor()
        await dialog.getByRole('button', { name: 'Continue to sign in', exact: true }).click()
        await approveConsent()
        // The callback returns to the agent's page, where the row now shows the account.
        await page
          .getByRole('link', { name: 'Manage account for automation', exact: true })
          .waitFor()
        expect(decodeURIComponent(new URL(page.url()).pathname)).toBe(decodeURIComponent(agentPath))
        const account = (await inspect(ProviderAccountScope.AGENT, agent)).account!
        expect(account.state).toBe(ProviderAccountState.READY)
        expect(account.connectedTime).toBeDefined()
        expect(account.configuredByUser).toBe(JSON.parse(ownerAccount).configuredByUser)
        expect(account.id).not.toBe(JSON.parse(ownerAccount).id)
        const { currentUser } = await api.workspaces.getWorkspace({
          name: `workspaces/${workspace}`,
        })
        expect(`users/${account.configuredByUser}`).toBe(currentUser)
        expect(
          toJsonString(ProviderAccountSchema, (await inspect(ProviderAccountScope.USER)).account!),
        ).toBe(ownerAccount)
        expect(upstream.counts.consents).toBe(2)
        expect(upstream.counts.tokens).toBe(2)
      },
    )
    await step(
      'The same Agent bearer discovers and executes exactly once with Agent attribution',
      async () => {
        expect((await run.describe('coral.providers.automation.lookup')).isError).not.toBe(true)
        let update = (await run.exec('return await coral.providers.automation.lookup({});'))
          .structuredContent
        const events = [...update.events]
        for (let i = 0; i < 10 && (update.state === 'running' || update.has_more); i++) {
          update = (await run.wait(update)).structuredContent
          events.push(...update.events)
        }
        expect(update.state, JSON.stringify(update.failure)).toBe('succeeded')
        // The fixture rejects the owner's original token after the Agent's consent.
        expect(JSON.stringify(update.final_value)).toContain('Hello from the OAuth fixture')
        const history = await inspectRunEvents(api, `workspaces/${workspace}/runs/${update.run_id}`)
        expect(history.filter((e) => e.payload.case === 'invocationStarted')).toHaveLength(1)
        expect(
          events.every((e) => e.kind === 'program.emitted' || e.kind === 'program.logged'),
        ).toBe(true)
        expect(upstream.counts.calls).toBe(1)
        // A fresh Agent session can observe only its own durable Run, never a User's Run.
        // This proves attribution independently of the executing MCP session.
        const observer = programRunner(page, target, workspace, { accessToken: credential })
        const retained = await observer.wait({ ...update, next_cursor: '' })
        expect(retained.isError).not.toBe(true)
        expect(retained.structuredContent.run_id).toBe(update.run_id)
        expect(retained.structuredContent.state).toBe('succeeded')
        const other = await api.agents.createAgent({
          parent: `workspaces/${workspace}`,
          displayName: 'Other worker',
          requestId: crypto.randomUUID(),
        })
        const wrongAgent = programRunner(page, target, workspace, {
          accessToken: other.credential,
        })
        expect((await wrongAgent.wait({ ...update, next_cursor: '' })).isError).toBe(true)
        expect((await wrongAgent.describe('coral.providers.automation.lookup')).isError).toBe(true)
        for (const value of [JSON.stringify(update), ...(await responses())]) {
          expect(value).not.toContain(access)
          expect(value).not.toContain(agentAccess)
          expect(value).not.toContain('disposable-code')
        }
      },
    )
  })
}
