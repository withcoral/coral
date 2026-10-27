import { toJsonString } from '@bufbuild/protobuf'
import { expect, type TestContext } from 'vitest'
import {
  AccountConnectionState,
  ProviderAccountSchema,
  ProviderAccountScope,
  ProviderAccountState,
} from '../../../generated/coral/management/v1/account_pb.ts'
import { inspectRunEvents, managementClients } from '../../../support/management.ts'
import { oauthMcpServer } from '../../../support/provider-fixtures.ts'
import {
  acceptTools,
  chooseAccount,
  collectDataResponses,
  confirmConfiguration,
  createWorkspace,
  nextPublication,
  openCustomSource,
  programRunner,
  signInToReadTools,
  waitForPublication,
} from '../../../support/providers.ts'
import { withOwner } from '../../../support/topology.ts'

/**
 * A separate User account from discovery to one execution. `connect` leaves the
 * account unconnected, so the first Invocation waits on an Account Connection
 * Request and the same Run continues after consent. `retry` replaces the
 * discovery sign-in for individual accounts; retaining or replacing it for a
 * shared account is oauth-mcp-discovery's table.
 */
export async function separateUserAccount(choice: 'connect' | 'retry', context: TestContext) {
  const access = `user-account-${crypto.randomUUID()}`
  const replacementAccess = `personal-replacement-${crypto.randomUUID()}`
  const upstream = await oauthMcpServer(context, access, { replacementAccess })
  const exchanges = choice === 'retry' ? 3 : 2
  await withOwner(context, async ({ page, target, step, accessToken, approveConsent }) => {
    const api = managementClients(accessToken)
    const responses = await collectDataResponses(page)
    const safeResponses: string[] = []
    let workspace = ''
    let provider = ''
    let grant = ''
    let originalGrant = ''
    const inspect = () =>
      api.accounts.getProviderAccount({
        parent: `workspaces/${workspace}`,
        provider,
        scope: ProviderAccountScope.USER,
      })
    await step('Discover tools and choose individual accounts explicitly', async () => {
      workspace = await createWorkspace(accessToken, `User account ${crypto.randomUUID()}`)
      await page.goto(`/workspaces/${workspace}/providers/new`)
      await openCustomSource(page)
      await page.getByLabel('MCP server URL', { exact: true }).fill(upstream.mcpUrl)
      await page.getByRole('button', { name: 'Connect', exact: true }).click()
      await page.getByLabel('Namespace', { exact: true }).fill('personal')
      await confirmConfiguration(page)
      await signInToReadTools(page)
      await approveConsent()
      await acceptTools(page)
      await chooseAccount(page, 'own')
      grant = (
        await api.setups.getProviderSetup({
          name: `workspaces/${workspace}/providerSetups/${new URL(page.url()).searchParams.get('setup')}`,
        })
      ).provisionalGrantId
      expect(grant).toBeTruthy()
      originalGrant = grant
      if (choice === 'retry') {
        await page.getByRole('link', { name: 'Back', exact: true }).click()
        await page.getByRole('heading', { name: 'Tools', exact: true }).waitFor()
        await page.getByRole('link', { name: 'Back', exact: true }).click()
        upstream.knobs.discoveryBroken = true
        await page.getByLabel('Client ID', { exact: true }).fill('personal-client')
        await confirmConfiguration(page)
        await signInToReadTools(page)
        await approveConsent()
        await page.getByText('Coral cannot read the tools', { exact: true }).waitFor()
        upstream.knobs.discoveryBroken = false
        await page.getByRole('button', { name: 'Try again', exact: true }).click()
        grant = (
          await api.setups.getProviderSetup({
            name: `workspaces/${workspace}/providerSetups/${new URL(page.url()).searchParams.get('setup')}`,
          })
        ).provisionalGrantId
        expect(grant).not.toBe(originalGrant)
        const rediscovered = grant
        await acceptTools(page)
        // Individual accounts can start with a new sign-in: the outcome names
        // the grant it replaces, and never shares the account it produces.
        // That sign-in answers the question, so it adds the provider.
        await chooseAccount(page, 'own-connect')
        await page.getByRole('button', { name: 'Sign in again', exact: true }).click()
        const published = nextPublication(page)
        await approveConsent()
        const form = await published
        expect(form.get('account')).toBe('caller')
        grant = form.get('accountGrant') ?? ''
        expect(grant).toBeTruthy()
        expect(grant).not.toBe(rediscovered)
      } else {
        await chooseAccount(page, 'own-later')
        await page.getByRole('button', { name: 'Add provider', exact: true }).click()
      }
      await waitForPublication(page)
      provider = new URL(page.url()).pathname.slice(1)
      const account = (await inspect()).account
      if (choice !== 'connect') {
        expect(account!.id).toBe(grant)
        expect(account!.state).toBe(ProviderAccountState.READY)
      } else expect(account).toBeUndefined()
      expect(upstream.counts.tokens).toBe(choice === 'connect' ? 1 : exchanges)
      expect(upstream.counts.consents).toBe(choice === 'connect' ? 1 : exchanges)
      expect(
        (
          await api.accounts.getProviderAccount({
            parent: `workspaces/${workspace}`,
            provider,
            scope: ProviderAccountScope.SHARED,
          })
        ).account,
      ).toBeUndefined()
    })
    await step(
      'Continue the original MCP invocation through the private Reef OAuth request',
      async () => {
        const run = programRunner(page, target, workspace, { waitMs: 100 })
        // A first-time User discovers the Tool before connecting; the
        // Invocation, not discovery, asks for the account.
        for (const found of [await run.search(), await run.search('lookup')]) {
          expect(found.isError).not.toBe(true)
          expect(found.structuredContent.hits.map((hit) => hit.call_path)).toContain(
            'coral.providers.personal.lookup',
          )
          safeResponses.push(JSON.stringify(found))
        }
        const description = await run.describe('coral.providers.personal.lookup')
        expect(description.isError).not.toBe(true)
        const pending = async () =>
          (
            await api.accounts.listUserAccounts({
              parent: `workspaces/${workspace}`,
            })
          ).accounts.flatMap((row) => (row.pendingConnection ? [row.pendingConnection] : []))
        expect(await pending()).toEqual([])
        expect(upstream.counts.calls).toBe(0)
        // Your own account is not Provider health: only your Accounts page shows it.
        expect((await api.providers.getProvider({ name: provider })).blockers).toEqual([])
        const result = await run.exec(
          'coral.emit("execution-once"); return await coral.providers.personal.lookup({});',
        )
        expect(result.isError).not.toBe(true)
        let update = result.structuredContent
        const runId = update.run_id
        const events = [...update.events]
        let requestName = ''
        if (choice === 'connect') {
          expect(update.state).toBe('running')
          expect(upstream.counts.calls).toBe(0)
          await expect
            .poll(async () => {
              const requests = await pending()
              requestName = requests[0]?.name ?? ''
              return requests.length
            })
            .toBe(1)
          // Observe rather than exec again; the current action identifies the human work.
          update = (await run.wait(update)).structuredContent
          events.push(...update.events)
          expect(update.required_actions).toEqual([
            expect.objectContaining({
              kind: 'account_connection',
              request_id: requestName.split('/').at(-1),
              required_actor: 'invoking_user',
            }),
          ])
          await page.goto(update.required_actions[0].run_status_url)
          // One click goes straight to the provider's sign-in.
          await page
            .getByRole('dialog', { name: 'Review action', exact: true })
            .getByRole('button', { name: /^Connect / })
            .click()
          // The grant is already expired when the waiting Invocation resumes.
          // Its refresh must continue this same call, not require another exec.
          upstream.knobs.expiresIn = 0
          upstream.knobs.refreshAccess = replacementAccess
          await approveConsent()
          // Sign-in returns to the same Run and request.
          await page.waitForURL((url) => url.searchParams.get('request') === requestName)
          await page
            .getByText('Account connected. Coral continues after its other checks.', {
              exact: true,
            })
            .waitFor()
          expect(
            (
              await api.accounts.getAccountConnectionRequest({
                name: requestName,
              })
            ).state,
          ).toBe(AccountConnectionState.CONNECTED)
        }
        for (
          let pages = 0;
          pages < 100 && (update.state === 'running' || update.has_more);
          pages++
        ) {
          update = (await run.wait(update)).structuredContent
          expect(update.run_id).toBe(runId)
          events.push(...update.events)
          safeResponses.push(JSON.stringify(update))
        }
        expect(update.state, JSON.stringify(update.failure)).toBe('succeeded')
        expect(update.has_more).toBe(false)
        expect(JSON.stringify(update.final_value)).toContain('Hello from the OAuth fixture')
        const history = await inspectRunEvents(api, `workspaces/${workspace}/runs/${runId}`)
        const starts = history.flatMap((e) =>
          e.payload.case === 'invocationStarted' ? [e.payload.value] : [],
        )
        const ends = history.flatMap((e) =>
          e.payload.case === 'invocationEnded' ? [e.payload.value] : [],
        )
        expect(starts).toHaveLength(1)
        expect(ends).toHaveLength(1)
        expect(ends[0].invocation).toBe(starts[0].invocation)
        expect(events.filter((event) => event.kind === 'program.emitted')).toHaveLength(1)
        expect(JSON.stringify(events)).toContain('execution-once')
        expect(history.filter((e) => e.payload.case === 'started')).toHaveLength(1)
        expect(update.required_actions).toEqual([])
        expect(run.calls.exec).toBe(1)
        expect(upstream.counts.calls).toBe(1)
        expect(upstream.counts.tokens).toBe(exchanges + (choice === 'connect' ? 1 : 0))
        expect(upstream.counts.refreshes).toBe(choice === 'connect' ? 1 : 0)
        expect(upstream.counts.consents).toBe(exchanges)
        const account = (await inspect()).account!
        expect(account.state).toBe(ProviderAccountState.READY)
        expect((await api.providers.getProvider({ name: provider })).blockers).toEqual([])
        expect(account.connectedTime).toBeDefined()
        expect(account.configuredByUser).toBeTruthy()
        // The Owner who connected it, whether signed in or the implicit local Owner.
        const { currentUser } = await api.workspaces.getWorkspace({
          name: `workspaces/${workspace}`,
        })
        expect(`users/${account.configuredByUser}`).toBe(currentUser)
        expect(account.id).not.toBe(originalGrant)
        safeResponses.push(
          toJsonString(ProviderAccountSchema, account),
          JSON.stringify(result),
          JSON.stringify(description),
          JSON.stringify(events),
        )
        expect((await run.describe('coral.providers.personal.lookup')).isError).not.toBe(true)
      },
    )
    for (const body of [...(await responses()), ...safeResponses]) {
      expect(body).not.toContain(access)
      expect(body).not.toContain(replacementAccess)
      expect(body).not.toContain('disposable-code')
    }
  })
}
