import { toJsonString } from '@bufbuild/protobuf'
import { expect, type TestContext } from 'vitest'
import {
  ProviderAccountSchema,
  ProviderAccountScope,
  ProviderAccountState,
} from '../../../../generated/coral/management/v1/account_pb.ts'
import { inspectRunEvents, managementClients } from '../../../../support/management.ts'
import { oauthMcpServer } from '../../../../support/provider-fixtures.ts'
import {
  addedProviders,
  acceptTools,
  accountQuestion,
  chooseAccount,
  collectDataResponses,
  confirmConfiguration,
  createWorkspace,
  nextPublication,
  observeRetainedRun,
  observeRun,
  openCustomSource,
  programRunner,
  signInToReadTools,
  waitForProviderStatus,
  waitForPublication,
  signInAndShareAccount,
  committedPublication,
} from '../../../../support/providers.ts'
import { withOwner } from '../../../../support/topology.ts'

/**
 * Protected discovery, then an explicit shared account: `retain` promotes the
 * discovery sign-in, `different` replaces it. Both cross the real Reef, OAuth,
 * Publication, exact Account inspection and Workspace MCP interfaces.
 */
export async function protectedDiscovery(choice: 'retain' | 'different', context: TestContext) {
  const access = `fixture-access-${crypto.randomUUID()}`
  const replacementAccess = `fixture-replacement-${crypto.randomUUID()}`
  const upstream = await oauthMcpServer(context, access, {
    replacementAccess,
  })
  const exchanges = choice === 'retain' ? 1 : 2
  await withOwner(context, async ({ page, target, step, accessToken, approveConsent }) => {
    const api = managementClients(accessToken)
    const responses = await collectDataResponses(page)
    const safeResponses: string[] = []
    let workspace = ''
    let reviewedGrant = ''
    let discoveryGrant = ''
    // Coral commits the publication, but its response never reaches the
    // browser; the recovery step replays it.
    let frozenUrl = ''
    const loseNextPublication = () =>
      page.route('**/new.data*', async function lose(route) {
        // Loader revalidations pass; only the submission is lost.
        if (route.request().method() !== 'POST') return route.fallback()
        await page.unroute('**/new.data*', lose)
        frozenUrl = route.request().url()
        const response = await route.fetch()
        const body = await response.text()
        expect(body).toMatch(committedPublication)
        safeResponses.push(body)
        await route.abort('failed')
      })
    const setupGrant = async () =>
      (
        await api.setups.getProviderSetup({
          name: `workspaces/${workspace}/providerSetups/${new URL(page.url()).searchParams.get('setup')}`,
        })
      ).provisionalGrantId
    await step('Sign in to discover the protected tools', async () => {
      workspace = await createWorkspace(accessToken, `OAuth MCP ${crypto.randomUUID()}`)
      await page.goto(`/workspaces/${workspace}/providers/new`)
      await openCustomSource(page)
      await page.getByLabel('MCP server URL', { exact: true }).fill(upstream.mcpUrl)
      await page.getByRole('button', { name: 'Connect', exact: true }).click()
      expect(await page.getByLabel('Client ID', { exact: true }).inputValue()).toBe('')
      upstream.knobs.discoveryBroken = true
      await page.getByLabel('Namespace', { exact: true }).fill('oauthmcp')
      await confirmConfiguration(page)
      expect(await accountQuestion(page).count()).toBe(0)
      await signInToReadTools(page)
      await page.getByRole('link', { name: 'Approve fixture access', exact: true }).waitFor()
      expect(new URL(page.url()).searchParams.get('client_id')).toBe('dcr-client')
      await approveConsent()
      await page.getByText('Coral cannot read the tools', { exact: true }).waitFor()
      expect(upstream.counts.tokens).toBe(1)
      upstream.knobs.discoveryBroken = false
      await page.getByRole('button', { name: 'Try again', exact: true }).click()
      await acceptTools(page)
      await chooseAccount(page, 'shared')
      discoveryGrant = await setupGrant()
      expect(discoveryGrant).not.toBe('')
      expect(upstream.counts.tokens).toBe(1)
      expect(upstream.counts.registrations).toBe(1)
      expect(upstream.counts.consents).toBe(1)
      expect(
        (
          await api.providers.listProviders({
            parent: `workspaces/${workspace}`,
          })
        ).providers,
      ).toHaveLength(1)
    })
    await step('Explicitly choose the authorization to retain for shared use', async () => {
      await chooseAccount(page, 'shared')
      expect(await page.getByText('OAuth application', { exact: true }).count()).toBe(0)
      expect(await page.getByLabel('Callback URL', { exact: true }).count()).toBe(0)
      if (choice === 'different') {
        // Replacing the sign-in is an outcome of the shared route, not a
        // control beside it.
        await chooseAccount(page, 'shared-connect')
        await signInAndShareAccount(page)
        await page
          .getByRole('link', {
            name: 'Approve fixture access',
            exact: true,
          })
          .waitFor()
        expect(new URL(page.url()).searchParams.get('client_id')).toBe('dcr-client')
        // This sign-in answered the question, so its callback adds the
        // provider by itself. Its response is lost for the next step.
        await loseNextPublication()
        const published = nextPublication(page)
        await approveConsent()
        reviewedGrant = (await published).get('accountGrant') ?? ''
      } else {
        await page.getByRole('button', { name: 'Add provider', exact: true }).waitFor()
        reviewedGrant = await setupGrant()
      }
      expect(reviewedGrant).toBeTruthy()
      if (choice === 'retain') expect(reviewedGrant).toBe(discoveryGrant)
      else expect(reviewedGrant).not.toBe(discoveryGrant)
      expect(upstream.counts.consents).toBe(exchanges)
      expect(upstream.counts.tokens).toBe(exchanges)
      expect(upstream.counts.registrations).toBe(1)
    })
    await step('Recover a lost publication response without creating another account', async () => {
      if (choice === 'retain') {
        await loseNextPublication()
        await page.getByRole('button', { name: 'Add provider', exact: true }).click()
      }
      await page.waitForURL(
        (url) =>
          Boolean(frozenUrl) &&
          url.searchParams.get('requestId') === new URL(frozenUrl).searchParams.get('requestId'),
      )
      const parent = `workspaces/${workspace}`
      const committed = addedProviders((await api.providers.listProviders({ parent })).providers)
      expect(committed).toHaveLength(1)
      const inspect = () =>
        api.accounts.getProviderAccount({
          parent,
          provider: committed[0].name,
          scope: ProviderAccountScope.SHARED,
        })
      const account = (await inspect()).account!
      expect(account.id).toBe(reviewedGrant)
      expect(account.state).toBe(ProviderAccountState.READY)
      expect(account.connectedTime).toBeDefined()
      expect(account.configuredByUser).not.toBe('')
      const { currentUser } = await api.workspaces.getWorkspace({ name: parent })
      expect(`users/${account.configuredByUser}`).toBe(currentUser)
      expect(account.grantedScope).toBe('read write')
      expect(
        (
          await api.accounts.getProviderAccount({
            parent,
            provider: committed[0].name,
            scope: ProviderAccountScope.USER,
          })
        ).account,
      ).toBeUndefined()
      safeResponses.push(toJsonString(ProviderAccountSchema, account))
      await page.reload()
      expect(new URL(page.url()).searchParams.get('account')).toBe('fixed')
      expect(new URL(page.url()).searchParams.get('accountGrant')).toBe(reviewedGrant)
      await page.getByRole('button', { name: 'Retry', exact: true }).click()
      await waitForPublication(page)
      await waitForProviderStatus(page, { path: new URL(page.url()).pathname }, 'Ready', {
        available: 1,
        total: 1,
      })
      const replayed = addedProviders((await api.providers.listProviders({ parent })).providers)
      expect(replayed.map((provider) => provider.name)).toEqual(
        committed.map((provider) => provider.name),
      )
      expect((await inspect()).account).toEqual(account)
      expect(upstream.counts.tokens).toBe(exchanges)
    })
    await step(
      'Describe and execute through real MCP with the selected shared authorization',
      async () => {
        const run = programRunner(page, target, workspace)
        const description = await run.describe('coral.providers.oauthmcp.lookup')
        expect(description.isError).not.toBe(true)
        expect(JSON.stringify(description)).toContain('coral.providers.oauthmcp.lookup')
        safeResponses.push(JSON.stringify(description))
        const result = await run.exec(
          'coral.emit("execution-once"); return await coral.providers.oauthmcp.lookup({});',
        )
        const observation = await observeRun(run, result)
        const { update, events } = observation
        safeResponses.push(...observation.responses)
        expect(JSON.stringify(update.final_value)).toContain('Hello from the OAuth fixture')
        const history = await inspectRunEvents(api, `workspaces/${workspace}/runs/${update.run_id}`)
        const starts = history.flatMap((e) =>
          e.payload.case === 'invocationStarted' ? [e.payload.value] : [],
        )
        const ends = history.flatMap((e) =>
          e.payload.case === 'invocationEnded' ? [e.payload.value] : [],
        )
        expect(starts).toHaveLength(1)
        expect(ends).toHaveLength(1)
        expect(starts[0].invocation).toBeTruthy()
        expect(ends[0].invocation).toBe(starts[0].invocation)
        expect(events.filter((event) => event.kind === 'program.emitted')).toHaveLength(1)
        expect(history.filter((e) => e.payload.case === 'started')).toHaveLength(1)
        // A fresh MCP session reads the same durable output without replay.
        const observer = programRunner(page, target, workspace)
        const retained = await observeRetainedRun(observer, observation)
        safeResponses.push(...retained.responses)
        expect(run.calls.exec).toBe(1)
        expect(observer.calls.exec ?? 0).toBe(0)
        expect(upstream.counts.calls).toBe(1)
        expect(upstream.counts.tokens).toBe(exchanges)
        expect(upstream.counts.consents).toBe(exchanges)
      },
    )
    for (const body of [...(await responses()), ...safeResponses]) {
      expect(body).not.toContain(access)
      expect(body).not.toContain(replacementAccess)
      expect(body).not.toContain('disposable-code')
    }
  })
}
