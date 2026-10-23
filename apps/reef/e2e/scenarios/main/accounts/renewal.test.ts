import { toJsonString } from '@bufbuild/protobuf'
import { expect, test } from 'vitest'
import {
  ProviderAccountSchema,
  ProviderAccountScope,
  ProviderAccountState,
} from '../../../generated/coral/management/v1/account_pb.ts'
import { authenticatedTarget, loginActor, mcpToken } from '../../../support/actors.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'
import { inspectRunEvents, managementClients } from '../../../support/management.ts'
import { oauthMcpServer } from '../../../support/provider-fixtures.ts'
import {
  acceptOperations,
  chooseAccount,
  collectDataResponses,
  confirmConfiguration,
  connectKeepingProgress,
  createWorkspace,
  openCustomSource,
  programRunner,
  signInToReadOperations,
  waitForNoAccount,
  waitForPublication,
} from '../../../support/providers.ts'

test.for(['user', 'shared', 'agent'] as const)(
  'Renew the exact %s Account without replay or stale callback resurrection',
  async (scope, context) => {
    const actor = await loginActor(context, authenticatedTarget().authentication.owner)
    const access = `original-${crypto.randomUUID()}`
    const replacement = `replacement-${crypto.randomUUID()}`
    const refreshed = `refreshed-${crypto.randomUUID()}`
    const upstream = await oauthMcpServer(context, access, {
      refreshAccess: refreshed,
      // The first renewed token also expires before the fence. Re-preparation
      // stays in this Invocation and must not create a second Operation send.
      firstRefreshExpiresIn: scope === 'shared' ? 0 : 3600,
    })
    const api = managementClients(actor.accessToken)
    await withBrowserSession(
      context,
      async ({ page, target, step }) => {
        const responses = await collectDataResponses(page)
        const safe: string[] = []
        let workspace = ''
        let provider = ''
        let accountPath = ''
        let agent = ''
        let agentCredential = ''
        let original = ''
        let accountId = ''
        let run: ReturnType<typeof programRunner>
        const inspect = () =>
          api.accounts.getProviderAccount({
            parent: `workspaces/${workspace}`,
            provider,
            scope: {
              user: ProviderAccountScope.USER,
              shared: ProviderAccountScope.SHARED,
              agent: ProviderAccountScope.AGENT,
            }[scope],
            agent,
          })
        const snapshot = async () => toJsonString(ProviderAccountSchema, (await inspect()).account!)
        const reconnect = async () => {
          if (scope === 'user') {
            // Your own account reconnects in one click; keep its progress page open.
            await page.goto(accountPath)
            await page.goto(await connectKeepingProgress(page, 'Reconnect'))
            await page.getByRole('button', { name: 'Continue to sign in', exact: true }).waitFor()
            return
          }
          await page.goto(accountPath)
          await page.getByRole('button', { name: 'Reconnect', exact: true }).click()
          await page
            .getByRole('dialog')
            .getByRole('button', { name: 'Reconnect', exact: true })
            .click()
          await page.getByRole('button', { name: 'Continue to sign in', exact: true }).waitFor()
        }
        const executeOnce = async () => {
          const callsBefore = upstream.counts.calls
          const execsBefore = run.calls.exec ?? 0
          let update = (
            await run.exec('coral.emit("once"); return await coral.providers.renewal.lookup({});')
          ).structuredContent
          const id = update.run_id
          const events = [...update.events]
          for (let i = 0; i < 100 && (update.state === 'running' || update.has_more); i++) {
            update = (await run.wait(update)).structuredContent
            expect(update.run_id).toBe(id)
            events.push(...update.events)
          }
          expect(update.state, JSON.stringify(update.failure)).toBe('succeeded')
          expect(JSON.stringify(update.final_value)).toContain('Hello from the OAuth fixture')
          const history = await inspectRunEvents(api, `workspaces/${workspace}/runs/${id}`)
          const starts = history.flatMap((e) =>
            e.payload.case === 'invocationStarted' ? [e.payload.value] : [],
          )
          const ends = history.flatMap((e) =>
            e.payload.case === 'invocationEnded' ? [e.payload.value] : [],
          )
          expect(starts).toHaveLength(1)
          expect(ends).toHaveLength(1)
          expect(ends[0].invocation).toBe(starts[0].invocation)
          expect(events.filter((e) => e.kind === 'program.emitted')).toHaveLength(1)
          expect(history.filter((e) => e.payload.case === 'started')).toHaveLength(1)
          expect(upstream.counts.calls).toBe(callsBefore + 1)
          expect(run.calls.exec).toBe(execsBefore + 1)
          safe.push(JSON.stringify(update), JSON.stringify(events))
        }
        await step('Connect the exact account through Reef', async () => {
          workspace = await createWorkspace(actor.accessToken, `Renewal ${crypto.randomUUID()}`)
          await page.goto(`/workspaces/${workspace}/providers/new`)
          await openCustomSource(page)
          await page.getByLabel('MCP server URL', { exact: true }).fill(upstream.mcpUrl)
          await page.getByRole('button', { name: 'Connect', exact: true }).click()
          await page.getByLabel('Namespace', { exact: true }).fill('renewal')
          await confirmConfiguration(page)
          await signInToReadOperations(page)
          await page.getByRole('link', { name: 'Approve fixture access', exact: true }).click()
          await acceptOperations(page)
          await chooseAccount(
            page,
            scope === 'shared' ? 'shared' : scope === 'user' ? 'own' : 'own-later',
          )
          await page.getByRole('button', { name: 'Add provider', exact: true }).click()
          await waitForPublication(page)
          provider = new URL(page.url()).pathname.slice(1)
          accountPath =
            scope === 'user'
              ? `/workspaces/${workspace}/accounts/${provider.split('/').at(-1)}`
              : `/${provider}/accounts?scope=${scope}`
          if (scope === 'agent') {
            const created = await api.agents.createAgent({
              parent: `workspaces/${workspace}`,
              displayName: 'Renewal worker',
              requestId: crypto.randomUUID(),
            })
            agent = created.agent!.name
            agentCredential = created.credential
            await page.goto(`/${agent}`)
            await page
              .getByRole('button', { name: 'Connect account for renewal', exact: true })
              .click()
            await page
              .getByRole('dialog')
              .getByRole('button', { name: 'Continue to sign in', exact: true })
              .click()
            await page.getByRole('link', { name: 'Approve fixture access', exact: true }).click()
            await page
              .getByRole('link', { name: 'Manage account for renewal', exact: true })
              .waitFor()
            accountPath += `&agent=${encodeURIComponent(agent.split('/').at(-1)!)}`
          }
          original = await snapshot()
          accountId = (await inspect()).account!.id
          run = programRunner(page, target, workspace, {
            waitMs: 100,
            accessToken: agentCredential || (await mcpToken(page, workspace)),
          })
          await executeOnce()
        })
        await step('Cancellation and failed OAuth preserve the active account', async () => {
          await reconnect()
          await page.getByRole('button', { name: 'Cancel', exact: true }).click()
          expect(await snapshot()).toBe(original)
          await reconnect()
          upstream.knobs.tokenError = 'invalid_grant'
          await page.getByRole('button', { name: 'Continue to sign in', exact: true }).click()
          await page.getByRole('link', { name: 'Approve fixture access', exact: true }).click()
          await page.getByText('The service rejected the sign-in code.', { exact: false }).waitFor()
          expect(await snapshot()).toBe(original)
          upstream.knobs.tokenError = undefined
          await executeOnce()
        })
        await step(
          'Successful replacement keeps the Account; refresh continues its triggering invocation',
          async () => {
            await reconnect()
            upstream.knobs.replacementAccess = replacement
            upstream.knobs.expiresIn = 0
            await page.getByRole('button', { name: 'Continue to sign in', exact: true }).click()
            await page.getByRole('link', { name: 'Approve fixture access', exact: true }).click()
            // Your own account returns to its page; another scope's progress page says so.
            await page
              .getByText('Account connected.', { exact: true })
              .or(page.getByRole('complementary', { name: 'Details', exact: true }))
              .waitFor()
            const connected = await snapshot()
            expect((await inspect()).account!.id).toBe(accountId)
            expect(connected).not.toBe(original)
            const consents = upstream.counts.consents
            const tokens = upstream.counts.tokens
            await executeOnce()
            const refreshes = scope === 'shared' ? 2 : 1
            expect(upstream.counts.refreshes).toBe(refreshes)
            expect(upstream.counts.tokens).toBe(tokens + refreshes)
            expect(upstream.counts.consents).toBe(consents)
            expect(await snapshot()).toBe(connected)
            safe.push(connected)
          },
        )
        await step('Disconnect wins over a late real OAuth callback', async () => {
          await reconnect()
          await page.getByRole('button', { name: 'Continue to sign in', exact: true }).click()
          const callback = await page
            .getByRole('link', { name: 'Approve fixture access', exact: true })
            .getAttribute('href')
          await page.goto(accountPath)
          await page.getByRole('button', { name: 'Disconnect', exact: true }).click()
          await page
            .getByRole('dialog')
            .getByRole('button', { name: 'Disconnect', exact: true })
            .click()
          // Your own account page shows no account; a shared or Agent account says Disconnected.
          if (scope === 'user') await waitForNoAccount(page)
          else await page.getByText('Disconnected', { exact: true }).waitFor()
          const disconnected = await snapshot()
          await page.goto(callback!)
          await page.getByText('Sign-in did not complete.', { exact: false }).waitFor()
          expect(await snapshot()).toBe(disconnected)
          expect((await inspect()).account!.state).toBe(ProviderAccountState.DISCONNECTED)
          // A User connects during invocation, so only an Agent loses discovery.
          expect((await run.describe('coral.providers.renewal.lookup')).isError).toBe(
            scope !== 'user',
          )
          safe.push(disconnected)
        })
        for (const body of [...(await responses()), ...safe]) {
          for (const secret of [access, replacement, refreshed, 'disposable-code'])
            expect(body).not.toContain(secret)
        }
      },
      { actor: actor.label, storageState: actor.storageState },
    )
  },
)
