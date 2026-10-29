import { expect, test } from 'vitest'
import { ProviderAccountScope } from '../../../../generated/coral/management/v1/account_pb.ts'
import { authenticatedTarget, loginActor } from '../../../../support/actors.ts'
import { withBrowserSession } from '../../../../support/browser-session.ts'
import { managementClients } from '../../../../support/management.ts'
import { oauthApiServer } from '../../../../support/provider-fixtures.ts'
import {
  acceptTools,
  chooseAccount,
  chooseFile,
  confirmConfiguration,
  createWorkspace,
  nextPublication,
  waitForPublication,
} from '../../../../support/providers.ts'

test('Concurrent personal sign-ins preserve each creation choice in the same browser', async (context) => {
  const actor = await loginActor(context, authenticatedTarget().authentication.owner)
  const api = managementClients(actor.accessToken)
  const upstreams = [
    await oauthApiServer(context, `personal-one-${crypto.randomUUID()}`),
    await oauthApiServer(context, `personal-two-${crypto.randomUUID()}`),
  ]
  await withBrowserSession(
    context,
    async ({ page, step }) => {
      const workspace = await createWorkspace(
        actor.accessToken,
        `Concurrent personal ${crypto.randomUUID()}`,
      )
      const flows: { consent: string; setup: string }[] = []
      await step('Start two personal sign-ins before completing either callback', async () => {
        for (const [index, upstream] of upstreams.entries()) {
          await page.goto(`/workspaces/${workspace}/providers/new`)
          await chooseFile(page, 'records.json', upstream.document('Personal records'))
          await page.getByRole('button', { name: 'Read', exact: true }).click()
          await page.getByLabel('Namespace', { exact: true }).fill(`personal${index}`)
          await confirmConfiguration(page)
          await acceptTools(page)
          const setup = `workspaces/${workspace}/providerSetups/${new URL(page.url()).searchParams.get('setup')}`
          await chooseAccount(page, 'own-connect')
          await page.getByRole('button', { name: /^Sign in/ }).click()
          await page.getByRole('link', { name: 'Approve fixture access', exact: true }).waitFor()
          flows.push({ consent: page.url(), setup })
        }
      })
      await step(
        'Each callback returns to its own personal grant review and publishes only that account',
        async () => {
          for (const [index, flow] of flows.entries()) {
            await page.goto(flow.consent)
            const published = nextPublication(page)
            await page.getByRole('link', { name: 'Approve fixture access', exact: true }).click()
            // The sign-in answered the account question, so its callback adds
            // the provider from this flow's own setup.
            const form = await published
            expect(form.get('setup')).toBe(flow.setup.split('/').at(-1))
            expect(form.get('account')).toBe('caller')
            const grant = form.get('accountGrant')
            await waitForPublication(page)
            const provider = new URL(page.url()).pathname.slice(1)
            const inspect = (scope: ProviderAccountScope) =>
              api.accounts.getProviderAccount({
                parent: `workspaces/${workspace}`,
                provider,
                scope,
              })
            expect((await inspect(ProviderAccountScope.USER)).account!.id).toBe(grant)
            expect((await inspect(ProviderAccountScope.SHARED)).account).toBeUndefined()
            expect(upstreams[index].counts.tokens).toBe(1)
          }
        },
      )
    },
    { actor: actor.label, storageState: actor.storageState },
  )
})
