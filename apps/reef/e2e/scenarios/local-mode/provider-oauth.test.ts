import { expect, test } from 'vitest'
import { withBrowserSession } from '../../support/browser-session.ts'
import { loopbackFixtures } from '../../support/config.ts'
import { oauthApiServer } from '../../support/provider-fixtures.ts'
import {
  acceptTools,
  chooseAccount,
  chooseFile,
  collectDataResponses,
  confirmConfiguration,
  createWorkspace,
  openProviderSettings,
  waitForPublication,
  openDialog,
} from '../../support/providers.ts'

// A local deployment has no Reef sign-in session to carry OAuth state back.
// Coral's own loopback listener, the one Desktop registers, completes the
// consent from its durable state; the person then returns to Reef.
test('Local provider OAuth completes on Coral’s loopback callback listener', async (context) => {
  const access = `local-access-${crypto.randomUUID()}`
  const upstream = await oauthApiServer(context, access, { address: loopbackFixtures })
  await withBrowserSession(context, async ({ page, target, step }) => {
    const responses = await collectDataResponses(page)
    const workspace = await createWorkspace(undefined, `Local OAuth ${crypto.randomUUID()}`)
    let stepper = ''
    await step('Send the shared account’s consent to Coral’s loopback listener', async () => {
      await page.goto(`/workspaces/${workspace}/providers/new`)
      await chooseFile(page, 'records.json', upstream.document('Local records'))
      await page.getByRole('button', { name: 'Read', exact: true }).click()
      await page.getByLabel('Namespace', { exact: true }).fill('localRecords')
      await confirmConfiguration(page)
      await acceptTools(page)
      await chooseAccount(page, 'shared')
      stepper = page.url()
      await openDialog(page, page.getByRole('button', { name: /^Sign in/ }))
      // Only you and your agents share the account in a local deployment.
      await page
        .getByRole('dialog', { name: 'Share this account with your agents?', exact: true })
        .getByRole('button', { name: 'Sign in and share the account', exact: true })
        .click()
      await page.getByRole('link', { name: 'Approve fixture access', exact: true }).waitFor()
      const callback = new URL(new URL(page.url()).searchParams.get('redirect_uri')!)
      expect(callback.hostname).toBe('127.0.0.1')
      expect(callback.pathname).toBe('/provider-oauth/callback')
      expect(callback.origin).not.toBe(new URL(target.baseURL).origin)
    })
    await step('Coral completes the sign-in without a Reef cookie', async () => {
      await page.getByRole('link', { name: 'Approve fixture access', exact: true }).click()
      await page
        .getByText('Return to Coral to check your connection. You can close this browser tab.')
        .waitFor()
      expect(upstream.counts.tokens).toBe(1)
    })
    await step('Return to Reef and add the provider with the shared account', async () => {
      await page.goto(stepper)
      await chooseAccount(page, 'shared')
      await page.getByRole('button', { name: 'Add provider', exact: true }).click()
      await waitForPublication(page)
      await openProviderSettings(page)
      await page
        .getByText('You and your agents use this shared account.', { exact: true })
        .waitFor()
      await page
        .getByRole('region', { name: 'One shared account', exact: true })
        .getByText('Connected', { exact: true })
        .waitFor()
      expect(upstream.counts.tokens).toBe(1)
      for (const body of await responses()) {
        expect(body).not.toContain(access)
        expect(body).not.toContain('disposable-code')
      }
    })
  })
})
