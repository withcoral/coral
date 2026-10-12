import { expect, test } from 'vitest'
import { withOwnerSession } from '../../../../support/actors.ts'
import { oauthApiServer } from '../../../../support/provider-fixtures.ts'
import {
  acceptOperations,
  accountQuestion,
  chooseAccount,
  chooseFile,
  collectDataResponses,
  confirmConfiguration,
  createWorkspace,
  nextPublication,
  openOperation,
  openProviderSettings,
  programRunner,
  waitForProviderStatus,
  waitForPublication,
  openDialog,
} from '../../../../support/providers.ts'

const CALLBACK_LABEL = 'Callback URL'

// An OpenAPI document that signs in with OAuth carries nothing to bootstrap a
// client from, so the shared account signs in through the owner's application.
// The one home of the consent URL, resume and forged-callback checks for
// provider OAuth; oauth-manual and accounts/oauth rely on it.
test('OpenAPI OAuth with one shared account: consent, resume, forged callbacks, and a complete exchange', async (context) => {
  const access = `fixture-access-${crypto.randomUUID()}`
  const upstream = await oauthApiServer(context, access)
  await withOwnerSession(context, async ({ page, target, step, actor }) => {
    const responses = await collectDataResponses(page)
    let workspace = ''
    let stepperUrl = ''
    let callback: URL | undefined
    await step('Whose account comes before any sign-in', async () => {
      workspace = await createWorkspace(actor.accessToken, `OpenAPI OAuth ${crypto.randomUUID()}`)
      await page.goto(`/workspaces/${workspace}/providers/new`)
      await chooseFile(page, 'records.json', upstream.document('Records'))
      await page.getByRole('button', { name: 'Read', exact: true }).click()
      await page.getByLabel('Namespace', { exact: true }).fill('records')
      await confirmConfiguration(page)
      await acceptOperations(page)
      await page.waitForURL((url) => url.searchParams.has('setup'))
      await accountQuestion(page).waitFor()
      stepperUrl = page.url()
      await chooseAccount(page, 'shared')
      // Only an explicit application applies to an OpenAPI document.
      expect(await page.getByRole('radio', { name: 'Automatic', exact: true }).count()).toBe(0)
      await page.getByText('manual-client', { exact: true }).waitFor()
      expect(await page.getByLabel(CALLBACK_LABEL, { exact: true }).count()).toBe(0)
      expect(
        await page.getByRole('button', { name: 'Copy callback URL', exact: true }).count(),
      ).toBe(0)
      expect(await page.getByLabel('Client ID', { exact: true }).count()).toBe(0)
      await openDialog(page, page.getByRole('button', { name: /^Sign in with / }))
      const confirmation = page.getByRole('dialog', {
        name: 'Share this account with the workspace?',
        exact: true,
      })
      await confirmation.waitFor()
      expect(upstream.counts.consents).toBe(0)
      await confirmation.getByRole('button', { name: 'Cancel', exact: true }).click()
      await confirmation.waitFor({ state: 'hidden' })
      expect(upstream.counts.consents).toBe(0)
      await page.getByRole('button', { name: /^Sign in with / }).click()
      await confirmation
        .getByRole('button', {
          name: 'Sign in and share the account',
          exact: true,
        })
        .click()
      await page.getByRole('link', { name: 'Approve fixture access', exact: true }).waitFor()
    })
    const consentUrl = () => new URL(page.url())
    await step('The consent URL carries scope, S256, and Reef’s callback', async () => {
      const consent = consentUrl()
      expect(consent.searchParams.get('client_id')).toBe('manual-client')
      expect(consent.searchParams.get('scope')).toBe('read write')
      expect(consent.searchParams.get('code_challenge_method')).toBe('S256')
      expect(consent.searchParams.get('resource')).toBeNull()
      expect(new URL(consent.searchParams.get('redirect_uri')!).origin).toBe(
        new URL(stepperUrl).origin,
      )
      expect(new URL(consent.searchParams.get('redirect_uri')!).pathname).toBe(
        '/provider-setup/oauth/callback',
      )
      callback = new URL(consent.searchParams.get('redirect_uri')!)
      callback.searchParams.set('state', consent.searchParams.get('state')!)
      callback.searchParams.set('code', 'disposable-code')
    })
    await step('Reopening the stepper resumes the same sign-in', async () => {
      const first = page.url()
      await page.goto(stepperUrl)
      await page.getByRole('button', { name: 'Continue sign-in', exact: true }).click()
      await page.getByRole('link', { name: 'Approve fixture access', exact: true }).waitFor()
      expect(page.url()).toBe(first)
      expect(upstream.counts.consents).toBe(2)
    })
    await step('A callback with the wrong state answers 400 and never exchanges', async () => {
      const wrong = new URL(callback!)
      wrong.searchParams.set('state', 'wrong-state')
      expect((await page.request.get(wrong.toString(), { maxRedirects: 0 })).status()).toBe(400)
      expect(upstream.counts.tokens).toBe(0)
    })
    await step('Approving exchanges the code exactly once', async () => {
      const published = nextPublication(page)
      await page.getByRole('link', { name: 'Approve fixture access', exact: true }).click()
      // The sign-in answered the account question, so the callback adds the
      // provider with the shared account rather than ask again.
      expect((await published).get('account')).toBe('fixed')
      await waitForPublication(page)
      expect(upstream.counts.tokens).toBe(1)
      const wrongCode = new URL(callback!)
      wrongCode.searchParams.set('code', 'different-code')
      expect((await page.request.get(wrongCode.toString(), { maxRedirects: 0 })).status()).toBe(400)
      expect(upstream.counts.tokens).toBe(1)
    })
    await step('A complete exchange adds it', async () => {
      await waitForProviderStatus(page, 'Records', 'Ready', { available: 1, total: 1 })
      // The operation's full name carries the chosen namespace.
      const drawer = await openOperation(page, 'listRecords')
      await drawer.getByText('coral.providers.records.listRecords', { exact: true }).waitFor()
      await page.keyboard.press('Escape')
      await drawer.waitFor({ state: 'hidden' })
      await openProviderSettings(page)
      await page.getByText('All callers use one shared account.', { exact: true }).waitFor()
      await page
        .getByRole('region', { name: 'Shared account', exact: true })
        .getByText('Connected', { exact: true })
        .waitFor()
    })
    await step('A Program run resolves the shared account', async () => {
      // Every OpenAPI Operation needs approval by default, so the run reaches
      // the approval gate with the shared account resolved; nothing calls upstream.
      const run = programRunner(page, target, workspace, { waitMs: 1000 })
      const result = await run('return await coral.providers.records.listRecords({});')
      expect(result).toContain('"state":"running"')
      expect(result).toContain('invocation_approval')
      expect(result).not.toContain('invocation.started')
      expect(result).not.toContain('invocation.ended')
      expect(result).not.toContain('account_required')
      expect(result).not.toContain(access)
      expect(upstream.counts.calls).toBe(0)
      expect(upstream.counts.tokens).toBe(1)
    })
    for (const body of await responses()) {
      expect(body).not.toContain(access)
      expect(body).not.toContain('disposable-code')
      expect(body).not.toContain('code_verifier')
    }
    expect((await page.content()).includes(access)).toBe(false)
  })
})

test('OpenAPI OAuth with each person’s own account signs nobody in', async (context) => {
  const upstream = await oauthApiServer(context, `fixture-access-${crypto.randomUUID()}`)
  await withOwnerSession(context, async ({ page, step, actor }) => {
    let workspace = ''
    const workspaceName = `Own account ${crypto.randomUUID()}`
    await step('Added in a Workspace without a sign-in', async () => {
      workspace = await createWorkspace(actor.accessToken, workspaceName)
      await page.goto(`/workspaces/${workspace}/providers/new`)
      await chooseFile(page, 'records.json', upstream.document('Records'))
      await page.getByRole('button', { name: 'Read', exact: true }).click()
      await page.getByLabel('Namespace', { exact: true }).fill('records')
      await confirmConfiguration(page)
      await acceptOperations(page)
      await accountQuestion(page).waitFor()
      await chooseAccount(page, 'own-later')
      expect(await page.getByRole('button', { name: /^Sign in/ }).count()).toBe(0)
      expect(await page.getByLabel('Client ID', { exact: true }).count()).toBe(0)
      expect(await page.getByLabel(CALLBACK_LABEL, { exact: true }).count()).toBe(0)
      await page
        .getByText(
          'Everyone, including you, connects an account when they first use the provider.',
          {
            exact: true,
          },
        )
        .waitFor()
      await page.getByRole('button', { name: 'Add provider', exact: true }).click()
      await waitForPublication(page)
      // Coral reports no provider blocker, but you cannot call it yet: the page
      // asks you to connect, and settings show your account is not connected.
      await waitForProviderStatus(page, 'Records', 'Connect your account')
      await openProviderSettings(page)
      await page.getByText('Each caller uses their own account.', { exact: true }).waitFor()
      await page
        .getByRole('region', { name: 'Your account', exact: true })
        .getByText('Not connected', { exact: true })
        .waitFor()
      expect(upstream.counts.consents).toBe(0)
    })
  })
})
