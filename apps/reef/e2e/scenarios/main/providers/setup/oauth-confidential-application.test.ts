import { expect, test } from 'vitest'
import {
  ProviderAccountScope,
  ProviderAccountState,
} from '../../../../generated/coral/management/v1/account_pb.ts'
import { withOwnerSession } from '../../../../support/actors.ts'
import { managementClients } from '../../../../support/management.ts'
import { oauthApiServer } from '../../../../support/provider-fixtures.ts'
import {
  acceptTools,
  chooseAccount,
  chooseFile,
  collectDataResponses,
  confirmConfiguration,
  connectKeepingProgress,
  createWorkspace,
  expectArrivalToast,
  openProviderSettings,
  openYourAccount,
  waitForPublication,
  waitForYourAccount,
} from '../../../../support/providers.ts'

test('Workspace OAuth supports external-browser consent and application secret rotation', async (context) => {
  const secret = `application-${crypto.randomUUID()}`
  const replacement = `replacement-${crypto.randomUUID()}`
  const access = `account-${crypto.randomUUID()}`
  const knobs = {
    clientSecret: secret,
    nonRefreshable: true,
  }
  const upstream = await oauthApiServer(context, access, knobs)
  await withOwnerSession(context, async ({ page, step, actor }) => {
    const api = managementClients(actor.accessToken)
    const responses = await collectDataResponses(page)
    let providerPath = ''
    let accountPage = ''
    const inspect = () =>
      api.accounts.getProviderAccount({
        parent: new URL(providerPath).pathname.split('/').slice(1, 3).join('/'),
        provider: new URL(providerPath).pathname.slice(1),
        scope: ProviderAccountScope.USER,
      })
    await step(
      'Configure the application with a write-only secret and offline access',
      async () => {
        const workspace = await createWorkspace(
          actor.accessToken,
          `Confidential ${crypto.randomUUID()}`,
        )
        await page.goto(`/workspaces/${workspace}/providers/new`)
        await chooseFile(page, 'confidential.json', upstream.document('Confidential records'))
        await page.getByRole('button', { name: 'Read', exact: true }).click()
        await page.getByLabel('Display name', { exact: true }).fill('My confidential records')
        await page.getByLabel('Namespace', { exact: true }).fill('confidential')
        await page.getByRole('radio', { name: 'Secret in request body', exact: true }).check()
        await page.getByLabel('Client secret', { exact: true }).fill(secret)
        await page
          .getByLabel('Additional authorization parameters', { exact: true })
          .fill('access_type=offline')
        await page.getByLabel('Client ID', { exact: true }).fill('manual-client')
        await page
          .getByRole('radio', { name: 'Disable PKCE for this service', exact: true })
          .check()
        await page.getByLabel('Token URL', { exact: true }).fill('not-a-url')
        const rejected = page.waitForResponse(
          (response) =>
            response.request().isNavigationRequest() && response.request().method() === 'POST',
        )
        await page.getByRole('button', { name: 'Next', exact: true }).click()
        expect(await (await rejected).text()).not.toContain(secret)
        await page
          .getByText('Enter a valid token URL without credentials or OAuth request parameters.', {
            exact: true,
          })
          .waitFor()
        expect(await page.getByLabel('Token URL', { exact: true }).inputValue()).toBe('not-a-url')
        expect(await page.getByLabel('Display name', { exact: true }).inputValue()).toBe(
          'My confidential records',
        )
        expect(await page.getByLabel('Namespace', { exact: true }).inputValue()).toBe(
          'confidential',
        )
        expect(await page.getByLabel('Client ID', { exact: true }).inputValue()).toBe(
          'manual-client',
        )
        expect(
          await page
            .getByRole('radio', { name: 'Secret in request body', exact: true })
            .isChecked(),
        ).toBe(true)
        expect(
          await page
            .getByRole('radio', { name: 'Disable PKCE for this service', exact: true })
            .isChecked(),
        ).toBe(true)
        expect(
          await page
            .getByLabel('Additional authorization parameters', { exact: true })
            .inputValue(),
        ).toBe('access_type=offline')
        expect(await page.getByLabel('Client secret', { exact: true }).inputValue()).toBe('')
        await page.getByLabel('Token URL', { exact: true }).fill(`${upstream.origin}/token`)
        await page.getByRole('radio', { name: 'Use S256 PKCE', exact: true }).check()
        await page.getByLabel('Client secret', { exact: true }).fill(secret)
        await confirmConfiguration(page)
        await acceptTools(page)
        await chooseAccount(page, 'own-later')
        await page.getByRole('button', { name: 'Add provider', exact: true }).waitFor()
        await page.getByRole('button', { name: 'Add provider', exact: true }).click()
        await waitForPublication(page)
        providerPath = page.url().split('?')[0]
        expect((await inspect()).account).toBeUndefined()
      },
    )
    await step('Denied external-browser consent leaves the account unconnected', async () => {
      await openProviderSettings(page)
      await openYourAccount(page)
      accountPage = new URL(page.url()).pathname
      const authorization = await connectKeepingProgress(page)
      await page.getByRole('link', { name: 'Approve fixture access', exact: true }).waitFor()
      const consent = page.url()
      await page.goto(authorization)
      await page.getByRole('button', { name: 'Cancel', exact: true }).waitFor()
      expect(await page.getByText('Account connected.', { exact: false }).count()).toBe(0)
      expect((await inspect()).account?.state).toBe(ProviderAccountState.DISCONNECTED)
      expect((await inspect()).account?.connectedTime).toBeUndefined()
      const browser = await page.context().newPage()
      try {
        await browser.goto(consent)
        await browser.getByRole('link', { name: 'Deny fixture access', exact: true }).click()
        await page.getByText('You denied access.', { exact: false }).waitFor()
      } finally {
        await browser.close()
      }
      expect(upstream.counts.tokens).toBe(0)
      expect((await inspect()).account?.state).toBe(ProviderAccountState.DISCONNECTED)
      expect((await inspect()).account?.connectedTime).toBeUndefined()
      await page.goto(`${providerPath}/settings`)
    })
    await step('Connect an account without seeing or entering the application secret', async () => {
      await openYourAccount(page)
      expect(await page.getByLabel('Client secret', { exact: true }).count()).toBe(0)
      const authorization = await connectKeepingProgress(page)
      await page.getByRole('link', { name: 'Approve fixture access', exact: true }).waitFor()
      expect(new URL(page.url()).searchParams.get('access_type')).toBe('offline')
      expect(page.url()).not.toContain(secret)
      const consent = page.url()
      await page.goto(authorization)
      await page.getByRole('button', { name: 'Cancel', exact: true }).waitFor()
      const browser = await page.context().newPage()
      try {
        await browser.goto(consent)
        // The progress page returns to your account page once sign-in completes.
        await browser.getByRole('link', { name: 'Approve fixture access', exact: true }).click()
        await page.waitForURL((url) => url.pathname === accountPage)
      } finally {
        await browser.close()
      }
      // Without a refresh token, the Details card says to reconnect when access expires.
      const details = page.getByRole('complementary', { name: 'Details', exact: true })
      await details
        .getByRole('definition')
        .getByText('Manual. Reconnect when access expires.', { exact: true })
        .waitFor()
      await page.goto(accountPage)
      await waitForYourAccount(page)
      const account = (await inspect()).account!
      expect(account.state).toBe(ProviderAccountState.READY)
      expect(account.connectedTime).toBeDefined()
      expect(account.configuredByUser).not.toBe('')
      // Your own account page does not name you as its configurer.
      expect(await page.getByText('Configured by Coral user', { exact: false }).count()).toBe(0)
      await details
        .getByRole('definition')
        .getByText('Manual. Reconnect when access expires.', { exact: true })
        .waitFor()
      expect(upstream.counts.tokens).toBe(1)
    })
    await step('Rotate the application secret without replacing the account', async () => {
      const connected = (await inspect()).account!
      expect(connected.id).not.toBe('')
      await page.goto(`${providerPath}/update`)
      await page.getByText('OAuth application secret', { exact: true }).click()
      await page.getByLabel('Replacement client secret', { exact: true }).fill(replacement)
      await expectArrivalToast(
        page,
        { flag: 'secretRotated', title: 'Application secret updated' },
        () => page.getByRole('button', { name: 'Replace secret', exact: true }).click(),
      )
      knobs.clientSecret = replacement
      expect((await inspect()).account?.id).toBe(connected.id)
      expect((await inspect()).account?.connectedTime).toEqual(connected.connectedTime)
      expect(upstream.counts.tokens).toBe(1)
      const application = await api.providers.getProviderOAuthClient({
        name: new URL(providerPath).pathname.slice(1),
      })
      expect(JSON.stringify(application)).not.toContain(secret)
      expect(JSON.stringify(application)).not.toContain(replacement)

      await page.goto(accountPage)
      expect(await page.getByLabel('Client secret', { exact: true }).count()).toBe(0)
      await page.getByRole('button', { name: 'Reconnect', exact: true }).click()
      await page.getByRole('link', { name: 'Approve fixture access', exact: true }).click()
      await page.waitForURL((url) => url.pathname === accountPage)
      await waitForYourAccount(page)
      expect((await inspect()).account?.id).toBe(connected.id)
      expect((await inspect()).account?.connectedTime).not.toEqual(connected.connectedTime)
      expect(upstream.counts.tokens).toBe(2)
      expect(upstream.counts.calls).toBe(0)
      expect(await page.content()).not.toContain(access)
      for (const body of await responses()) {
        expect(body).not.toContain(secret)
        expect(body).not.toContain(replacement)
        expect(body).not.toContain(access)
        expect(body).not.toContain('disposable-code')
        expect(body).not.toContain('code_verifier')
      }
    })
  })
})
