import { expect, test } from 'vitest'
import { withOwnerSession } from '../../../../support/actors.ts'
import { oauthApiServer } from '../../../../support/provider-fixtures.ts'
import {
  acceptTools,
  chooseAccount,
  chooseFile,
  confirmConfiguration,
  createWorkspace,
  nextPublication,
  waitForPublication,
  signInAndShareAccount,
} from '../../../../support/providers.ts'

test('Denied OAuth consent settles the setup and allows a new sign-in', async (context) => {
  const upstream = await oauthApiServer(context, `denied-${crypto.randomUUID()}`)
  await withOwnerSession(context, async ({ page, step, actor }) => {
    await step('Deny access to the shared account', async () => {
      const workspace = await createWorkspace(
        actor.accessToken,
        `Denied consent ${crypto.randomUUID()}`,
      )
      await page.goto(`/workspaces/${workspace}/providers/new`)
      await chooseFile(page, 'oauth.json', upstream.document('Denied consent'))
      await page.getByRole('button', { name: 'Read', exact: true }).click()
      await confirmConfiguration(page)
      await acceptTools(page)
      await chooseAccount(page, 'shared')
      await signInAndShareAccount(page)
      await page.getByRole('link', { name: 'Deny fixture access', exact: true }).click()
    })
    await step('Retain the terminal result after reload without requesting a token', async () => {
      await page
        .getByText('You denied access. Sign in again to approve access.', {
          exact: false,
        })
        .waitFor()
      await page.reload()
      await page
        .getByText('You denied access. Sign in again to approve access.', {
          exact: false,
        })
        .waitFor()
      expect(upstream.counts.tokens).toBe(0)
    })
    await step('Start a new authorization and add the provider with it', async () => {
      await signInAndShareAccount(page)
      const published = nextPublication(page)
      await page.getByRole('link', { name: 'Approve fixture access', exact: true }).click()
      // The new sign-in answers the account question, so it adds the provider.
      expect((await published).get('account')).toBe('fixed')
      await waitForPublication(page)
      expect(upstream.counts.tokens).toBe(1)
    })
  })
})
