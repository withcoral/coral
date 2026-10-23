import { expect, test } from 'vitest'

import { loopbackFixtures } from '../../support/config.ts'
import {
  desktopProfile,
  markSamePage,
  openInPersonsBrowser,
  recordSystemBrowser,
  withDesktopApp,
} from '../../support/desktop-app.ts'
import { oauthApiServer } from '../../support/provider-fixtures.ts'
import {
  addProviderFromFile,
  createWorkspaceViaUi,
  openProviderSettings,
  openYourAccount,
  waitForYourAccount,
} from '../../support/providers.ts'

// Only the host's part of Provider consent. Coral's loopback callback itself is
// local-mode/provider-oauth's claim.
test('Desktop opens provider consent in the system browser and keeps sign-in progress in its window', async (context) => {
  const access = `desktop-access-${crypto.randomUUID()}`
  const upstream = await oauthApiServer(context, access, { address: loopbackFixtures })
  const profile = await desktopProfile(context)
  await withDesktopApp(context, profile, async ({ page, app, reef, step }) => {
    const opened = await recordSystemBrowser(app)
    await step('Add a provider whose callers connect their own accounts', async () => {
      const workspace = await createWorkspaceViaUi(page, 'Desktop consent')
      await addProviderFromFile(page, workspace, upstream.document('Desktop records'), 'records', {
        account: 'own-later',
      })
      await openProviderSettings(page)
      await openYourAccount(page)
    })
    let consent = ''
    await step(
      'Connect sends consent to the system browser and keeps the window on Coral',
      async () => {
        await page.getByRole('button', { name: 'Connect', exact: true }).click()
        await page.getByRole('button', { name: 'Cancel', exact: true }).waitFor()
        expect(new URL(page.url()).origin).toBe(reef)
        await expect.poll(opened).toHaveLength(1)
        consent = (await opened())[0]
        expect(new URL(consent).origin).toBe(upstream.origin)
        // The window never loaded the consent page.
        expect(upstream.counts.consents).toBe(0)
      },
    )
    await step(
      'Approve in the system browser, and the window shows the account connected',
      async () => {
        const samePage = await markSamePage(page)
        const tab = await openInPersonsBrowser(context, consent)
        await tab.getByRole('link', { name: 'Approve fixture access', exact: true }).click()
        await tab
          .getByText('Return to Coral to check your connection. You can close this browser tab.')
          .waitFor()
        // Reef's activity stream returns the open page to the account; nothing reloads it.
        await waitForYourAccount(page)
        expect(await samePage()).toBe(true)
        expect(upstream.counts.tokens).toBe(1)
      },
    )
  })
})
