import { expect, test } from 'vitest'

import { loopbackFixtures } from '../../support/config.ts'
import {
  appWindows,
  desktopProfile,
  markSamePage,
  openInPersonsBrowser,
  recordSystemBrowser,
  withDesktopApp,
} from '../../support/desktop-app.ts'
import { oauthApiServer } from '../../support/provider-fixtures.ts'
import {
  acceptTools,
  chooseAccount,
  chooseDeviceSignIn,
  chooseFile,
  createWorkspaceViaUi,
  waitForPublication,
  signInAndShareAccount,
} from '../../support/providers.ts'

// Reef opens a device sign-in in a new tab, as in a browser. Only the host's
// part is Desktop's: the tab's POST must still reach the consent hand-off.
test('Desktop opens a device sign-in in the system browser and keeps the code on screen', async (context) => {
  const access = `desktop-device-${crypto.randomUUID()}`
  const upstream = await oauthApiServer(context, access, {
    device: true,
    address: loopbackFixtures,
  })
  const profile = await desktopProfile(context)
  await withDesktopApp(context, profile, async ({ page, app, reef, step }) => {
    const opened = await recordSystemBrowser(app)
    await step('Start a device sign-in for a shared account', async () => {
      const workspace = await createWorkspaceViaUi(page, 'Desktop device sign-in')
      await page.goto(`/workspaces/${workspace}/providers/new`)
      await chooseFile(page, 'device.json', upstream.document('Desktop device records'))
      await page.getByRole('button', { name: 'Read', exact: true }).click()
      await page.getByLabel('Namespace', { exact: true }).fill('device')
      await chooseDeviceSignIn(page, upstream.origin)
      await page.getByRole('button', { name: 'Next', exact: true }).click()
      await acceptTools(page)
      await chooseAccount(page, 'shared')
      await signInAndShareAccount(page)
      await page.getByText('ABCD-EFGH', { exact: true }).waitFor()
    })
    let verification = ''
    await step(
      'Open sign-in page sends the verification page to the system browser and leaves one window',
      async () => {
        const samePage = await markSamePage(page)
        await page
          .getByRole('dialog')
          .getByRole('button', { name: 'Open sign-in page', exact: true })
          .click()
        await expect.poll(opened).toHaveLength(1)
        verification = (await opened())[0]
        expect(new URL(verification).origin).toBe(upstream.origin)
        // The hidden window that submitted the form is gone, and the window
        // still shows the code on the same page.
        await expect.poll(() => appWindows(app)).toEqual({ total: 1, visible: 1 })
        expect(new URL(page.url()).origin).toBe(reef)
        expect(await samePage()).toBe(true)
        await page.getByText('ABCD-EFGH', { exact: true }).waitFor()
      },
    )
    // Desktop canceled the hidden window's answer. The sign-in it started must
    // still finish on the page that stayed.
    await step('Approve in the system browser, and the same page adds the provider', async () => {
      const tab = await openInPersonsBrowser(context, verification)
      await tab.getByRole('link', { name: 'Approve device access', exact: true }).click()
      await tab.getByRole('heading', { name: 'Device approved', exact: true }).waitFor()
      await waitForPublication(page)
    })
  })
})
