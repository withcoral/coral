import { expect, test } from 'vitest'
import { withOwnerSession } from '../../../../support/actors.ts'
import { oauthApiServer } from '../../../../support/provider-fixtures.ts'
import {
  acceptTools,
  approveDeviceInNewTab,
  chooseAccount,
  chooseFile,
  chooseDeviceSignIn,
  collectDataResponses,
  createWorkspace,
  enableTool,
  nextPublication,
  programRunner,
  waitForPublication,
  signInAndShareAccount,
} from '../../../../support/providers.ts'

test('Device-code sign-in in a new tab adds the provider and supplies a shared account to a Tool', async (context) => {
  const access = `device-access-${crypto.randomUUID()}`
  const upstream = await oauthApiServer(context, access, { device: true })
  await withOwnerSession(context, async ({ page, step, target, actor }) => {
    const responses = await collectDataResponses(page)
    let workspace = ''
    await step(
      'Configure a public device-code application without a callback or PKCE',
      async () => {
        workspace = await createWorkspace(actor.accessToken, `Device ${crypto.randomUUID()}`)
        await page.goto(`/workspaces/${workspace}/providers/new`)
        await chooseFile(page, 'device.json', upstream.document('Device records'))
        await page.getByRole('button', { name: 'Read', exact: true }).click()
        await page.getByLabel('Namespace', { exact: true }).fill('device')
        await chooseDeviceSignIn(page, upstream.origin)
        expect(await page.getByLabel('Callback URL', { exact: true }).count()).toBe(0)
        expect(await page.getByLabel('PKCE', { exact: true }).count()).toBe(0)
        await page.getByRole('button', { name: 'Next', exact: true }).click()
        await acceptTools(page)
        await chooseAccount(page, 'shared')
        await signInAndShareAccount(page)
        await page.getByText('ABCD-EFGH', { exact: true }).waitFor()
      },
    )
    await step(
      'Approve in a new tab while the setup page adds the provider by itself',
      async () => {
        const published = nextPublication(page)
        await approveDeviceInNewTab(page)
        // The sign-in answered the account question, so Coral does not ask it again.
        const form = await published
        expect(form.get('account')).toBe('fixed')
        expect(form.get('accountGrant')).toBeTruthy()
        await waitForPublication(page)
      },
    )
    await step('Call a Tool with the resulting account', async () => {
      await enableTool(page, 'listRecords')
      const run = programRunner(page, target, workspace)
      const result = await run('return await coral.providers.device.listRecords({});')
      expect(result).toContain('Hello from the OAuth API')
      expect(upstream.counts.calls).toBe(1)
    })
    for (const body of await responses()) {
      expect(body).not.toContain(access)
      expect(body).not.toContain('private-device-canary')
    }
  })
})
