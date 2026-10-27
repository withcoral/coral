import { expect, test } from 'vitest'
import { withOwnerSession } from '../../../../support/actors.ts'
import { oauthMcpServer } from '../../../../support/provider-fixtures.ts'
import {
  acceptTools,
  approveDeviceInNewTab,
  chooseAccount,
  chooseSignIn,
  createWorkspace,
  openCustomSource,
  programRunner,
  replaceScopes,
  signInToReadTools,
  waitForPublication,
} from '../../../../support/providers.ts'

test('MCP device discovery, publication and call from manual settings', async (context) => {
  const upstream = await oauthMcpServer(context, `device-access-${crypto.randomUUID()}`, {
    device: true,
    expectedDeviceScope: 'read',
    requiredServiceHeaders: { 'X-Service-Version': '2022-11-28' },
  })
  await withOwnerSession(context, async ({ page, step, target, actor }) => {
    let workspace = ''
    let returnTo = ''
    await step('Select discovered device authorization', async () => {
      workspace = await createWorkspace(actor.accessToken, `MCP device ${crypto.randomUUID()}`)
      await page.goto(`/workspaces/${workspace}/providers/new`)
      await openCustomSource(page)
      await page.getByLabel('MCP server URL', { exact: true }).fill(upstream.mcpUrl)
      await page.getByRole('button', { name: 'Connect', exact: true }).click()
      await page.waitForURL((url) => url.searchParams.has('setup'))
      await chooseSignIn(page, 'OAuth device code')
      await page.getByLabel('Client ID', { exact: true }).fill('manual-client')
      await replaceScopes(page, ['read'])
      // The server wants the header only once it holds a token, so it is set here.
      await page.getByRole('button', { name: 'Add request header', exact: true }).click()
      await page.getByLabel('Request header 1', { exact: true }).fill('x-service-version')
      await page.getByLabel('Request header value 1', { exact: true }).fill('2022-11-28')
      await page.getByLabel('Namespace', { exact: true }).fill('device')
      expect(
        await page
          .getByRole('textbox', { name: 'Device authorization URL', exact: true })
          .inputValue(),
      ).toBe(`${upstream.origin}/device`)
      expect(
        await page
          .getByRole('textbox', { name: 'Device authorization URL', exact: true })
          .isDisabled(),
      ).toBe(true)
      expect(await page.getByLabel('PKCE', { exact: true }).count()).toBe(0)
      await page.getByRole('button', { name: 'Next', exact: true }).click()
      await signInToReadTools(page)
      await page.getByText('ABCD-EFGH', { exact: true }).waitFor()
      returnTo = page.url()
    })
    await step('Approve the device and publish the discovered tools', async () => {
      await approveDeviceInNewTab(page)
      expect(page.url()).toBe(returnTo)
      await acceptTools(page)
      await chooseAccount(page, 'shared')
      await page.getByRole('button', { name: 'Add provider', exact: true }).click()
      await waitForPublication(page)
    })
    await step('Call through the published MCP provider', async () => {
      const result = await programRunner(
        page,
        target,
        workspace,
      )('return await coral.providers.device.lookup({});')
      expect(result).toContain('Hello from the OAuth fixture')
      expect(upstream.counts.calls).toBe(1)
      expect(upstream.counts.registrations).toBe(0)
    })
  })
})
