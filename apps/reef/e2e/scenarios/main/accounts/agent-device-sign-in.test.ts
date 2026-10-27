import { expect, test } from 'vitest'
import {
  ProviderAccountScope,
  ProviderAccountState,
} from '../../../generated/coral/management/v1/account_pb.ts'
import { withOwnerSession } from '../../../support/actors.ts'
import { managementClients } from '../../../support/management.ts'
import { oauthApiServer } from '../../../support/provider-fixtures.ts'
import {
  acceptTools,
  approveDeviceInNewTab,
  chooseAccount,
  chooseDeviceSignIn,
  chooseFile,
  collectDataResponses,
  createWorkspace,
  waitForPublication,
} from '../../../support/providers.ts'

test("Device sign-in in a new tab connects an Agent's account while its page stays open", async (context) => {
  const access = `agent-device-access-${crypto.randomUUID()}`
  const upstream = await oauthApiServer(context, access, { device: true })
  await withOwnerSession(context, async ({ page, step, actor }) => {
    const api = managementClients(actor.accessToken)
    const responses = await collectDataResponses(page)
    let workspace = ''
    let provider = ''
    let agent = ''
    const inspect = () =>
      api.accounts.getProviderAccount({
        parent: `workspaces/${workspace}`,
        provider,
        scope: ProviderAccountScope.AGENT,
        agent,
      })
    await step('Add a device-code provider whose agents connect their own accounts', async () => {
      workspace = await createWorkspace(actor.accessToken, `Agent device ${crypto.randomUUID()}`)
      await page.goto(`/workspaces/${workspace}/providers/new`)
      await chooseFile(page, 'device.json', upstream.document('Device records'))
      await page.getByRole('button', { name: 'Read', exact: true }).click()
      await page.getByLabel('Namespace', { exact: true }).fill('device')
      await chooseDeviceSignIn(page, upstream.origin)
      await page.getByRole('button', { name: 'Next', exact: true }).click()
      await acceptTools(page)
      await chooseAccount(page, 'own-later')
      await page.getByRole('button', { name: 'Add provider', exact: true }).click()
      await waitForPublication(page)
      provider = new URL(page.url()).pathname.slice(1)
      const created = await api.agents.createAgent({
        parent: `workspaces/${workspace}`,
        displayName: 'Device worker',
        requestId: crypto.randomUUID(),
      })
      agent = created.agent!.name
    })
    await step("Open the connect dialog from the agent's page and read the code", async () => {
      await page.goto(`/${agent}`)
      await page.getByRole('button', { name: 'Connect account for device', exact: true }).click()
      await page.getByRole('dialog').getByText('ABCD-EFGH', { exact: true }).waitFor()
      // Opening the dialog starts the connection; nothing is connected yet.
      expect((await inspect()).account?.state).not.toBe(ProviderAccountState.READY)
    })
    await step(
      'Approve in a new tab; the dialog closes and the row shows the account',
      async () => {
        await approveDeviceInNewTab(page)
        await page.getByText('Account connected.', { exact: true }).waitFor()
        await page.getByRole('dialog').waitFor({ state: 'detached' })
        await page.getByRole('link', { name: 'Manage account for device', exact: true }).waitFor()
        // The agent's page never left for the authorization page.
        expect(decodeURIComponent(new URL(page.url()).pathname)).toBe(`/${agent}`)
        expect((await inspect()).account!.state).toBe(ProviderAccountState.READY)
      },
    )
    for (const body of await responses()) {
      expect(body).not.toContain(access)
      expect(body).not.toContain('private-device-canary')
    }
  })
})
