import { expect, test } from 'vitest'
import {
  ProviderAccountScope,
  ProviderAccountState,
} from '../../../generated/coral/management/v1/account_pb.ts'
import { CredentialRoute_Kind } from '../../../generated/coral/management/v1/provider_pb.ts'
import { withOwnerSession } from '../../../support/actors.ts'
import { managementClients } from '../../../support/management.ts'
import { openApiDocument } from '../../../support/provider-fixtures.ts'
import { collectDataResponses } from '../../../support/providers.ts'

test("A secret entered in the agent's connect dialog reaches Coral and returns to the agent's page", async (context) => {
  const secret = `agent-dialog-secret-${crypto.randomUUID()}`
  await withOwnerSession(context, async ({ page, step, actor }) => {
    const api = managementClients(actor.accessToken)
    const responses = await collectDataResponses(page)
    // A router submission posts to a `.data` URL; a document submission does not.
    const routedBodies: string[] = []
    page.on('request', (request) => {
      if (new URL(request.url()).pathname.endsWith('.data'))
        routedBodies.push(request.postData() ?? '')
    })
    let workspace = ''
    let provider = ''
    let agent = ''
    const inspect = () =>
      api.accounts.getProviderAccount({
        parent: workspace,
        provider,
        scope: ProviderAccountScope.AGENT,
        agent,
      })
    await step('Add a token provider whose agents connect their own accounts', async () => {
      workspace = (
        await api.workspaces.createWorkspace({
          displayName: `Agent secret ${crypto.randomUUID()}`,
          requestId: crypto.randomUUID(),
        })
      ).name
      const setup = await api.setups.createProviderSetup({
        parent: workspace,
        input: {
          case: 'openapiDocument',
          value: openApiDocument(
            'Vault',
            { listEntries: { summary: 'List entries' } },
            { security: 'bearer' },
          ),
        },
      })
      await api.setups.confirmProviderSetupConfiguration({
        name: setup.name,
        expectedRevision: setup.configuration!.revision,
        configuration: setup.configuration!.suggested,
      })
      provider = (
        await api.providers.publishProvider({
          parent: workspace,
          setup: setup.name,
          requestId: crypto.randomUUID(),
          namespace: 'vault',
          credentialRoute: { kind: CredentialRoute_Kind.CALLER },
        })
      ).provider
      agent = (
        await api.agents.createAgent({
          parent: workspace,
          displayName: 'Secret worker',
          requestId: crypto.randomUUID(),
        })
      ).agent!.name
    })
    await step("Enter the token in the agent page's connect dialog", async () => {
      await page.goto(`/${agent}`)
      await page.getByRole('button', { name: 'Connect account for vault', exact: true }).click()
      const dialog = page.getByRole('dialog')
      await dialog.getByLabel('Token', { exact: true }).fill(secret)
      // Opening the dialog starts the connection; nothing is connected yet.
      expect((await inspect()).account?.state).not.toBe(ProviderAccountState.READY)
      await dialog.getByRole('button', { name: 'Connect account', exact: true }).click()
    })
    await step("Land back on the agent's page with the agent's account ready", async () => {
      await page.getByRole('link', { name: 'Manage account for vault', exact: true }).waitFor()
      expect(decodeURIComponent(new URL(page.url()).pathname)).toBe(`/${agent}`)
      const account = (await inspect()).account!
      expect(account.state).toBe(ProviderAccountState.READY)
      expect(account.configuredByUser).toBe(actor.userId)
      // The secret went to Coral as a document submission, never through the router.
      expect(await page.content()).not.toContain(secret)
      // The dialog's start and step reads are router data, so the checks are not vacuous.
      expect(routedBodies.length).toBeGreaterThan(0)
      for (const body of routedBodies) expect(body).not.toContain(secret)
      for (const body of await responses()) expect(body).not.toContain(secret)
    })
  })
})
