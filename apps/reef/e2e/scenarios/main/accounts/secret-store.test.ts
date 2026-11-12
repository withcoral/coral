import { createServer } from 'node:http'
import { expect, test, type TestContext } from 'vitest'
import { CredentialRoute_Kind } from '../../../generated/coral/management/v1/provider_pb.ts'
import { createActors, mcpToken } from '../../../support/actors.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'
import type { Page } from 'playwright'
import { managementClients } from '../../../support/management.ts'
import { listen, openApiDocument } from '../../../support/provider-fixtures.ts'
import { checkRadio, programRunner } from '../../../support/providers.ts'
import { ENV_SECRET_STORE } from '../../../support/secret-store-fixture.ts'

/**
 * A workspace with a Weather provider whose shared account holds a typed token, and the
 * upstream that records the bearer token each call sends.
 */
async function weatherProvider(context: TestContext) {
  const { owner, bob } = await createActors(context)
  const upstream = { sent: '' }
  const server = await listen(
    context,
    createServer((request, response) => {
      upstream.sent = request.headers.authorization ?? ''
      response.writeHead(200, { 'content-type': 'application/json' }).end('{"temperature":18}')
    }),
  )
  const api = managementClients(bob.accessToken)
  const workspace = (
    await api.workspaces.createWorkspace({
      displayName: `Secret store ${crypto.randomUUID()}`,
      requestId: crypto.randomUUID(),
    })
  ).name
  const setup = await api.setups.createProviderSetup({
    parent: workspace,
    input: {
      case: 'openapiDocument',
      value: openApiDocument(
        'Weather',
        { getForecast: { summary: 'Get forecast' } },
        { security: 'bearer', server: server.origin },
      ),
    },
  })
  const confirmed = await api.setups.confirmProviderSetupConfiguration({
    name: setup.name,
    expectedRevision: setup.configuration!.revision,
    configuration: setup.configuration!.suggested,
  })
  const authorized = await api.setups.authorizeProviderSetup({
    name: setup.name,
    configurationRevision: confirmed.configuration!.revision,
    credentialValues: { token: 'typed-value-canary' },
  })
  const provider = (
    await api.providers.publishProvider({
      parent: workspace,
      setup: setup.name,
      requestId: crypto.randomUUID(),
      namespace: 'weather',
      accountGrant: authorized.provisionalGrantId,
      credentialRoute: { kind: CredentialRoute_Kind.FIXED },
    })
  ).provider
  const workspaceDisplay = (await api.workspaces.getWorkspace({ name: workspace })).displayName
  return {
    owner,
    bob,
    upstream,
    id: workspace.split('/')[1],
    provider: provider.split('/').at(-1),
    workspaceDisplay,
  }
}

/** The organization owner grants `store` to the workspace from the account menu. */
async function grantStore(page: Page, store: string, workspaceDisplay: string) {
  await page.goto('/')
  await page.getByRole('button', { name: /^Open account menu for / }).click()
  await page.getByRole('menuitem', { name: 'Secret stores', exact: true }).click()
  await page.getByRole('button', { name: `Actions for ${store}` }).click()
  await page.getByRole('menuitem', { name: 'Grant to a workspace' }).click()
  const dialog = page.getByRole('dialog')
  await dialog.getByRole('button', { name: 'Workspace' }).click()
  await page.getByRole('menuitemradio', { name: workspaceDisplay }).click()
  await dialog.getByRole('button', { name: 'Grant' }).click()
  await page
    .getByRole('row', { name: new RegExp(store) })
    .getByText(workspaceDisplay)
    .waitFor()
}

// The `env` kind is Community, so this journey runs to the value upstream on any Edition.
test('Organization owner grants an env store, and a tool sends the variable it names', async (context) => {
  const { owner, bob, upstream, id, provider, workspaceDisplay } = await weatherProvider(context)

  await withBrowserSession(
    context,
    async ({ page, step }) => {
      await step('The organization owner grants the env store to the workspace', async () => {
        await grantStore(page, ENV_SECRET_STORE.name, workspaceDisplay)
        await page
          .getByRole('row', { name: new RegExp(ENV_SECRET_STORE.name) })
          .getByText('Available', { exact: true })
          .waitFor()
      })
    },
    { actor: owner.label, storageState: owner.storageState },
  )

  await withBrowserSession(
    context,
    async ({ page, target, step }) => {
      await step('A reference to a set variable, with no key, saves', async () => {
        await page.goto(`/workspaces/${id}/providers/${provider}/accounts`)
        await page.getByRole('button', { name: 'Reconnect' }).click()
        await page.getByRole('dialog').getByRole('button', { name: 'Reconnect' }).click()
        await checkRadio(page.getByRole('radio', { name: 'Use a secret store' }))
        await page.getByRole('textbox', { name: 'Variable name' }).fill(ENV_SECRET_STORE.variable)
        await page.getByRole('button', { name: 'Connect account' }).click()
        await page.getByText('Account connected.').waitFor()
        await page.goto(`/workspaces/${id}/providers/${provider}/accounts`)
        await page
          .getByText(
            `secret store ${ENV_SECRET_STORE.name}, variable name ${ENV_SECRET_STORE.variable}`,
          )
          .waitFor()
      })
      await step("A tool sends the variable's value upstream", async () => {
        const runner = programRunner(page, target, id, { accessToken: await mcpToken(page, id) })
        const run = (await runner.exec('return await coral.providers.weather.getForecast({});'))
          .structuredContent
        expect(run.state).toBe('succeeded')
        expect(upstream.sent).toBe(`Bearer ${ENV_SECRET_STORE.value}`)
      })
    },
    { actor: bob.label, storageState: bob.storageState },
  )
})
