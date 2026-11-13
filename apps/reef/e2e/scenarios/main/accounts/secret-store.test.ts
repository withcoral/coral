import { createServer } from 'node:http'
import { expect, test, type TestContext } from 'vitest'
import { CredentialRoute_Kind } from '../../../generated/coral/management/v1/provider_pb.ts'
import { createActors, mcpToken } from '../../../support/actors.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'
import type { Page } from 'playwright'
import { managementClients } from '../../../support/management.ts'
import { listen, openApiDocument } from '../../../support/provider-fixtures.ts'
import {
  acceptTools,
  checkRadio,
  chooseAccount,
  chooseFile,
  confirmConfiguration,
  openCustomSource,
  openDialog,
  programRunner,
} from '../../../support/providers.ts'
import { ENV_SECRET_STORE, SECRET_STORE } from '../../../support/secret-store-fixture.ts'

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
    origin: server.origin,
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

// Reel runs the released binary, and nothing sets its Edition yet, so it runs as the
// Community Edition and the KV v2 store refuses every read. #3034 brings back the full
// journey, from a reference to the stored value upstream, once a test licence sets it.
test('Organization owner grants a KV v2 store, and a reference to it needs the Enterprise Edition', async (context) => {
  const { owner, bob, upstream, origin, id, provider, workspaceDisplay } =
    await weatherProvider(context)

  await withBrowserSession(
    context,
    async ({ page, step }) => {
      await step('The organization owner grants the store to the workspace', async () => {
        await grantStore(page, SECRET_STORE.name, workspaceDisplay)
        await page
          .getByRole('row', { name: new RegExp(SECRET_STORE.name) })
          .getByText('Needs Enterprise', { exact: true })
          .waitFor()
      })
    },
    { actor: owner.label, storageState: owner.storageState },
  )

  await withBrowserSession(
    context,
    async ({ page, target, step }) => {
      await step('The workspace owner sees the granted store needs Enterprise', async () => {
        await page.goto(`/workspaces/${id}`)
        await page.getByRole('heading', { name: 'Secret stores' }).waitFor()
        await page.getByText('Needs Enterprise', { exact: true }).waitFor()
      })
      await step('Coral refuses a reference to the store, and keeps the account', async () => {
        await page.goto(`/workspaces/${id}/providers/${provider}/accounts`)
        await page.getByRole('button', { name: 'Reconnect' }).click()
        await page.getByRole('dialog').getByRole('button', { name: 'Reconnect' }).click()
        await checkRadio(page.getByRole('radio', { name: 'Use a secret store' }))
        await page.getByRole('textbox', { name: 'Path' }).fill(SECRET_STORE.path)
        await page.getByRole('textbox', { name: 'Key' }).fill(SECRET_STORE.key)
        await page.getByRole('button', { name: 'Connect account' }).click()
        await page.getByText('This secret store needs the Enterprise Edition.').waitFor()
      })
      await step('A tool still sends the typed value upstream', async () => {
        const runner = programRunner(page, target, id, { accessToken: await mcpToken(page, id) })
        const run = (await runner.exec('return await coral.providers.weather.getForecast({});'))
          .structuredContent
        expect(run.state).toBe('succeeded')
        expect(upstream.sent).toBe('Bearer typed-value-canary')
      })
      await step('Adding a provider with a reference asks for the Enterprise Edition', async () => {
        await page.goto(`/workspaces/${id}/providers/new`)
        await openCustomSource(page)
        await chooseFile(
          page,
          'climate.json',
          openApiDocument(
            'Climate',
            { getClimate: { summary: 'Get climate' } },
            { security: 'bearer', server: origin },
          ),
        )
        await page.getByRole('button', { name: 'Read', exact: true }).click()
        await page.getByLabel('Display name', { exact: true }).waitFor()
        await page.getByLabel('Namespace', { exact: true }).fill('climate')
        await confirmConfiguration(page)
        await acceptTools(page)
        await chooseAccount(page, 'shared')
        const dialog = await openDialog(
          page,
          page.getByRole('button', { name: 'Connect the shared account', exact: true }),
        )
        await checkRadio(dialog.getByRole('radio', { name: 'Use a secret store' }))
        await dialog.getByRole('textbox', { name: 'Path' }).fill(SECRET_STORE.path)
        await dialog.getByRole('textbox', { name: 'Key' }).fill(SECRET_STORE.key)
        await dialog
          .getByRole('button', { name: 'Save and share the account', exact: true })
          .click()
        await page.getByText('This secret store needs the Enterprise Edition.').waitFor()
      })
    },
    { actor: bob.label, storageState: bob.storageState },
  )
})

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
