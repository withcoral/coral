import { createServer } from 'node:http'
import { expect, test } from 'vitest'
import { CredentialRoute_Kind } from '../../../generated/coral/management/v1/provider_pb.ts'
import { createActors, mcpToken } from '../../../support/actors.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'
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
  openProviderSettings,
  programRunner,
  saveConfirmedSettingsChange,
  waitForPublication,
  enableTool,
} from '../../../support/providers.ts'
import { SECRET_STORE } from '../../../support/secret-store-fixture.ts'

test('Organization owner grants a store, a workspace owner sets a reference, and a tool sends the stored value', async (context) => {
  const { owner, bob } = await createActors(context)
  let sent = ''
  const upstream = await listen(
    context,
    createServer((request, response) => {
      sent = request.headers.authorization ?? ''
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
        { security: 'bearer', server: upstream.origin },
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

  await withBrowserSession(
    context,
    async ({ page, step }) => {
      await step('The organization owner grants the store to the workspace', async () => {
        await page.goto('/')
        await page.getByRole('button', { name: /^Open account menu for / }).click()
        await page.getByRole('menuitem', { name: 'Secret stores', exact: true }).click()
        await page.getByRole('button', { name: `Actions for ${SECRET_STORE.name}` }).click()
        await page.getByRole('menuitem', { name: 'Grant to a workspace' }).click()
        const dialog = page.getByRole('dialog')
        await dialog.getByRole('button', { name: 'Workspace' }).click()
        await page.getByRole('menuitemradio', { name: workspaceDisplay }).click()
        await dialog.getByRole('button', { name: 'Grant' }).click()
        await page
          .getByRole('row', { name: new RegExp(SECRET_STORE.name) })
          .getByText(workspaceDisplay)
          .waitFor()
      })
    },
    { actor: owner.label, storageState: owner.storageState },
  )

  await withBrowserSession(
    context,
    async ({ page, target, step }) => {
      const id = workspace.split('/')[1]
      await step('The workspace owner sees the granted store', async () => {
        await page.goto(`/workspaces/${id}`)
        await page.getByRole('heading', { name: 'Secret stores' }).waitFor()
        await page.getByText('Available', { exact: true }).waitFor()
      })
      await step('The workspace owner sets a reference on the shared account', async () => {
        await page.goto(`/workspaces/${id}/providers/${provider.split('/').at(-1)}/accounts`)
        await page.getByRole('button', { name: 'Reconnect' }).click()
        await page.getByRole('dialog').getByRole('button', { name: 'Reconnect' }).click()
        await checkRadio(page.getByRole('radio', { name: 'Use a secret store' }))
        await page.getByRole('textbox', { name: 'Path' }).fill(SECRET_STORE.path)
        await page.getByRole('textbox', { name: 'Key' }).fill(SECRET_STORE.key)
        await page.getByRole('button', { name: 'Connect account' }).click()
        await page.getByText('Account connected.').waitFor()
      })
      await step('A tool sends the stored value upstream', async () => {
        const runner = programRunner(page, target, id, { accessToken: await mcpToken(page, id) })
        const run = (await runner.exec('return await coral.providers.weather.getForecast({});'))
          .structuredContent
        expect(run.state).toBe('succeeded')
        expect(sent).toBe(`Bearer ${SECRET_STORE.value}`)
        expect(JSON.stringify(run)).not.toContain(SECRET_STORE.value)
      })
      await step('The account page shows the reference, and Reconnect opens with it', async () => {
        await page.goto(`/workspaces/${id}/providers/${provider.split('/').at(-1)}/accounts`)
        await page.getByText(`path ${SECRET_STORE.path}`, { exact: false }).waitFor()
        await page.getByRole('button', { name: 'Reconnect' }).click()
        await page.getByRole('dialog').getByRole('button', { name: 'Reconnect' }).click()
        await expect
          .poll(() => page.getByRole('textbox', { name: 'Path' }).inputValue())
          .toBe(SECRET_STORE.path)
        await page.getByRole('button', { name: 'Connect account' }).click()
        await page.getByText('Account connected.').waitFor()
      })
      await step(
        'A change to a shared account can name a store, and a tool sends its value',
        async () => {
          const individual = await api.setups.createProviderSetup({
            parent: workspace,
            input: {
              case: 'openapiDocument',
              value: openApiDocument(
                'Tides',
                { getTides: { summary: 'Get tides' } },
                { security: 'bearer', server: upstream.origin },
              ),
            },
          })
          const reviewed = await api.setups.confirmProviderSetupConfiguration({
            name: individual.name,
            expectedRevision: individual.configuration!.revision,
            configuration: individual.configuration!.suggested,
          })
          const tides = (
            await api.providers.publishProvider({
              parent: workspace,
              setup: reviewed.name,
              requestId: crypto.randomUUID(),
              namespace: 'tides',
              credentialRoute: { kind: CredentialRoute_Kind.CALLER },
            })
          ).provider
          await page.goto(`/workspaces/${id}/providers/${tides.split('/').at(-1)}`)
          await openProviderSettings(page)
          await page.getByRole('button', { name: 'Change account setting', exact: true }).click()
          await page.getByRole('radio', { name: 'Connect an account', exact: true }).check()
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
          await checkRadio(page.getByRole('radio', { name: 'Use this account', exact: true }))
          await saveConfirmedSettingsChange(page)
          sent = ''
          const runner = programRunner(page, target, id, { accessToken: await mcpToken(page, id) })
          const run = (await runner.exec('return await coral.providers.tides.getTides({});'))
            .structuredContent
          expect(run.state).toBe('succeeded')
          expect(sent).toBe(`Bearer ${SECRET_STORE.value}`)
        },
      )
      await step('A provider added with a reference from the start calls a tool', async () => {
        await page.goto(`/workspaces/${id}/providers/new`)
        await openCustomSource(page)
        await chooseFile(
          page,
          'climate.json',
          openApiDocument(
            'Climate',
            { getClimate: { summary: 'Get climate' } },
            { security: 'bearer', server: upstream.origin },
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
        await page.getByRole('button', { name: 'Add provider', exact: true }).click()
        await waitForPublication(page)
        await enableTool(page, 'getClimate')
        sent = ''
        const runner = programRunner(page, target, id, { accessToken: await mcpToken(page, id) })
        const run = (await runner.exec('return await coral.providers.climate.getClimate({});'))
          .structuredContent
        expect(run.state).toBe('succeeded')
        expect(sent).toBe(`Bearer ${SECRET_STORE.value}`)
      })
    },
    { actor: bob.label, storageState: bob.storageState },
  )
})
