import { expect, test } from 'vitest'
import { mcpToken, withOwnerSession } from '../../../support/actors.ts'
import { managementClients } from '../../../support/management.ts'
import {
  addedProviders,
  connectSharedAccount,
  observeRun,
  programRunner,
  waitForPublication,
} from '../../../support/providers.ts'
import { openApiDocument } from '../../../support/provider-fixtures.ts'
import { OperationAccessState } from '../../../generated/coral/management/v1/policy_pb.ts'

/** A workspace where the owner's Programs can create providers without approval. */
async function programWorkspace(
  api: ReturnType<typeof managementClients>,
  step: (name: string, act: () => Promise<void>) => Promise<void>,
) {
  const workspace = await api.workspaces.createWorkspace({
    displayName: 'Program provider',
    requestId: crypto.randomUUID(),
  })
  await step("Let the owner's Programs create providers without approval", async () => {
    const builtin = (await api.providers.listProviders({ parent: workspace.name })).providers.find(
      (provider) => provider.callRoot === 'coral.admin',
    )!
    const catalog = await api.providers.listOperations({ parent: builtin.name })
    await api.policies.updateOperationPolicies({
      parent: builtin.name,
      policies: [
        { operation: catalog.operations[0].name, accessState: OperationAccessState.ENABLED },
      ],
    })
  })
  return workspace
}

test("A workspace owner's Program adds a provider that needs no sign-in, and Coral publishes it with no Reef step", async (context) => {
  await withOwnerSession(context, async ({ page, target, actor, step }) => {
    const api = managementClients(actor.accessToken)
    const workspace = await programWorkspace(api, step)
    const id = workspace.name.split('/')[1]
    const document = JSON.parse(
      openApiDocument('Weather', { getForecast: { summary: 'Get the forecast' } }).toString(),
    )
    // The source declares that it needs no credential.
    document.security = []
    const runner = programRunner(page, target, id, {
      accessToken: await mcpToken(page, id),
      taskIntent: 'Add the weather provider',
    })
    await step('The Program gets the provider that Coral published', async () => {
      const call = {
        namespace: 'weather',
        source: { kind: 'openapi_document', document: JSON.stringify(document) },
        credentialRoute: 'none',
        displayName: 'Forecasts',
      }
      const { update } = await observeRun(
        runner,
        await runner.exec(`return await coral.admin.createProvider(${JSON.stringify(call)});`),
      )
      expect(update.required_actions).toEqual([])
      expect(update.final_value).toMatchObject({
        value: {
          data: {
            namespace: 'weather',
            displayName: 'Forecasts',
          },
          metadata: { kind: 'builtin' },
        },
      })
    })
    await step('Show the provider in the workspace', async () => {
      const providers = addedProviders(
        (await api.providers.listProviders({ parent: workspace.name })).providers,
      )
      expect(providers.map((provider) => [provider.callRoot, provider.displayName])).toEqual([
        ['coral.providers.weather', 'Forecasts'],
      ])
      await page.goto(`/workspaces/${id}/providers`)
      await page
        .getByRole('navigation', { name: 'installed providers', exact: true })
        .getByRole('link', { name: 'Forecasts settings', exact: true })
        .waitFor()
    })
  })
})

test("A workspace owner's Program adds a provider with a shared account, and you sign in at its link", async (context) => {
  await withOwnerSession(context, async ({ page, target, actor, step }) => {
    const api = managementClients(actor.accessToken)
    const workspace = await programWorkspace(api, step)
    const id = workspace.name.split('/')[1]
    const document = openApiDocument(
      'Weather',
      { getForecast: { summary: 'Get the forecast' } },
      { security: 'bearer' },
    ).toString()
    const runner = programRunner(page, target, id, {
      accessToken: await mcpToken(page, id),
      taskIntent: 'Add the weather provider',
    })
    const call = {
      namespace: 'weather',
      source: { kind: 'openapi_document', document },
      credentialRoute: 'fixed',
    }
    let link = ''
    await step('The Program gets a link where you sign in', async () => {
      const { update } = await observeRun(
        runner,
        await runner.exec(`return await coral.admin.createProvider(${JSON.stringify(call)});`),
      )
      expect(update.required_actions).toEqual([])
      const { data } = (update.final_value as { value: { data: { status: string; url: string } } })
        .value
      expect(data.status).toBe('needs_sign_in')
      link = data.url
      const url = new URL(link)
      expect(url.pathname).toBe(`/workspaces/${id}/providers/new`)
      expect(url.searchParams.get('namespace')).toBe('weather')
      expect(url.searchParams.get('account')).toBe('fixed')
    })
    await step('Enter the shared credential and add the provider', async () => {
      await page.goto(link)
      // The stepper opens at the account, with nothing left to configure.
      await connectSharedAccount(page, 'weather-token')
      const add = page.getByRole('button', { name: 'Add provider', exact: true })
      await add.waitFor()
      await add.click()
      await waitForPublication(page)
    })
    await step('Show the provider in the workspace', async () => {
      const providers = addedProviders(
        (await api.providers.listProviders({ parent: workspace.name })).providers,
      )
      expect(providers.map((provider) => provider.callRoot)).toEqual(['coral.providers.weather'])
    })
  })
})
