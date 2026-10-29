import { expect, test } from 'vitest'
import { createActors, mcpToken, withOwnerSession } from '../../../support/actors.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'
import { shownArguments } from '../../../support/arguments.ts'
import { managementClients } from '../../../support/management.ts'
import {
  acceptTools,
  addedProviders,
  collectDataResponses,
  connectSharedAccount,
  observeRun,
  programRunner,
  programWorkspace,
  signInAndShareAccount,
  waitForPublication,
} from '../../../support/providers.ts'
import { oauthApiServer, openApiDocument } from '../../../support/provider-fixtures.ts'
import {
  ProviderAccountScope,
  ProviderAccountState,
} from '../../../generated/coral/management/v1/account_pb.ts'
import { WorkspaceRole } from '../../../generated/coral/management/v1/workspace_pb.ts'
import { approvedProgramProvider, deniedProgramProvider } from './program-approval.ts'

test("A workspace owner's Program adds a provider that needs no sign-in, and Coral publishes it with no Reef step", async (context) => {
  await withOwnerSession(context, async ({ page, target, actor, step }) => {
    const api = managementClients(actor.accessToken)
    const workspace = await programWorkspace(api, step, ['addProvider'])
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
        await runner.exec(`return await coral.workspace.addProvider(${JSON.stringify(call)});`),
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
    const workspace = await programWorkspace(api, step, ['addProvider'])
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
        await runner.exec(`return await coral.workspace.addProvider(${JSON.stringify(call)});`),
      )
      expect(update.required_actions).toEqual([])
      const { data } = (update.final_value as { value: { data: { status: string; url: string } } })
        .value
      expect(data.status).toBe('needs_sign_in')
      link = data.url
      const url = new URL(link)
      expect(url.pathname).toBe(`/workspaces/${id}/providers/new`)
      // The Setup holds the namespace and route the Program asked for.
      expect([...url.searchParams.keys()]).toEqual(['setup'])
    })
    await step('Review the tools, connect the account, and add the provider', async () => {
      await page.goto(link)
      // The stepper opens at the tools, with nothing left to configure.
      await acceptTools(page)
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

test("A workspace owner's Program prepares a confidential OAuth application, and you enter its secret only in Reef", async (context) => {
  const secret = `application-${crypto.randomUUID()}`
  // The token endpoint refuses an exchange that does not carry this secret.
  const upstream = await oauthApiServer(context, `account-${crypto.randomUUID()}`, {
    clientSecret: secret,
  })
  await withOwnerSession(context, async ({ page, target, actor, step }) => {
    const api = managementClients(actor.accessToken)
    const responses = await collectDataResponses(page)
    const workspace = await programWorkspace(api, step, ['addProvider'])
    const id = workspace.name.split('/')[1]
    const runner = programRunner(page, target, id, {
      accessToken: await mcpToken(page, id),
      taskIntent: 'Add the records provider',
    })
    // The Program names the application but has no secret to give.
    const call = {
      namespace: 'records',
      source: { kind: 'openapi_document', document: upstream.document('Records').toString() },
      credentialRoute: 'fixed',
      authentication: {
        method: 'oauth_authorization_code',
        authorization_url: `${upstream.origin}/authorize`,
        token_url: `${upstream.origin}/token`,
        scopes: ['read', 'write'],
        client: { client_id: 'manual-client', authentication: 'client_secret_post' },
      },
    }
    let link = ''
    let run = ''
    await step('The Program gets a link where you sign in', async () => {
      const { update } = await observeRun(
        runner,
        await runner.exec(`return await coral.workspace.addProvider(${JSON.stringify(call)});`),
      )
      run = `${workspace.name}/runs/${update.run_id}`
      const { data } = (update.final_value as { value: { data: { status: string; url: string } } })
        .value
      expect(data.status).toBe('needs_sign_in')
      link = data.url
    })
    await step('The stepper asks for the secret and names the one host it goes to', async () => {
      await page.goto(link)
      await acceptTools(page)
      await page
        .getByText(
          `Enter the secret of this OAuth application. Coral sends it only to ${new URL(upstream.origin).host}.`,
          { exact: true },
        )
        .waitFor()
      await page.getByLabel('Client secret', { exact: true }).fill(secret)
    })
    await step('Sign in, and add the provider with the shared account', async () => {
      await signInAndShareAccount(page)
      await page.getByRole('link', { name: 'Approve fixture access', exact: true }).click()
      await page.waitForURL((url) => url.pathname === `/workspaces/${id}/providers/new`)
      const add = page.getByRole('button', { name: 'Add provider', exact: true })
      await add.waitFor()
      await add.click()
      await waitForPublication(page)
      const [provider, ...others] = addedProviders(
        (await api.providers.listProviders({ parent: workspace.name })).providers,
      )
      expect([provider.callRoot, others]).toEqual(['coral.providers.records', []])
      // One exchange, and the endpoint accepted it, so it carried the secret.
      expect(upstream.counts.tokens).toBe(1)
      const { account } = await api.accounts.getProviderAccount({
        parent: workspace.name,
        provider: provider.name,
        scope: ProviderAccountScope.SHARED,
      })
      expect(account?.state).toBe(ProviderAccountState.READY)
    })
    await step("The secret stays out of the Run's arguments and Activity", async () => {
      const entries = await api.activity.browseRunEntries({ name: run, pageSize: 25 })
      const invocation = entries.entries.find((entry) => entry.record.case === 'invocation')!.record
      if (invocation.case !== 'invocation') throw new Error('Expected the addProvider call')
      const query = new URLSearchParams({ tree: '1', record: invocation.value.name })
      query.append('expand', `${workspace.name}/tasks/${runner.taskId()}`)
      query.append('expand', run)
      await page.goto(`/${workspace.name}/activity?${query}`)
      // The exact call shows, field by field, and none of it is the secret.
      expect(await shownArguments(page.getByRole('dialog'))).toMatchObject({
        namespace: call.namespace,
        credentialRoute: call.credentialRoute,
        authentication: call.authentication,
      })
      expect(await page.content()).not.toContain(secret)
      for (const body of await responses()) expect(body).not.toContain(secret)
    })
  })
})

test("Another owner finds nothing at an owner's sign-in link, and the owner who got it adds the provider", async (context) => {
  const { alice, bob } = await createActors(context)
  const api = managementClients(alice.accessToken)
  let workspace = ''
  let link = ''
  await withBrowserSession(
    context,
    async ({ page, target, step }) => {
      workspace = (await programWorkspace(api, step, ['addProvider'])).name
      await api.memberships.createWorkspaceMembership({
        parent: workspace,
        email: bob.email,
        role: WorkspaceRole.OWNER,
        requestId: crypto.randomUUID(),
      })
      const id = workspace.split('/')[1]
      const runner = programRunner(page, target, id, {
        accessToken: await mcpToken(page, id),
        taskIntent: 'Add the weather provider',
      })
      const call = {
        namespace: 'weather',
        source: {
          kind: 'openapi_document',
          document: openApiDocument(
            'Weather',
            { getForecast: { summary: 'Get the forecast' } },
            { security: 'bearer' },
          ).toString(),
        },
        credentialRoute: 'fixed',
      }
      await step("Alice's Program gets a link where she signs in", async () => {
        const { update } = await observeRun(
          runner,
          await runner.exec(`return await coral.workspace.addProvider(${JSON.stringify(call)});`),
        )
        link = (update.final_value as { value: { data: { url: string } } }).value.data.url
      })
    },
    { actor: alice.label, storageState: alice.storageState },
  )
  await withBrowserSession(
    context,
    async ({ page, step }) => {
      await step("Bob opens Alice's link and finds no page there", async () => {
        await page.goto(link)
        await page.getByText('This page does not exist.', { exact: true }).waitFor()
        expect(await page.getByRole('button', { name: 'Add provider', exact: true }).count()).toBe(
          0,
        )
        const providers = addedProviders(
          (await api.providers.listProviders({ parent: workspace })).providers,
        )
        expect(providers).toEqual([])
      })
    },
    { actor: bob.label, storageState: bob.storageState },
  )
  await withBrowserSession(
    context,
    async ({ page, step }) => {
      await step('Alice opens the same link and adds the provider', async () => {
        await page.goto(link)
        await acceptTools(page)
        await connectSharedAccount(page, 'weather-token')
        const add = page.getByRole('button', { name: 'Add provider', exact: true })
        await add.waitFor()
        await add.click()
        await waitForPublication(page)
        const providers = addedProviders(
          (await api.providers.listProviders({ parent: workspace })).providers,
        )
        expect(providers.map((provider) => provider.callRoot)).toEqual(['coral.providers.weather'])
      })
    },
    { actor: alice.label, storageState: alice.storageState },
  )
})

test(
  "A workspace owner approves their Program's new provider, then signs in at its link",
  approvedProgramProvider,
)

test(
  "A workspace owner denies their Program's new provider, and the Program catches the failure",
  deniedProgramProvider,
)
