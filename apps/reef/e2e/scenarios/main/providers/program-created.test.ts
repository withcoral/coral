import { expect, test } from 'vitest'
import { createActors, mcpToken, withOwnerSession } from '../../../support/actors.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'
import { managementClients } from '../../../support/management.ts'
import {
  acceptOperations,
  addedProviders,
  collectDataResponses,
  connectSharedAccount,
  observeRun,
  programRunner,
  signInAndShareAccount,
  waitForPublication,
} from '../../../support/providers.ts'
import { oauthApiServer, openApiDocument } from '../../../support/provider-fixtures.ts'
import {
  ProviderAccountScope,
  ProviderAccountState,
} from '../../../generated/coral/management/v1/account_pb.ts'
import { OperationAccessState } from '../../../generated/coral/management/v1/policy_pb.ts'
import { WorkspaceRole } from '../../../generated/coral/management/v1/workspace_pb.ts'
import { approvedProgramProvider, deniedProgramProvider } from './program-approval.ts'

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
    await step('Review the operations, connect the account, and add the provider', async () => {
      await page.goto(link)
      // The stepper opens at the operations, with nothing left to configure.
      await acceptOperations(page)
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

test("A workspace owner's Program adds a provider where each person signs in with OAuth, and you add it after you sign in", async (context) => {
  const secret = `application-${crypto.randomUUID()}`
  const upstream = await oauthApiServer(context, `account-${crypto.randomUUID()}`, {
    clientSecret: secret,
  })
  await withOwnerSession(context, async ({ page, target, actor, step }) => {
    const api = managementClients(actor.accessToken)
    const workspace = await programWorkspace(api, step)
    const id = workspace.name.split('/')[1]
    const runner = programRunner(page, target, id, {
      accessToken: await mcpToken(page, id),
      taskIntent: 'Add the records provider',
    })
    const call = {
      namespace: 'records',
      source: { kind: 'openapi_document', document: upstream.document('Records').toString() },
      credentialRoute: 'caller',
      // Only a person can enter a confidential application's secret, so Coral
      // links you to the stepper rather than publish the provider itself.
      authentication: {
        method: 'oauth_authorization_code',
        authorization_url: `${upstream.origin}/authorize`,
        token_url: `${upstream.origin}/token`,
        scopes: ['read', 'write'],
        client: { client_id: 'manual-client', authentication: 'client_secret_post' },
      },
    }
    let link = ''
    await step('The Program gets a link where you sign in', async () => {
      const { update } = await observeRun(
        runner,
        await runner.exec(`return await coral.admin.createProvider(${JSON.stringify(call)});`),
      )
      const { data } = (update.final_value as { value: { data: { status: string; url: string } } })
        .value
      expect(data.status).toBe('needs_sign_in')
      link = data.url
      expect(new URL(link).searchParams.get('account')).toBe('caller')
    })
    const publications: string[] = []
    page.on('request', (request) => {
      const form = new URLSearchParams(request.postData() ?? '')
      if (request.method() === 'POST' && form.get('intent') === 'publish')
        publications.push(request.url())
    })
    await step('Sign in to your own account at the service', async () => {
      await page.goto(link)
      await acceptOperations(page)
      await page.getByLabel('Client secret', { exact: true }).fill(secret)
      await page.getByRole('button', { name: /^Sign in/ }).click()
      await page.getByRole('link', { name: 'Approve fixture access', exact: true }).click()
      await page.waitForURL((url) => url.pathname === `/workspaces/${id}/providers/new`)
    })
    await step('The stepper waits for Add provider, and no provider exists yet', async () => {
      // A menu opens only once the page hydrates, and React runs the step's
      // effects before it handles the click, so any publication has started.
      const menu = page.getByRole('menu')
      await expect
        .poll(
          async () => {
            if (!(await menu.isVisible()))
              await page.getByRole('button', { name: /^Open account menu for / }).click()
            return menu.isVisible()
          },
          { interval: 250, timeout: 5000 },
        )
        .toBe(true)
      await page.keyboard.press('Escape')
      await page.getByRole('button', { name: 'Add provider', exact: true }).waitFor()
      expect(publications).toEqual([])
      const providers = addedProviders(
        (await api.providers.listProviders({ parent: workspace.name })).providers,
      )
      expect(providers).toEqual([])
    })
    await step('Add the provider, with your account connected', async () => {
      await page.getByRole('button', { name: 'Add provider', exact: true }).click()
      await waitForPublication(page)
      expect(publications).toHaveLength(1)
      const [provider, ...others] = addedProviders(
        (await api.providers.listProviders({ parent: workspace.name })).providers,
      )
      expect([provider.callRoot, others]).toEqual(['coral.providers.records', []])
      const { account } = await api.accounts.getProviderAccount({
        parent: workspace.name,
        provider: provider.name,
        scope: ProviderAccountScope.USER,
      })
      expect(account?.state).toBe(ProviderAccountState.READY)
      expect(upstream.counts.tokens).toBe(1)
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
    const workspace = await programWorkspace(api, step)
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
        await runner.exec(`return await coral.admin.createProvider(${JSON.stringify(call)});`),
      )
      run = `${workspace.name}/runs/${update.run_id}`
      const { data } = (update.final_value as { value: { data: { status: string; url: string } } })
        .value
      expect(data.status).toBe('needs_sign_in')
      link = data.url
    })
    await step('The stepper asks for the secret and names the one host it goes to', async () => {
      await page.goto(link)
      await acceptOperations(page)
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
      if (invocation.case !== 'invocation') throw new Error('Expected the createProvider call')
      const query = new URLSearchParams({ tree: '1', record: invocation.value.name })
      query.append('expand', `${workspace.name}/tasks/${runner.taskId()}`)
      query.append('expand', run)
      await page.goto(`/${workspace.name}/activity?${query}`)
      await page.locator('pre').filter({ hasText: '"credentialRoute"' }).first().waitFor()
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
      workspace = (await programWorkspace(api, step)).name
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
          await runner.exec(`return await coral.admin.createProvider(${JSON.stringify(call)});`),
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
        await acceptOperations(page)
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
