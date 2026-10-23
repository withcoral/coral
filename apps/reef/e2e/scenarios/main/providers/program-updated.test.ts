import { createServer } from 'node:http'
import { expect, test } from 'vitest'
import { createActors, mcpToken, withOwnerSession } from '../../../support/actors.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'
import { managementClients } from '../../../support/management.ts'
import {
  bearerMcpServer,
  listen,
  oauthApiServer,
  openApiDocument,
} from '../../../support/provider-fixtures.ts'
import {
  acceptOperations,
  addedProviders,
  checkRadio,
  configurationNext,
  connectSharedAccount,
  observeRun,
  openDialog,
  operationGroup,
  programRunner,
  programWorkspace,
  saveConfirmedSettingsChange,
  signInAndShareAccount,
  waitForPublication,
} from '../../../support/providers.ts'
import {
  ProviderAccountScope,
  ProviderAccountState,
} from '../../../generated/coral/management/v1/account_pb.ts'
import { ProviderSetupInputKind } from '../../../generated/coral/management/v1/provider_pb.ts'
import { WorkspaceRole } from '../../../generated/coral/management/v1/workspace_pb.ts'

type Session = Parameters<Parameters<typeof withOwnerSession>[1]>[0]

/**
 * A Weather document source with `operations` that declares it needs no credential. Calls go to
 * `server`, else to a host nobody calls.
 */
function credentialFree(operations: Record<string, object>, server?: string) {
  const document = JSON.parse(openApiDocument('Weather', operations, { server }).toString())
  document.security = []
  return { kind: 'openapi_document', document: JSON.stringify(document) }
}

/** Runs one Built-in Operation in a Program and returns its result. */
async function builtinCall(
  runner: ReturnType<typeof programRunner>,
  operation: string,
  call: unknown,
) {
  const { update } = await observeRun(
    runner,
    await runner.exec(`return await coral.workspace.${operation}(${JSON.stringify(call)});`),
  )
  expect(update.required_actions).toEqual([])
  return (update.final_value as { value: { data: { status: string; url: string } } }).value.data
}

/**
 * A workspace with the credential-free `weather` provider that the owner's Program added from
 * `source`, and a runner for their Programs, which update providers without approval.
 */
async function weatherProvider({ page, target, actor, step }: Session, source: object) {
  const api = managementClients(actor.accessToken)
  const workspace = await programWorkspace(api, step, ['createProvider', 'updateProvider'])
  const id = workspace.name.split('/')[1]
  const program = programRunner(page, target, id, {
    accessToken: await mcpToken(page, id),
    taskIntent: 'Update the weather provider',
  })
  await step('The Program adds a provider that needs no sign-in', async () => {
    const created = await builtinCall(program, 'createProvider', {
      namespace: 'weather',
      source,
      credentialRoute: 'none',
    })
    expect(created.status).toBe('published')
  })
  const [provider] = addedProviders(
    (await api.providers.listProviders({ parent: workspace.name })).providers,
  )
  return { api, workspace, program, provider }
}

type OAuthApi = Awaited<ReturnType<typeof oauthApiServer>>

/** How a Program asks for a sign-in to `upstream` with `scopes`. */
function oauth(upstream: OAuthApi, scopes: string[]) {
  return {
    method: 'oauth_authorization_code',
    authorization_url: `${upstream.origin}/authorize`,
    token_url: `${upstream.origin}/token`,
    scopes,
  }
}

/**
 * A workspace with the `records` provider that the owner's Program added from `upstream`'s OAuth
 * document with `scopes`, signed in to one shared account, and a runner for their Programs.
 */
async function recordsProvider(
  { page, target, actor, step }: Session,
  upstream: OAuthApi,
  scopes: string[],
) {
  const api = managementClients(actor.accessToken)
  const workspace = await programWorkspace(api, step, ['createProvider', 'updateProvider'])
  const id = workspace.name.split('/')[1]
  const program = programRunner(page, target, id, {
    accessToken: await mcpToken(page, id),
    taskIntent: 'Update the records provider',
  })
  await step(
    'The Program adds an OAuth provider, and you sign in to its shared account',
    async () => {
      const created = await builtinCall(program, 'createProvider', {
        namespace: 'records',
        source: { kind: 'openapi_document', document: upstream.document('Records').toString() },
        credentialRoute: 'fixed',
        authentication: { ...oauth(upstream, scopes), client: { client_id: 'manual-client' } },
      })
      expect(created.status).toBe('needs_sign_in')
      await page.goto(created.url)
      await acceptOperations(page)
      await signInAndShareAccount(page)
      await page.getByRole('link', { name: 'Approve fixture access', exact: true }).click()
      await page.waitForURL((url) => url.pathname === `/workspaces/${id}/providers/new`)
      const add = page.getByRole('button', { name: 'Add provider', exact: true })
      await add.waitFor()
      await add.click()
      await waitForPublication(page)
    },
  )
  const [provider] = addedProviders(
    (await api.providers.listProviders({ parent: workspace.name })).providers,
  )
  return { api, workspace, program, provider }
}

test("A workspace owner's Program replaces a source that drops an operation, and you review the operations before you save", async (context) => {
  await withOwnerSession(context, async (session) => {
    const { page, step } = session
    const { api, program, provider } = await weatherProvider(
      session,
      credentialFree({ getForecast: {}, oldReport: {} }),
    )
    let link = ''
    await step('The Program replaces the source, and gets a link to review it', async () => {
      const changed = await builtinCall(program, 'updateProvider', {
        namespace: 'weather',
        source: credentialFree({ getForecast: {}, newReport: {} }),
      })
      expect(changed.status).toBe('needs_sign_in')
      link = changed.url
      // Nothing changes until you save.
      expect(await api.providers.getProvider({ name: provider.name })).toEqual(provider)
    })
    await step('Review the operations the change adds and removes, and save', async () => {
      await page.goto(link)
      await operationGroup(page, 'New').getByText('newReport', { exact: true }).waitFor()
      await operationGroup(page, 'Removed').getByText('oldReport', { exact: true }).waitFor()
      await page.getByRole('button', { name: 'Save', exact: true }).click()
      await page.waitForURL((url) => url.pathname.endsWith('/settings'))
      const operations = (await api.providers.listOperations({ parent: provider.name })).operations
      expect(operations.map((operation) => operation.relativePath).toSorted()).toEqual([
        'getForecast',
        'newReport',
      ])
    })
  })
})

test("A workspace owner's Program moves a provider to a shared account, and you connect it and confirm the change", async (context) => {
  await withOwnerSession(context, async (session) => {
    const { page, step } = session
    const { api, workspace, program, provider } = await weatherProvider(
      session,
      credentialFree({ getForecast: {} }),
    )
    let link = ''
    await step(
      'The Program asks for a shared account, and gets a link where you sign in',
      async () => {
        const changed = await builtinCall(program, 'updateProvider', {
          namespace: 'weather',
          source: {
            kind: 'openapi_document',
            document: openApiDocument(
              'Weather',
              { getForecast: {} },
              { security: 'bearer' },
            ).toString(),
          },
          credentialRoute: 'fixed',
          authentication: { method: 'bearer' },
        })
        expect(changed.status).toBe('needs_sign_in')
        link = changed.url
        expect(await api.providers.getProvider({ name: provider.name })).toEqual(provider)
      },
    )
    await step('Check the configuration, connect the shared account, and save', async () => {
      await page.goto(link)
      // As after Replace source, a change that breaks access opens on its configuration.
      await configurationNext(page).click()
      // The account step opens on the account the Program asked for.
      await expect
        .poll(() =>
          page.getByRole('radio', { name: 'Connect an account', exact: true }).isChecked(),
        )
        .toBe(true)
      await connectSharedAccount(page, 'weather-token')
      // A new account setting breaks access, so Save asks first.
      await saveConfirmedSettingsChange(page)
      const { account } = await api.accounts.getProviderAccount({
        parent: workspace.name,
        provider: provider.name,
        scope: ProviderAccountScope.SHARED,
      })
      expect(account?.state).toBe(ProviderAccountState.READY)
    })
  })
})

test("A workspace owner's Program changes a header on an OAuth provider, and calls keep the shared account's sign-in", async (context) => {
  const upstream = await oauthApiServer(context, `account-${crypto.randomUUID()}`)
  await withOwnerSession(context, async (session) => {
    const { step } = session
    const { api, workspace, program, provider } = await recordsProvider(session, upstream, ['read'])
    const before = { ...upstream.counts }
    await step('The Program changes a header, and Coral publishes it with no sign-in', async () => {
      const changed = await builtinCall(program, 'updateProvider', {
        namespace: 'records',
        requestHeaders: { 'x-trace': 'on' },
      })
      expect(changed.status).toBe('published')
      expect([upstream.counts.consents, upstream.counts.tokens]).toEqual([
        before.consents,
        before.tokens,
      ])
    })
    await step("A Program's call still uses the shared account", async () => {
      expect(
        await finalValue(program, 'return await coral.providers.records.listRecords({});'),
      ).toContain('Hello from the OAuth API')
      expect(upstream.counts.calls).toBe(before.calls + 1)
      const { account } = await api.accounts.getProviderAccount({
        parent: workspace.name,
        provider: provider.name,
        scope: ProviderAccountScope.SHARED,
      })
      expect(account?.state).toBe(ProviderAccountState.READY)
    })
  })
})

test("A workspace owner's Program adds a scope to an OAuth provider, and you sign in again at its link", async (context) => {
  const upstream = await oauthApiServer(context, `account-${crypto.randomUUID()}`)
  await withOwnerSession(context, async (session) => {
    const { page, step } = session
    const { api, program, provider } = await recordsProvider(session, upstream, ['read'])
    const before = { ...upstream.counts }
    let link = ''
    await step(
      'The Program asks for another scope, and gets a link where you sign in',
      async () => {
        // A sign-in sent without client keeps the saved OAuth application.
        const changed = await builtinCall(program, 'updateProvider', {
          namespace: 'records',
          authentication: oauth(upstream, ['read', 'write']),
        })
        expect(changed.status).toBe('needs_sign_in')
        link = changed.url
        // Nothing changes until you save.
        expect(await api.providers.getProvider({ name: provider.name })).toEqual(provider)
      },
    )
    await step('Sign in again with both scopes, and save', async () => {
      await page.goto(link)
      const dialog = await openDialog(
        page,
        page.getByRole('button', { name: 'Connect the shared account', exact: true }),
      )
      await dialog
        .getByRole('button', { name: 'Sign in and share the account', exact: true })
        .click()
      const approve = page.getByRole('link', { name: 'Approve fixture access', exact: true })
      await approve.waitFor()
      expect(upstream.lastConsent?.searchParams.get('scope')).toBe('read write')
      await approve.click()
      await checkRadio(page.getByRole('radio', { name: 'Use this account', exact: true }))
      // The new scope breaks access, so Save asks first.
      await saveConfirmedSettingsChange(page)
      expect(upstream.counts.tokens).toBe(before.tokens + 1)
    })
    await step("A Program's call uses the new sign-in", async () => {
      expect(
        await finalValue(program, 'return await coral.providers.records.listRecords({});'),
      ).toContain('Hello from the OAuth API')
      expect(upstream.counts.calls).toBe(before.calls + 1)
    })
  })
})

/** An OpenAPI service whose one Operation, `getForecast`, answers with a sunny forecast. */
async function forecastServer(context: Parameters<typeof listen>[0]) {
  const counts = { calls: 0 }
  const fixture = await listen(
    context,
    createServer((request, response) => {
      if (request.url !== '/getForecast') {
        response.writeHead(404).end()
        return
      }
      counts.calls++
      response.writeHead(200, { 'content-type': 'application/json' }).end('{"forecast":"sunny"}')
    }),
  )
  return { ...fixture, counts }
}

/** Runs `program` in a new Run and returns its Final Value as JSON text. */
async function finalValue(runner: ReturnType<typeof programRunner>, program: string) {
  const { update } = await observeRun(runner, await runner.exec(program))
  return JSON.stringify(update.final_value)
}

test("A workspace owner's Program moves a provider from an OpenAPI document to an MCP server, and calls reach the server's tool", async (context) => {
  const server = await bearerMcpServer(context, '', true)
  const mcpUrl = `${server.origin}/mcp`
  await withOwnerSession(context, async (session) => {
    const { page, step } = session
    const { api, program, provider } = await weatherProvider(
      session,
      credentialFree({ getForecast: {} }),
    )
    let link = ''
    await step('The Program reads the provider from an MCP server, and gets a link', async () => {
      const changed = await builtinCall(program, 'updateProvider', {
        namespace: 'weather',
        source: { kind: 'mcp_url', url: mcpUrl },
      })
      // The server drops getForecast, which only a person can confirm.
      expect(changed.status).toBe('needs_sign_in')
      link = changed.url
    })
    await step('Review the tool the server adds and the operation it drops, and save', async () => {
      await page.goto(link)
      await operationGroup(page, 'New').getByText('hello', { exact: true }).waitFor()
      await operationGroup(page, 'Removed').getByText('getForecast', { exact: true }).waitFor()
      await page.getByRole('button', { name: 'Save', exact: true }).click()
      await page.waitForURL((url) => url.pathname.endsWith('/settings'))
      const source = await api.providers.getProviderSource({ name: provider.name })
      expect([source.kind, source.url]).toEqual([ProviderSetupInputKind.MCP, mcpUrl])
    })
    await step("A Program calls the server's tool under the same namespace", async () => {
      expect(
        await finalValue(program, 'return await coral.providers.weather.hello({});'),
      ).toContain('Hello from the bearer fixture')
      expect(server.counts.calls).toBe(1)
      const operations = (await api.providers.listOperations({ parent: provider.name })).operations
      expect(operations.map((operation) => operation.relativePath)).toEqual(['hello'])
    })
  })
})

test("A workspace owner's Program moves a provider from an MCP server to an OpenAPI document, and calls reach the document's service", async (context) => {
  const server = await bearerMcpServer(context, '', true)
  const service = await forecastServer(context)
  await withOwnerSession(context, async (session) => {
    const { page, step } = session
    const { api, program, provider } = await weatherProvider(session, {
      kind: 'mcp_url',
      url: `${server.origin}/mcp`,
    })
    let link = ''
    await step(
      'The Program reads the provider from an OpenAPI document, and gets a link',
      async () => {
        const changed = await builtinCall(program, 'updateProvider', {
          namespace: 'weather',
          source: credentialFree({ getForecast: {} }, service.origin),
        })
        // The document drops the server's hello tool, which only a person can confirm.
        expect(changed.status).toBe('needs_sign_in')
        link = changed.url
      },
    )
    await step(
      'Review the operation the document adds and the tool it drops, and save',
      async () => {
        await page.goto(link)
        await operationGroup(page, 'New').getByText('getForecast', { exact: true }).waitFor()
        await operationGroup(page, 'Removed').getByText('hello', { exact: true }).waitFor()
        await page.getByRole('button', { name: 'Save', exact: true }).click()
        await page.waitForURL((url) => url.pathname.endsWith('/settings'))
        const source = await api.providers.getProviderSource({ name: provider.name })
        // An uploaded document has no URL to read again.
        expect([source.kind, source.url]).toEqual([ProviderSetupInputKind.OPENAPI, ''])
      },
    )
    await step("A Program calls the document's operation under the same namespace", async () => {
      expect(
        await finalValue(program, 'return await coral.providers.weather.getForecast({});'),
      ).toContain('sunny')
      expect(service.counts.calls).toBe(1)
      const operations = (await api.providers.listOperations({ parent: provider.name })).operations
      expect(operations.map((operation) => operation.relativePath)).toEqual(['getForecast'])
    })
  })
})

test("Another owner finds nothing at an owner's update link, and the owner who got it saves the change", async (context) => {
  const { alice, bob } = await createActors(context)
  const api = managementClients(alice.accessToken)
  let workspace = ''
  let link = ''
  const operations = async () => {
    const [provider] = addedProviders(
      (await api.providers.listProviders({ parent: workspace })).providers,
    )
    return (await api.providers.listOperations({ parent: provider.name })).operations
      .map((operation) => operation.relativePath)
      .toSorted()
  }
  await withBrowserSession(
    context,
    async ({ page, target, step }) => {
      workspace = (await programWorkspace(api, step, ['createProvider', 'updateProvider'])).name
      await api.memberships.createWorkspaceMembership({
        parent: workspace,
        email: bob.email,
        role: WorkspaceRole.OWNER,
        requestId: crypto.randomUUID(),
      })
      const id = workspace.split('/')[1]
      const program = programRunner(page, target, id, {
        accessToken: await mcpToken(page, id),
        taskIntent: 'Update the weather provider',
      })
      await step(
        "Alice's Program adds a provider, then replaces its source and gets a link",
        async () => {
          const created = await builtinCall(program, 'createProvider', {
            namespace: 'weather',
            source: credentialFree({ getForecast: {}, oldReport: {} }),
            credentialRoute: 'none',
          })
          expect(created.status).toBe('published')
          const changed = await builtinCall(program, 'updateProvider', {
            namespace: 'weather',
            source: credentialFree({ getForecast: {}, newReport: {} }),
          })
          expect(changed.status).toBe('needs_sign_in')
          link = changed.url
        },
      )
    },
    { actor: alice.label, storageState: alice.storageState },
  )
  await withBrowserSession(
    context,
    async ({ page, step }) => {
      await step("Bob opens Alice's link and finds no page there", async () => {
        await page.goto(link)
        await page.getByText('This page does not exist.', { exact: true }).waitFor()
        expect(await page.getByRole('button', { name: 'Save', exact: true }).count()).toBe(0)
        expect(await operations()).toEqual(['getForecast', 'oldReport'])
      })
    },
    { actor: bob.label, storageState: bob.storageState },
  )
  await withBrowserSession(
    context,
    async ({ page, step }) => {
      await step('Alice opens the same link, reviews the operations, and saves', async () => {
        await page.goto(link)
        await operationGroup(page, 'Removed').getByText('oldReport', { exact: true }).waitFor()
        await page.getByRole('button', { name: 'Save', exact: true }).click()
        await page.waitForURL((url) => url.pathname.endsWith('/settings'))
        expect(await operations()).toEqual(['getForecast', 'newReport'])
      })
    },
    { actor: alice.label, storageState: alice.storageState },
  )
})
