import { createServer, type IncomingHttpHeaders } from 'node:http'
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
  acceptTools,
  addedProviders,
  checkRadio,
  configurationNext,
  connectSharedAccount,
  observeRun,
  openDialog,
  openReview,
  toolGroup,
  pausedForApproval,
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
 * A Weather document source with `tools` that declares it needs no credential. Calls go to
 * `server`, else to a host nobody calls.
 */
function credentialFree(tools: Record<string, object>, server?: string) {
  const document = JSON.parse(openApiDocument('Weather', tools, { server }).toString())
  document.security = []
  return { kind: 'openapi_document', document: JSON.stringify(document) }
}

/** Runs one Built-in Tool in a Program and returns its result. */
async function builtinCall(runner: ReturnType<typeof programRunner>, tool: string, call: unknown) {
  const { update } = await observeRun(
    runner,
    await runner.exec(`return await coral.workspace.${tool}(${JSON.stringify(call)});`),
  )
  expect(update.required_actions).toEqual([])
  return (update.final_value as { value: { data: { status: string; url: string } } }).value.data
}

/** Opens the link a Program got, saves the change it prepared, and waits for it to publish. */
async function saveAtLink(page: Session['page'], link: string) {
  await page.goto(link)
  await page.getByRole('button', { name: 'Save', exact: true }).click()
  await page.waitForURL((url) => url.pathname.endsWith('/settings'))
}

/**
 * A workspace with the credential-free `weather` provider that the owner's Program added from
 * `source` with the configuration `choices`, and a runner for their Programs. They call each
 * Built-in Tool in `unattended` without approval: by default, they update providers too.
 */
async function weatherProvider(
  { page, target, actor, step }: Session,
  source: object,
  {
    choices = {},
    unattended = ['addProvider', 'updateProvider'],
  }: { choices?: object; unattended?: string[] } = {},
) {
  const api = managementClients(actor.accessToken)
  const workspace = await programWorkspace(api, step, unattended)
  const id = workspace.name.split('/')[1]
  const program = programRunner(page, target, id, {
    accessToken: await mcpToken(page, id),
    taskIntent: 'Update the weather provider',
  })
  await step('The Program adds a provider that needs no sign-in, and you add it', async () => {
    const created = await builtinCall(program, 'addProvider', {
      namespace: 'weather',
      source,
      credentialRoute: 'none',
      ...choices,
    })
    expect(created.status).toBe('needs_person')
    await page.goto(created.url)
    await acceptTools(page)
    const add = page.getByRole('button', { name: 'Add provider', exact: true })
    await add.waitFor()
    await add.click()
    await waitForPublication(page)
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
  const workspace = await programWorkspace(api, step, ['addProvider', 'updateProvider'])
  const id = workspace.name.split('/')[1]
  const program = programRunner(page, target, id, {
    accessToken: await mcpToken(page, id),
    taskIntent: 'Update the records provider',
  })
  await step(
    'The Program adds an OAuth provider, and you sign in to its shared account',
    async () => {
      const created = await builtinCall(program, 'addProvider', {
        namespace: 'records',
        source: { kind: 'openapi_document', document: upstream.document('Records').toString() },
        credentialRoute: 'fixed',
        authentication: { ...oauth(upstream, scopes), client: { client_id: 'manual-client' } },
      })
      expect(created.status).toBe('needs_person')
      await page.goto(created.url)
      await acceptTools(page)
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

test("A workspace owner's Program replaces a source that drops a tool, and you review the tools before you save", async (context) => {
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
      expect(changed.status).toBe('needs_person')
      link = changed.url
      // Nothing changes until you save.
      expect(await api.providers.getProvider({ name: provider.name })).toEqual(provider)
    })
    await step('Review the tools the change adds and removes, and save', async () => {
      await page.goto(link)
      await toolGroup(page, 'New').getByText('newReport', { exact: true }).waitFor()
      await toolGroup(page, 'Removed').getByText('oldReport', { exact: true }).waitFor()
      await page.getByRole('button', { name: 'Save', exact: true }).click()
      await page.waitForURL((url) => url.pathname.endsWith('/settings'))
      const tools = (await api.providers.listTools({ parent: provider.name })).tools
      expect(tools.map((tool) => tool.relativePath).toSorted()).toEqual([
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
        expect(changed.status).toBe('needs_person')
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
    const { page, step } = session
    const { api, workspace, program, provider } = await recordsProvider(session, upstream, ['read'])
    const before = { ...upstream.counts }
    await step('The Program changes a header, and you save it with no sign-in', async () => {
      const changed = await builtinCall(program, 'updateProvider', {
        namespace: 'records',
        requestHeaders: { 'x-trace': 'on' },
      })
      expect(changed.status).toBe('needs_person')
      await saveAtLink(page, changed.url)
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
        expect(changed.status).toBe('needs_person')
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

/**
 * A Weather service that answers each of its `tools` with a sunny forecast, and serves their
 * credential-free document at /openapi.json. It keeps each call's path and headers.
 */
async function weatherService(
  context: Parameters<typeof listen>[0],
  tools: Record<string, object> = { getForecast: {} },
) {
  const service = {
    tools,
    reads: 0,
    calls: [] as { path: string; headers: IncomingHttpHeaders }[],
  }
  const fixture = await listen(
    context,
    createServer((request, response) => {
      const path = request.url ?? ''
      if (path === '/openapi.json') {
        service.reads++
        response
          .writeHead(200, { 'content-type': 'application/json' })
          .end(credentialFree(service.tools, fixture.origin).document)
        return
      }
      if (!Object.hasOwn(service.tools, path.slice(1))) {
        response.writeHead(404).end()
        return
      }
      service.calls.push({ path, headers: request.headers })
      response.writeHead(200, { 'content-type': 'application/json' }).end('{"forecast":"sunny"}')
    }),
  )
  return Object.assign(service, fixture)
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
      expect(changed.status).toBe('needs_person')
      link = changed.url
    })
    await step('Review the tool the server adds and the tool it drops, and save', async () => {
      await page.goto(link)
      await toolGroup(page, 'New').getByText('hello', { exact: true }).waitFor()
      await toolGroup(page, 'Removed').getByText('getForecast', { exact: true }).waitFor()
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
      const tools = (await api.providers.listTools({ parent: provider.name })).tools
      expect(tools.map((tool) => tool.relativePath)).toEqual(['hello'])
    })
  })
})

test("A workspace owner's Program moves a provider from an MCP server to an OpenAPI document, and calls reach the document's service", async (context) => {
  const server = await bearerMcpServer(context, '', true)
  const service = await weatherService(context)
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
        expect(changed.status).toBe('needs_person')
        link = changed.url
      },
    )
    await step('Review the tool the document adds and the tool it drops, and save', async () => {
      await page.goto(link)
      await toolGroup(page, 'New').getByText('getForecast', { exact: true }).waitFor()
      await toolGroup(page, 'Removed').getByText('hello', { exact: true }).waitFor()
      await page.getByRole('button', { name: 'Save', exact: true }).click()
      await page.waitForURL((url) => url.pathname.endsWith('/settings'))
      const source = await api.providers.getProviderSource({ name: provider.name })
      // An uploaded document has no URL to read again.
      expect([source.kind, source.url]).toEqual([ProviderSetupInputKind.OPENAPI, ''])
    })
    await step("A Program calls the document's tool under the same namespace", async () => {
      expect(
        await finalValue(program, 'return await coral.providers.weather.getForecast({});'),
      ).toContain('sunny')
      expect(service.calls).toHaveLength(1)
      const tools = (await api.providers.listTools({ parent: provider.name })).tools
      expect(tools.map((tool) => tool.relativePath)).toEqual(['getForecast'])
    })
  })
})

test("A workspace owner's Program renames a provider, moves it to another server and drops a header, and you save the change", async (context) => {
  const first = await weatherService(context)
  const second = await weatherService(context)
  await withOwnerSession(context, async (session) => {
    const { page, step } = session
    const { api, program, provider } = await weatherProvider(
      session,
      credentialFree({ getForecast: {} }, first.origin),
      { choices: { requestHeaders: { 'x-trace': 'on' } } },
    )
    const forecast = 'return await coral.providers.weather.getForecast({});'
    await step('A call reaches the first server with the header', async () => {
      expect(await finalValue(program, forecast)).toContain('sunny')
      expect(first.calls.map((call) => call.headers['x-trace'])).toEqual(['on'])
    })
    await step(
      'The Program changes the name, the server and the header in one call, and you save it',
      async () => {
        const changed = await builtinCall(program, 'updateProvider', {
          namespace: 'weather',
          displayName: 'Forecasts',
          servers: { '#/servers': { kind: 'custom', url: second.origin } },
          // null removes the saved header.
          requestHeaders: { 'x-trace': null },
        })
        expect(changed.status).toBe('needs_person')
        await saveAtLink(page, changed.url)
        const updated = await api.providers.getProvider({ name: provider.name })
        expect([updated.callRoot, updated.displayName]).toEqual([
          'coral.providers.weather',
          'Forecasts',
        ])
      },
    )
    await step('A call reaches the new server without the header', async () => {
      expect(await finalValue(program, forecast)).toContain('sunny')
      expect(first.calls).toHaveLength(1)
      expect(second.calls.map((call) => call.headers['x-trace'])).toEqual([undefined])
    })
  })
})

test("A workspace owner's Program reads a provider's document URL again, and calls reach the tool it adds", async (context) => {
  const service = await weatherService(context)
  await withOwnerSession(context, async (session) => {
    const { page, step } = session
    const { api, program, provider } = await weatherProvider(session, {
      kind: 'openapi_url',
      url: `${service.origin}/openapi.json`,
    })
    await step('The service adds a tool, the Program reads it, and you save it', async () => {
      service.tools = { getForecast: {}, getAlerts: {} }
      const reads = service.reads
      const changed = await builtinCall(program, 'updateProvider', {
        namespace: 'weather',
        refresh: true,
      })
      expect(changed.status).toBe('needs_person')
      expect(service.reads).toBe(reads + 1)
      await saveAtLink(page, changed.url)
    })
    await step('A Program calls the new tool', async () => {
      expect(
        await finalValue(program, 'return await coral.providers.weather.getAlerts({});'),
      ).toContain('sunny')
      expect(service.calls.map((call) => call.path)).toEqual(['/getAlerts'])
      const tools = (await api.providers.listTools({ parent: provider.name })).tools
      expect(tools.map((tool) => tool.relativePath).toSorted()).toEqual([
        'getAlerts',
        'getForecast',
      ])
    })
  })
})

test("A workspace owner's Program asks for a shared API key in a header, and you enter it in the box the Program labels", async (context) => {
  const service = await weatherService(context)
  await withOwnerSession(context, async (session) => {
    const { page, step } = session
    const { api, workspace, program, provider } = await weatherProvider(
      session,
      credentialFree({ getForecast: {} }, service.origin),
    )
    let link = ''
    await step(
      'The Program asks for a key in a header, and gets a link where you enter it',
      async () => {
        const changed = await builtinCall(program, 'updateProvider', {
          namespace: 'weather',
          credentialRoute: 'fixed',
          authentication: {
            method: 'header_credentials',
            headers: { 'X-Weather-Key': { source: 'field', field: 'key' } },
          },
          credentialFields: { key: { label: 'Weather key', sensitive: true } },
        })
        expect(changed.status).toBe('needs_person')
        link = changed.url
        expect(await api.providers.getProvider({ name: provider.name })).toEqual(provider)
      },
    )
    await step('Enter the key in the box the Program labelled, and save', async () => {
      await page.goto(link)
      const dialog = await openDialog(
        page,
        page.getByRole('button', { name: 'Connect the shared account', exact: true }),
      )
      await dialog.getByLabel('Weather key', { exact: true }).fill('weather-key')
      await dialog.getByRole('button', { name: 'Save and share the account', exact: true }).click()
      // A new account setting breaks access, so Save asks first.
      await saveConfirmedSettingsChange(page)
      const { account } = await api.accounts.getProviderAccount({
        parent: workspace.name,
        provider: provider.name,
        scope: ProviderAccountScope.SHARED,
      })
      expect(account?.state).toBe(ProviderAccountState.READY)
    })
    await step("A Program's call sends the key in the header", async () => {
      expect(
        await finalValue(program, 'return await coral.providers.weather.getForecast({});'),
      ).toContain('sunny')
      expect(service.calls.map((call) => call.headers['x-weather-key'])).toEqual(['weather-key'])
    })
  })
})

test("A workspace owner denies their Program's rename, then approves it, and the providers page shows the new name", async (context) => {
  await withOwnerSession(context, async (session) => {
    const { page, step } = session
    // Only addProvider runs without approval, so each updateProvider call waits for you.
    const { api, workspace, program, provider } = await weatherProvider(
      session,
      credentialFree({ getForecast: {} }),
      { unattended: ['addProvider'] },
    )
    const rename = `try { return await coral.workspace.updateProvider(${JSON.stringify({
      namespace: 'weather',
      displayName: 'Forecasts',
    })}); } catch (error) { return error.code; }`
    await step('Deny the rename, and the Program catches the failure', async () => {
      const paused = await pausedForApproval(program, rename)
      const review = await openReview(page, paused, 'updateProvider')
      await review.getByRole('button', { name: 'Deny this call', exact: true }).click()
      await page.getByText('Denied', { exact: true }).waitFor()
      const { update } = await observeRun(program, await program.wait(paused))
      expect(update.final_value).toEqual({ kind: 'value', value: 'approval_declined' })
      expect(await api.providers.getProvider({ name: provider.name })).toEqual(provider)
    })
    await step('Approve the same rename in a new Run, and Coral publishes it', async () => {
      const paused = await pausedForApproval(program, rename)
      const review = await openReview(page, paused, 'updateProvider')
      await review.getByRole('button', { name: 'Approve this call', exact: true }).click()
      await page.getByText('Approved', { exact: true }).waitFor()
      const { update } = await observeRun(program, await program.wait(paused))
      expect(update.final_value).toMatchObject({
        kind: 'value',
        value: { data: { status: 'published' } },
      })
    })
    await step('Show the new name on the providers page', async () => {
      await page.goto(`/workspaces/${workspace.name.split('/')[1]}/providers`)
      await page
        .getByRole('navigation', { name: 'installed providers', exact: true })
        .getByRole('link', { name: 'Forecasts settings', exact: true })
        .waitFor()
    })
  })
})

test("Another owner finds nothing at an owner's update link, and the owner who got it saves the change", async (context) => {
  const { alice, bob } = await createActors(context)
  const api = managementClients(alice.accessToken)
  let workspace = ''
  let link = ''
  const tools = async () => {
    const [provider] = addedProviders(
      (await api.providers.listProviders({ parent: workspace })).providers,
    )
    return (await api.providers.listTools({ parent: provider.name })).tools
      .map((tool) => tool.relativePath)
      .toSorted()
  }
  await withBrowserSession(
    context,
    async ({ page, target, step }) => {
      workspace = (await programWorkspace(api, step, ['addProvider', 'updateProvider'])).name
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
          const created = await builtinCall(program, 'addProvider', {
            namespace: 'weather',
            source: credentialFree({ getForecast: {}, oldReport: {} }),
            credentialRoute: 'none',
          })
          expect(created.status).toBe('published')
          const changed = await builtinCall(program, 'updateProvider', {
            namespace: 'weather',
            source: credentialFree({ getForecast: {}, newReport: {} }),
          })
          expect(changed.status).toBe('needs_person')
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
        expect(await tools()).toEqual(['getForecast', 'oldReport'])
      })
    },
    { actor: bob.label, storageState: bob.storageState },
  )
  await withBrowserSession(
    context,
    async ({ page, step }) => {
      await step('Alice opens the same link, reviews the tools, and saves', async () => {
        await page.goto(link)
        await toolGroup(page, 'Removed').getByText('oldReport', { exact: true }).waitFor()
        await page.getByRole('button', { name: 'Save', exact: true }).click()
        await page.waitForURL((url) => url.pathname.endsWith('/settings'))
        expect(await tools()).toEqual(['getForecast', 'newReport'])
      })
    },
    { actor: alice.label, storageState: alice.storageState },
  )
})
