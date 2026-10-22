import { createServer } from 'node:http'
import { expect, test } from 'vitest'
import { mcpToken, withOwnerSession } from '../../../support/actors.ts'
import { managementClients } from '../../../support/management.ts'
import { bearerMcpServer, listen, openApiDocument } from '../../../support/provider-fixtures.ts'
import {
  addedProviders,
  configurationNext,
  connectSharedAccount,
  observeRun,
  operationGroup,
  programRunner,
  programWorkspace,
  type RunUpdate,
  saveConfirmedSettingsChange,
} from '../../../support/providers.ts'
import {
  ProviderAccountScope,
  ProviderAccountState,
} from '../../../generated/coral/management/v1/account_pb.ts'
import { ProviderSetupInputKind } from '../../../generated/coral/management/v1/provider_pb.ts'
import { openReview, pausedForApproval } from './program-approval.ts'

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
 * `source`, and a runner for their Programs. Their Programs update providers without approval
 * unless `approveUpdates` is set.
 */
async function weatherProvider(
  { page, target, actor, step }: Session,
  source: object,
  {
    requestHeaders,
    approveUpdates = false,
  }: { requestHeaders?: Record<string, string>; approveUpdates?: boolean } = {},
) {
  const api = managementClients(actor.accessToken)
  const workspace = await programWorkspace(
    api,
    step,
    approveUpdates ? ['createProvider'] : ['createProvider', 'updateProvider'],
  )
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
      ...(requestHeaders && { requestHeaders }),
    })
    expect(created.status).toBe('published')
  })
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

test("A workspace owner reads the saved values their Program's change replaces, approves it, and Coral publishes it", async (context) => {
  await withOwnerSession(context, async (session) => {
    const { page, step } = session
    const { api, program, provider } = await weatherProvider(
      session,
      credentialFree({ getForecast: {} }),
      {
        requestHeaders: { 'x-trace': 'on', 'x-tenant': 'acme' },
        approveUpdates: true,
      },
    )
    let paused!: RunUpdate
    await step('The Run waits for you to approve updateProvider', async () => {
      // The call sends one of the two saved headers, so it drops the other.
      const call = {
        namespace: 'weather',
        displayName: 'Forecasts',
        requestHeaders: { 'x-trace': 'on' },
      }
      paused = await pausedForApproval(
        program,
        await program.exec(`return await coral.workspace.updateProvider(${JSON.stringify(call)});`),
      )
    })
    await step('Review the saved values the call replaces', async () => {
      const review = await openReview(page, paused, 'coral.workspace.updateProvider')
      const changes = review.locator('summary', { hasText: /^Changes \(2\)$/ }).locator('xpath=..')
      await changes.getByText(JSON.stringify(provider.displayName), { exact: true }).waitFor()
    })
    await step('Approve the call, and Coral publishes the change', async () => {
      await page.getByRole('button', { name: 'Approve this call', exact: true }).click()
      await page.getByText('Approved', { exact: true }).waitFor()
      const { update } = await observeRun(program, await program.wait(paused))
      expect(update.final_value).toMatchObject({
        value: { data: { status: 'published', displayName: 'Forecasts' } },
      })
      const published = await api.providers.getProvider({
        name: provider.name,
      })
      expect(published.displayName).toBe('Forecasts')
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
