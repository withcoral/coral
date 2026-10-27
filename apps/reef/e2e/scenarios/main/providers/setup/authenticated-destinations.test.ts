import { loopbackFixtures } from '../../../../support/config.ts'
import { createServer } from 'node:http'
import { Code } from '@connectrpc/connect'
import { expect, test, type TestContext } from 'vitest'
import {
  OAuthScopeFormat,
  SetupAuthorizationState,
} from '../../../../generated/coral/management/v1/provider_pb.ts'
import { authenticatedTarget, loginActor } from '../../../../support/actors.ts'
import { withBrowserSession } from '../../../../support/browser-session.ts'
import { managementClients } from '../../../../support/management.ts'
import { listen, openApiDocument } from '../../../../support/provider-fixtures.ts'
import {
  acceptTools,
  addProviderFromFile,
  chooseAccount,
  chooseFile,
  clearScopes,
  configurationNext,
  confirmConfiguration,
  createWorkspace,
  destinationField,
  ensureNamespace,
  oauthSettings,
  openCustomSource,
  openTool,
  openProviderSettings,
  requestedScopes,
  scopesField,
  setNamespace,
  waitForProviderStatus,
  waitForPublication,
} from '../../../../support/providers.ts'

type BrowserSession = Parameters<Parameters<typeof withBrowserSession>[1]>[0]

async function session(
  context: TestContext,
  body: (
    browser: BrowserSession,
    api: ReturnType<typeof managementClients>,
    accessToken: string,
  ) => Promise<void>,
) {
  const owner = await loginActor(context, authenticatedTarget().authentication.owner)
  await withBrowserSession(
    context,
    (browser) => body(browser, managementClients(owner.accessToken), owner.accessToken),
    { storageState: owner.storageState, actor: owner.label },
  )
}

async function sink(context: TestContext) {
  let requests = 0
  const fixture = await listen(
    context,
    createServer((_request, response) => {
      requests++
      response.writeHead(500).end()
    }),
    loopbackFixtures,
  )
  return { ...fixture, requests: () => requests }
}

// The person states the kind, so each is refused on its own declared path and
// the deployment's destination policy stops the loopback before either one
// leaves Reef: the sink records no request at all.
test.for(['OpenAPI URL', 'MCP server'])(
  'Authenticated setup rejects a loopback %s before connecting',
  async (source, context) => {
    const upstream = await sink(context)
    await session(context, async ({ page, step }, api, accessToken) => {
      const workspace = await createWorkspace(accessToken, `Blocked source ${crypto.randomUUID()}`)
      await step('Reject the source before discovery or credential entry', async () => {
        await page.goto(`/workspaces/${workspace}/providers/new`)
        await openCustomSource(page)
        await page
          .getByLabel(source === 'MCP server' ? 'MCP server URL' : 'OpenAPI URL', { exact: true })
          .fill(`${upstream.origin}/source`)
        const response = page.waitForResponse(
          (value) => value.request().method() === 'POST' && value.url().includes('/new.data'),
        )
        await page
          .getByRole('button', { name: source === 'MCP server' ? 'Connect' : 'Read', exact: true })
          .click()
        expect((await response).status()).toBe(409)
        await page
          .getByRole('form', { name: 'New source', exact: true })
          .getByText("Coral's outbound settings do not allow this address.", { exact: false })
          .waitFor()
        expect(await configurationNext(page).count()).toBe(0)
        expect(await page.locator('input[type="password"]').count()).toBe(0)
        expect(
          await page
            .getByRole('textbox', {
              name: source === 'MCP server' ? 'MCP server URL' : 'OpenAPI URL',
              exact: true,
            })
            .inputValue(),
        ).toBe(`${upstream.origin}/source`)
        expect(upstream.requests()).toBe(0)
        expect(
          (await api.providers.listProviders({ parent: `workspaces/${workspace}` })).providers,
        ).toHaveLength(1)
      })
    })
  },
)

test('Authenticated configuration rejects a loopback server and accepts a public correction', async (context) => {
  const upstream = await sink(context)
  await session(context, async ({ page, step }, api, accessToken) => {
    const workspace = await createWorkspace(
      accessToken,
      `Blocked declaration ${crypto.randomUUID()}`,
    )
    await step('Report the rejected destination before publication', async () => {
      await page.goto(`/workspaces/${workspace}/providers/new`)
      const document = JSON.parse(
        openApiDocument('Blocked API', { hello: {} }, { server: upstream.origin }).toString(),
      )
      document.security = []
      await chooseFile(page, 'blocked.json', Buffer.from(JSON.stringify(document)))
      await page.getByRole('button', { name: 'Read', exact: true }).click()
      await confirmConfiguration(page, 'No authentication')
      await page.getByText('outbound_destination_rejected', { exact: true }).waitFor()
      expect(
        (await api.providers.listProviders({ parent: `workspaces/${workspace}` })).providers,
      ).toHaveLength(1)
      expect(upstream.requests()).toBe(0)
    })
    await step(
      'Correct the declaration and publish without contacting the rejected server',
      async () => {
        // The rejection shows on the tools step, whose Back returns to the
        // configuration that declared the refused server.
        await page.getByRole('link', { name: 'Back', exact: true }).click()
        await destinationField(page, 'Main server').fill('https://api.example.test')
        await page.keyboard.press('Escape')
        await confirmConfiguration(page, 'No authentication')
        await acceptTools(page)
        await setNamespace(page, 'corrected')
        await page.getByRole('button', { name: 'Add provider', exact: true }).click()
        await waitForPublication(page)
        expect(upstream.requests()).toBe(0)
      },
    )
  })
})

test('Authenticated update rejects loopback MCP discovery and preserves the existing provider', async (context) => {
  const upstream = await sink(context)
  await session(context, async ({ page, step }, api, accessToken) => {
    const workspace = await createWorkspace(accessToken, `Blocked update ${crypto.randomUUID()}`)
    const providerPath = await addProviderFromFile(
      page,
      workspace,
      openApiDocument('Existing API', { hello: {} }, { security: 'bearer' }),
      'existing',
      { account: 'shared', secret: 'fixture-shared-account' },
    )
    const parent = `workspaces/${workspace}`
    const before = (await api.providers.listProviders({ parent })).providers
    expect(before).toHaveLength(2)
    await step('Refuse the replacement source before reaching its server', async () => {
      await openProviderSettings(page)
      await page.getByRole('link', { name: 'Replace source', exact: true }).click()
      await openCustomSource(page)
      await page.getByLabel('MCP server URL', { exact: true }).fill(`${upstream.origin}/mcp`)
      const response = page.waitForResponse(
        (value) => value.request().method() === 'POST' && value.url().includes('/update.data'),
      )
      await page.getByRole('button', { name: 'Connect', exact: true }).click()
      expect((await response).status()).toBe(409)
      await page
        .getByRole('form', { name: 'New source', exact: true })
        .getByText("Coral's outbound settings do not allow this address.", { exact: false })
        .waitFor()
      expect(upstream.requests()).toBe(0)
      expect((await api.providers.listProviders({ parent })).providers).toEqual(before)
    })
    await step('Return to the unchanged provider and its shared account', async () => {
      await page.goto(providerPath)
      await waitForProviderStatus(page, 'Existing API', 'Ready', { available: 1, total: 1 })
      // The tool's full name keeps the original namespace.
      const drawer = await openTool(page, 'hello')
      await drawer.getByText('coral.providers.existing.hello', { exact: true }).waitFor()
      await page.goto(`${providerPath}/settings`)
      await page.getByText('People and agents use this shared account.', { exact: true }).waitFor()
      await page
        .getByRole('region', { name: 'One shared account', exact: true })
        .getByText('Connected', { exact: true })
        .waitFor()
    })
  })
})

test('Authenticated OAuth configuration retains typed scopes through field errors and publication', async (context) => {
  await session(context, async ({ page, step }, api, accessToken) => {
    const workspace = await createWorkspace(accessToken, `Public scopes ${crypto.randomUUID()}`)
    let setup = ''
    await step('Retain typed scopes and headers while correcting the token URL', async () => {
      await page.goto(`/workspaces/${workspace}/providers/new`)
      await chooseFile(
        page,
        'scopes.json',
        openApiDocument('Public OAuth', { hello: {} }, { security: 'oauth' }),
      )
      await page.getByRole('button', { name: 'Read', exact: true }).click()
      await clearScopes(page)
      // A comma ends a scope too; the chosen separator applies only when Coral sends them.
      await scopesField(page).pressSequentially('read,write')
      await expect.poll(() => requestedScopes(page)).toEqual(['read'])
      // Leaving the field adds the scope still being typed and closes its suggestions.
      await scopesField(page).press('Tab')
      await expect.poll(() => requestedScopes(page)).toEqual(['read', 'write'])
      await page.getByRole('button', { name: 'Send scopes separated by', exact: true }).click()
      await page.getByRole('menuitemradio', { name: 'Commas', exact: true }).click()
      await page.getByRole('button', { name: 'Add request header', exact: true }).click()
      await page.getByLabel('Request header 1', { exact: true }).fill('x-service-version')
      await page.getByLabel('Request header value 1', { exact: true }).fill('2022-11-28')
      await page.getByLabel('Client ID', { exact: true }).fill('manual-client')
      await page.getByLabel('Token URL', { exact: true }).fill('not-a-url')
      await ensureNamespace(page)
      await page.getByRole('button', { name: 'Next', exact: true }).click()
      await page
        .getByText('Enter a valid token URL without credentials or OAuth request parameters.', {
          exact: true,
        })
        .waitFor()
      expect(await requestedScopes(page)).toEqual(['read', 'write'])
      expect(
        await page
          .getByRole('button', { name: 'Send scopes separated by', exact: true })
          .textContent(),
      ).toContain('Commas')
      expect(await page.getByLabel('Request header value 1', { exact: true }).inputValue()).toBe(
        '2022-11-28',
      )
      expect(await page.getByLabel('Client ID', { exact: true }).inputValue()).toBe('manual-client')
      expect(await page.getByLabel('Authorization URL', { exact: true }).inputValue()).toBe(
        'https://auth.example.test/authorize',
      )
      await page.getByLabel('Token URL', { exact: true }).fill('https://auth.example.test/token')
      await confirmConfiguration(page)
      setup = `workspaces/${workspace}/providerSetups/${new URL(page.url()).searchParams.get('setup')}`
    })
    await step(
      'Publish caller accounts with the confirmed public OAuth configuration',
      async () => {
        await page.reload()
        expect(
          oauthSettings(
            (await api.setups.getProviderSetup({ name: setup })).configuration?.confirmed,
          )?.scopes,
        ).toEqual(['read', 'write'])
        const confirmed = (await api.setups.getProviderSetup({ name: setup })).configuration
          ?.confirmed
        expect(oauthSettings(confirmed)?.scopeFormat).toBe(OAuthScopeFormat.COMMA)
        expect(confirmed?.requestHeaders).toEqual({ 'x-service-version': '2022-11-28' })
        await acceptTools(page)
        await setNamespace(page, 'public_scopes')
        await chooseAccount(page, 'own-later')
        await page.getByRole('button', { name: 'Add provider', exact: true }).click()
        await waitForPublication(page)
      },
    )
  })
})

test('Authenticated OAuth application changes cancel pending consent and retain the new client', async (context) => {
  await session(context, async ({ page, target, step }, api, accessToken) => {
    const workspace = await createWorkspace(
      accessToken,
      `Public application ${crypto.randomUUID()}`,
    )
    let setup = ''
    let edit = ''
    await step('Configure a public OAuth application before consent', async () => {
      await page.goto(`/workspaces/${workspace}/providers/new`)
      await chooseFile(
        page,
        'application.json',
        openApiDocument('Public application', { hello: {} }, { security: 'oauth' }),
      )
      await page.getByRole('button', { name: 'Read', exact: true }).click()
      await confirmConfiguration(page)
      setup = `workspaces/${workspace}/providerSetups/${new URL(page.url()).searchParams.get('setup')}`
      edit = new URL(
        (await page.getByRole('link', { name: 'Back', exact: true }).getAttribute('href'))!,
        page.url(),
      ).toString()
    })
    // Arrange pending consent through the real management API; the UI edit is the behavior under test.
    const configured = await api.setups.getProviderSetup({ name: setup })
    const pending = await api.setups.startProviderSetupAuthorization({
      name: setup,
      configurationRevision: configured.configuration!.revision,
      redirectUri: `${target.baseURL}/provider-setup/oauth/callback`,
      application: { mode: { case: 'clientId', value: 'manual-client' } },
    })
    expect(pending.authorization?.state).toBe(SetupAuthorizationState.PENDING)
    const state = new URL(pending.authorization!.consentNavigation!.url).searchParams.get('state')!
    await step(
      'Replace the client and invalidate consent for the previous application',
      async () => {
        await page.goto(edit)
        await page.getByLabel('Client ID', { exact: true }).fill('replacement-client')
        await confirmConfiguration(page)
        await page.reload()
        const updated = await api.setups.getProviderSetup({ name: setup })
        expect(updated.authorization).toBeUndefined()
        expect(updated.authorized).toBe(false)
        expect(oauthSettings(updated.configuration?.confirmed)?.oauthClientId).toBe(
          'replacement-client',
        )
        await expect(
          api.accounts.completeProviderSetupAuthorization({
            query: new URLSearchParams({ state, code: 'fixture-obsolete-code' }).toString(),
          }),
        ).rejects.toMatchObject({
          code: Code.InvalidArgument,
          rawMessage: 'INVALID_PROVIDER_INPUT',
        })
        await acceptTools(page)
        await setNamespace(page, 'public_application')
        await chooseAccount(page, 'own-later')
        await page.getByRole('button', { name: 'Add provider', exact: true }).click()
        await waitForPublication(page)
      },
    )
  })
})
