import { randomUUID } from 'node:crypto'
import {
  auth,
  Client,
  StreamableHTTPClientTransport,
  type OAuthClientProvider,
  type OAuthDiscoveryState,
  type StoredOAuthClientInformation,
  type StoredOAuthTokens,
} from '@modelcontextprotocol/client'
import { expect, test } from 'vitest'
import { authenticatedTarget, loginActor } from '../../../support/actors.ts'
import { withBrowserSession } from '../../../support/browser-session.ts'
import { managementClients } from '../../../support/management.ts'

// Same public-client profile as pi-mcp-adapter 2.34.0 / SDK 2.0.0. All
// credential state is ephemeral memory; nothing touches an OS credential store.
function clientProvider(redirect: string, registeredRedirect = redirect) {
  let information: StoredOAuthClientInformation | undefined
  let tokens: StoredOAuthTokens | undefined
  let discovery: OAuthDiscoveryState | undefined
  let verifier = ''
  let authorization: URL | undefined
  const state = randomUUID()
  const provider: OAuthClientProvider = {
    redirectUrl: redirect,
    clientMetadata: {
      client_name: 'pi-mcp-adapter',
      redirect_uris: [registeredRedirect],
      grant_types: ['authorization_code', 'refresh_token'],
      response_types: ['code'],
      token_endpoint_auth_method: 'none',
    },
    state: () => state,
    discoveryState: () => discovery,
    saveDiscoveryState: (value) => {
      discovery = value
    },
    clientInformation: () => information,
    saveClientInformation: (value) => {
      information = value
    },
    tokens: () => tokens,
    saveTokens: (value) => {
      tokens = value
    },
    codeVerifier: () => verifier,
    saveCodeVerifier: (value) => {
      verifier = value
    },
    redirectToAuthorization: (value) => {
      authorization = value
    },
  }
  return {
    provider,
    state,
    authorization: () => {
      if (!authorization) throw new Error('SDK did not start authorization')
      return authorization
    },
  }
}

test('URL-only MCP client registers, obtains consent, signs in, and uses the exact Workspace', async (context) => {
  const target = authenticatedTarget()
  const owner = await loginActor(context, target.authentication.owner)
  const api = managementClients(owner.accessToken)
  const workspace = await api.workspaces.createWorkspace({
    displayName: 'DCR Workspace',
    requestId: randomUUID(),
  })
  const other = await api.workspaces.createWorkspace({
    displayName: 'Other DCR Workspace',
    requestId: randomUUID(),
  })
  const resource = `${target.mcpURL}/${workspace.name}/mcp`
  const callbackOrigin = target.authentication.oidcURL
  const redirect = `${callbackOrigin}/fixture/callback`
  // Register a different loopback port, then authorize the actual OS-selected
  // callback. The code exchange must still bind to the actual requested port.
  const client = clientProvider(redirect, 'http://127.0.0.1:1/fixture/callback')
  await withBrowserSession(
    context,
    async ({ page, step }) => {
      await step('Discover Coral from only the Workspace MCP URL', async () => {
        // Reproduce the reported SDK failure against Coral's real discovery,
        // removing only the new advertisement as a pre-fix negative control.
        await expect(
          auth(clientProvider(redirect).provider, {
            serverUrl: resource,
            fetchFn: async (input, init) => {
              const response = await fetch(input, init)
              if (new URL(response.url).pathname === '/.well-known/oauth-authorization-server') {
                const metadata = await response.json()
                delete metadata.registration_endpoint
                return Response.json(metadata)
              }
              return response
            },
          }),
        ).rejects.toThrow('Incompatible auth server: does not support dynamic client registration')
        expect(await auth(client.provider, { serverUrl: resource })).toBe('REDIRECT')
        const registered = await client.provider.clientInformation()
        expect(registered?.client_id).toMatch(/^dcr_[a-f0-9]{32}$/)
        expect(registered).toMatchObject({
          grant_types: ['authorization_code'],
          token_endpoint_auth_method: 'none',
          scope: '',
        })
        expect(registered).not.toHaveProperty('client_secret')
        expect(client.authorization().searchParams.get('resource')).toBe(resource)
        expect(client.authorization().searchParams.get('code_challenge_method')).toBe('S256')
      })
      await step('Review the self-asserted client and allow sign-in', async () => {
        await page.goto(client.authorization().href)
        await page.getByRole('heading', { name: /^Connect .+ to Coral$/ }).waitFor()
        await page.getByRole('button', { name: 'Connect', exact: true }).click()
        await page
          .getByRole('heading', { name: 'Synthetic identity provider', exact: true })
          .waitFor()
        await page.locator(`button[value="${owner.subject}"]`).click()
        await page.waitForURL(
          (url) => url.origin === callbackOrigin && url.pathname === '/fixture/callback',
        )
        await page.getByRole('heading', { name: 'Fixture authorization received' }).waitFor()
      })
      await step('Exchange the code with the SDK and access Workspace MCP', async () => {
        const result = new URL(page.url())
        expect(result.searchParams.get('state')).toBe(client.state)
        const code = result.searchParams.get('code')
        expect(code).toBeTruthy()
        const correct = {
          grant_type: 'authorization_code',
          code: code!,
          client_id: (await client.provider.clientInformation())!.client_id,
          redirect_uri: redirect,
          resource,
          code_verifier: await client.provider.codeVerifier(),
        }
        for (const mismatch of [
          { client_id: 'dcr_00000000000000000000000000000000' },
          { redirect_uri: 'http://127.0.0.1:1/fixture/callback' },
          { resource: `${target.mcpURL}/${other.name}/mcp` },
          { code_verifier: 'a'.repeat(43) },
        ]) {
          const rejected = await page.request.post(
            `${target.authentication.issuerURL}/oauth/token`,
            { form: { ...correct, ...mismatch } },
          )
          expect(rejected.status()).toBe(400)
          expect(await rejected.json()).toMatchObject({ error: 'invalid_grant' })
        }
        // Rejected bindings do not consume the code; the one correct exchange does.
        expect(await auth(client.provider, { serverUrl: resource, authorizationCode: code! })).toBe(
          'AUTHORIZED',
        )
        const tokens = await client.provider.tokens()
        expect(tokens).not.toHaveProperty('refresh_token')
        const claims = JSON.parse(
          Buffer.from(tokens!.access_token.split('.')[1], 'base64url').toString(),
        )
        expect(claims.sub).toBe(owner.userId)
        expect(claims.aud).toBe(resource)
        const mcp = new Client({ name: 'DCR regression client', version: '1' })
        try {
          await mcp.connect(
            new StreamableHTTPClientTransport(new URL(resource), { authProvider: client.provider }),
          )
          const tools = await mcp.listTools()
          expect(tools.tools.map((tool) => tool.name)).toContain('exec')
        } finally {
          await mcp.close()
        }
        const refused = await fetch(`${target.mcpURL}/${other.name}/mcp`, {
          headers: { Authorization: `Bearer ${tokens!.access_token}` },
        })
        expect(refused.status).toBe(401)
        const replay = await page.request.post(`${target.authentication.issuerURL}/oauth/token`, {
          form: correct,
        })
        expect(replay.status()).toBe(400)
        expect(await replay.json()).toMatchObject({ error: 'invalid_grant' })
      })
    },
    { actor: owner.label, label: 'URL-only MCP sign-in' },
  )
})

test('URL-only MCP client connects at /mcp, picks a workspace, and acts only there', async (context) => {
  const target = authenticatedTarget()
  const owner = await loginActor(context, target.authentication.owner)
  const api = managementClients(owner.accessToken)
  const suffix = randomUUID().slice(0, 8)
  const picked = await api.workspaces.createWorkspace({
    displayName: `Picked ${suffix}`,
    requestId: randomUUID(),
  })
  const unpicked = await api.workspaces.createWorkspace({
    displayName: `Unpicked ${suffix}`,
    requestId: randomUUID(),
  })
  const resource = `${target.mcpURL}/mcp`
  const callbackOrigin = target.authentication.oidcURL
  const redirect = `${callbackOrigin}/fixture/callback`
  const client = clientProvider(redirect)
  await withBrowserSession(
    context,
    async ({ page, step }) => {
      let accessToken = ''
      await step('Discover Coral from only the fixed /mcp URL', async () => {
        expect(await auth(client.provider, { serverUrl: resource })).toBe('REDIRECT')
        expect(client.authorization().searchParams.get('resource')).toBe(resource)
      })
      await step('Sign in, then pick a workspace and allow access', async () => {
        await page.goto(client.authorization().href)
        await page
          .getByRole('heading', { name: 'Synthetic identity provider', exact: true })
          .waitFor()
        await page.locator(`button[value="${owner.subject}"]`).click()
        await page.getByRole('heading', { name: /^Connect .+ to Coral$/ }).waitFor()
        await page.getByRole('radio', { name: new RegExp(`^Picked ${suffix}`) }).check()
        await page.getByRole('button', { name: `Connect to Picked ${suffix}`, exact: true }).click()
        await page.waitForURL(
          (url) => url.origin === callbackOrigin && url.pathname === '/fixture/callback',
        )
      })
      await step('The token carries the picked workspace and works only at /mcp', async () => {
        const result = new URL(page.url())
        expect(result.searchParams.get('state')).toBe(client.state)
        const code = result.searchParams.get('code')!
        expect(await auth(client.provider, { serverUrl: resource, authorizationCode: code })).toBe(
          'AUTHORIZED',
        )
        accessToken = (await client.provider.tokens())!.access_token
        const claims = JSON.parse(Buffer.from(accessToken.split('.')[1], 'base64url').toString())
        expect(claims).toMatchObject({
          sub: owner.userId,
          aud: resource,
          workspace_id: picked.name.replace('workspaces/', ''),
        })
        const mcp = new Client({ name: 'Fixed URL client', version: '1' })
        try {
          await mcp.connect(
            new StreamableHTTPClientTransport(new URL(resource), { authProvider: client.provider }),
          )
          expect(mcp.getServerVersion()?.title).toBe(`Coral · Picked ${suffix}`)
          const tools = await mcp.listTools()
          expect(tools.tools.map((tool) => tool.name)).toContain('exec')
        } finally {
          await mcp.close()
        }
        for (const workspace of [picked, unpicked]) {
          const refused = await fetch(`${target.mcpURL}/${workspace.name}/mcp`, {
            headers: { Authorization: `Bearer ${accessToken}` },
          })
          expect(refused.status, workspace.displayName).toBe(401)
          await refused.body?.cancel()
        }
      })
      await step('Archiving the workspace sends the client back to sign in', async () => {
        const current = await api.workspaces.getWorkspace({ name: picked.name })
        await api.workspaces.archiveWorkspace({ name: picked.name, etag: current.etag })
        const lost = await fetch(resource, {
          method: 'POST',
          headers: {
            Authorization: `Bearer ${accessToken}`,
            Accept: 'application/json, text/event-stream',
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            jsonrpc: '2.0',
            id: 1,
            method: 'initialize',
            params: {
              protocolVersion: '2025-11-25',
              capabilities: {},
              clientInfo: { name: 'Fixed URL client', version: '1' },
            },
          }),
        })
        expect(lost.status).toBe(401)
        expect(lost.headers.get('www-authenticate')).toContain('error="invalid_token"')
        await lost.body?.cancel()
      })
    },
    { actor: owner.label, label: 'Fixed /mcp sign-in' },
  )
})
