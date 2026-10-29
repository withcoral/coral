// Disposable upstream servers; callers use real Coral transports. Loopback HTTP
// is the native default. Container runs advertise private DNS hostnames.
import { createHash, X509Certificate } from 'node:crypto'
import { once } from 'node:events'
import { isIP } from 'node:net'
import { readFile } from 'node:fs/promises'
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http'
import { createServer as createSecureServer } from 'node:https'
import { createSecureContext } from 'node:tls'
import type { TestContext } from 'vitest'
import { loopbackFixtures, type FixtureAddress } from './config.ts'

export interface Fixture {
  origin: string
  server: Server
}

async function fixtureTLS(address: FixtureAddress) {
  if (!address.tls) return undefined
  try {
    const [cert, key, ca] = await Promise.all([
      readFile(address.tls.certFile),
      readFile(address.tls.keyFile),
      readFile(address.tls.caFile),
    ])
    const certificate = new X509Certificate(cert)
    if (
      !(isIP(address.advertisedHost)
        ? certificate.checkIP(address.advertisedHost)
        : certificate.checkHost(address.advertisedHost)) ||
      Date.parse(certificate.validFrom) > Date.now() ||
      Date.parse(certificate.validTo) <= Date.now()
    )
      throw new Error()
    if (!new X509Certificate(ca).ca) throw new Error()
    createSecureContext({ cert, key, ca })
    return { cert, key }
  } catch {
    // Do not serialize certificate paths, key contents or OpenSSL diagnostics.
    throw new Error(
      'Fixture TLS requires readable certificate/key/CA files and a valid certificate for the advertised host',
    )
  }
}

export async function validateFixtureTLS(address: FixtureAddress) {
  await fixtureTLS(address)
}

export async function listen(
  test: Pick<TestContext, 'onTestFinished'>,
  server: Server,
  settings?: FixtureAddress,
): Promise<Fixture> {
  // Vitest injection is the bootstrap seam; fixtures never read process env.
  const address = settings ?? (await import('vitest')).inject('reel').fixtures ?? loopbackFixtures
  const tls = await fixtureTLS(address)
  if (tls) {
    const http = server
    server = createSecureServer(tls, (request, response) => http.emit('request', request, response))
  }
  test.onTestFinished(
    () =>
      new Promise<void>((resolve) => {
        server.closeAllConnections()
        server.close(() => resolve())
      }),
  )
  // Coral reuses its connections. Node closes an idle one after five seconds,
  // and a request sent as it closes fails; the fixture closes them at teardown.
  server.keepAliveTimeout = 0
  server.listen(0, address.bindHost)
  await once(server, 'listening')
  const bound = server.address()
  if (!bound || typeof bound === 'string') throw new Error('missing fixture address')
  return {
    origin: `${tls ? 'https' : 'http'}://${address.advertisedHost}:${bound.port}`,
    server,
  }
}

async function body(request: IncomingMessage) {
  let text = ''
  for await (const chunk of request) text += chunk
  return text
}

function json(response: ServerResponse, status: number, value: unknown, headers = {}) {
  response
    .writeHead(status, { 'content-type': 'application/json', ...headers })
    .end(JSON.stringify(value))
}

/**
 * Public HTTPS declarations need no live server; callers that dispatch supply a fixture origin.
 * Tools are GET, which runs without approval by default, unless they set `method`.
 */
export function openApiDocument(
  title: string,
  tools: Record<
    string,
    { summary?: string; tags?: string[]; broken?: boolean; method?: 'get' | 'post' }
  >,
  options: {
    server?: string
    security?: 'bearer' | 'oauth'
    /** The origin whose /authorize and /token the OAuth flow uses; a placeholder otherwise. */
    oauthOrigin?: string
  } = {},
) {
  const document: Record<string, unknown> = {
    openapi: '3.1.0',
    info: { title, version: '1' },
    servers: [{ url: options.server ?? 'https://api.example.test' }],
    paths: Object.fromEntries(
      Object.entries(tools).map(([id, o]) => [
        `/${id}`,
        {
          [o.method ?? 'get']: {
            operationId: id,
            summary: o.summary,
            tags: o.tags,
            ...(o.broken ? {} : { responses: { '200': { description: 'ok' } } }),
          },
        },
      ]),
    ),
  }
  if (options.security === 'bearer') {
    document.security = [{ bearer: [] }]
    document.components = {
      securitySchemes: { bearer: { type: 'http', scheme: 'bearer' } },
    }
  }
  if (options.security === 'oauth') {
    const origin = options.oauthOrigin ?? 'https://auth.example.test'
    document.security = [{ account: ['read', 'write'] }]
    document.components = {
      securitySchemes: {
        account: {
          type: 'oauth2',
          flows: {
            authorizationCode: {
              authorizationUrl: `${origin}/authorize`,
              tokenUrl: `${origin}/token`,
              scopes: { read: 'Read records', write: 'Write records' },
            },
          },
        },
      },
    }
  }
  return Buffer.from(JSON.stringify(document))
}

/**
 * RFC 8628 device authorization for both OAuth fixtures: `/device` issues one
 * code and `/activate` is the page where the person approves it.
 */
function deviceAuthorization(deviceCode: string, expiresIn: number) {
  let approved = false
  return {
    get approved() {
      return approved
    },
    issue(response: ServerResponse, origin: string) {
      json(response, 200, {
        device_code: deviceCode,
        user_code: 'ABCD-EFGH',
        verification_uri: `${origin}/activate`,
        expires_in: expiresIn,
        interval: 1,
      })
    },
    activate(url: URL, response: ServerResponse) {
      if (url.searchParams.get('approve') === 'yes') approved = true
      response
        .writeHead(200, { 'content-type': 'text/html' })
        .end(
          approved
            ? '<h1>Device approved</h1>'
            : '<h1>Approve ABCD-EFGH</h1><a href="/activate?approve=yes">Approve device access</a>',
        )
    },
  }
}

/**
 * An API whose OpenAPI document signs in with OAuth: consent, token, and the
 * API itself on one origin. The token endpoint expects the public client
 * "manual-client" with S256 PKCE and no resource binding.
 */
export async function oauthApiServer(
  test: TestContext,
  access: string,
  knobs: {
    /** The `scope` the token endpoint reports; defaults to the request. */
    reportedScope?: string
    clientSecret?: string
    nonRefreshable?: boolean
    device?: boolean
    tokenError?: string
    /** Where the fixture listens; local mode reaches only loopback services. */
    address?: FixtureAddress
  } = {},
) {
  const counts = { tokens: 0, consents: 0, calls: 0 }
  let origin = ''
  let lastConsent: URL | undefined
  const device = deviceAuthorization('private-device-canary', 60)
  const server = createServer(async (request, response) => {
    const url = new URL(request.url!, origin)
    if (knobs.device && url.pathname === '/device') return device.issue(response, origin)
    if (knobs.device && url.pathname === '/activate') {
      counts.consents++
      return device.activate(url, response)
    }
    if (url.pathname === '/authorize') {
      counts.consents++
      lastConsent = url
      const callback = new URL(url.searchParams.get('redirect_uri')!)
      callback.searchParams.set('state', url.searchParams.get('state')!)
      callback.searchParams.set('code', 'disposable-code')
      const denied = new URL(callback)
      denied.searchParams.delete('code')
      denied.searchParams.set('error', 'access_denied')
      response
        .writeHead(200, { 'content-type': 'text/html' })
        .end(
          `<h1>Fixture sign-in</h1><a href="${callback.toString().replaceAll('&', '&amp;')}">Approve fixture access</a><a href="${denied.toString().replaceAll('&', '&amp;')}">Deny fixture access</a>`,
        )
      return
    }
    if (url.pathname === '/token') {
      counts.tokens++
      if (knobs.tokenError) {
        json(response, 200, {
          error: knobs.tokenError,
          error_description: 'private-upstream-description-canary',
          error_uri: 'https://private-upstream-uri-canary.test',
        })
        return
      }
      const form = new URLSearchParams(await body(request))
      if (
        knobs.clientSecret &&
        (form.get('client_secret') !== knobs.clientSecret || request.headers.authorization)
      ) {
        json(response, 401, { error: 'invalid_client' })
        return
      }
      if (knobs.device && !device.approved) {
        // GitHub-shaped: some services send OAuth errors with HTTP 200. The MCP
        // fixture below sends the RFC 8628 status (400), so both shapes stay covered.
        json(response, 200, { error: 'authorization_pending' })
        return
      }
      const valid = knobs.device
        ? form.get('grant_type') === 'urn:ietf:params:oauth:grant-type:device_code' &&
          form.get('device_code') === 'private-device-canary' &&
          form.get('client_id') === 'manual-client' &&
          !form.has('code_verifier') &&
          !form.has('redirect_uri')
        : form.get('grant_type') === 'authorization_code' &&
          form.get('code') === 'disposable-code' &&
          form.get('client_id') === 'manual-client' &&
          form.get('resource') === null &&
          createHash('sha256')
            .update(form.get('code_verifier') ?? '')
            .digest('base64url') === lastConsent?.searchParams.get('code_challenge')
      if (!valid) {
        json(response, 400, { error: 'invalid_grant' })
        return
      }
      const token = {
        access_token: access,
        token_type: 'Bearer',
        expires_in: 3600,
        ...(knobs.nonRefreshable ? {} : { refresh_token: `${access}-refresh` }),
        scope: knobs.reportedScope ?? 'read write',
      }
      if (request.headers.accept === 'application/json') {
        json(response, 200, token)
      } else {
        response
          .writeHead(200, {
            'content-type': 'application/x-www-form-urlencoded',
          })
          .end(
            new URLSearchParams(
              Object.entries(token).map(([key, value]) => [key, String(value)]),
            ).toString(),
          )
      }
      return
    }
    if (url.pathname === '/listRecords') {
      if (request.headers.authorization !== `Bearer ${access}`) {
        response.writeHead(401, { 'www-authenticate': 'Bearer' }).end()
        return
      }
      counts.calls++
      json(response, 200, { records: ['Hello from the OAuth API'] })
      return
    }
    response.writeHead(404).end()
  })
  const fixture = await listen(test, server, knobs.address)
  origin = fixture.origin
  return {
    ...fixture,
    counts,
    document: (title: string) =>
      openApiDocument(
        title,
        { listRecords: { summary: 'List records' } },
        { server: origin, security: 'oauth', oauthOrigin: origin },
      ),
    get lastConsent() {
      return lastConsent
    },
  }
}

/** A Streamable HTTP MCP server that needs a bearer token before it lists anything. */
export async function bearerMcpServer(
  test: TestContext,
  token: string,
  anonymous = false,
  emptyCatalog = false,
) {
  const counts = { discovery: 0, calls: 0, credentialRequests: 0 }
  const server = createServer(async (request, response) => {
    if (request.method === 'GET') {
      response.writeHead(404).end()
      return
    }
    if (request.headers.authorization) counts.credentialRequests++
    if (!anonymous && request.headers.authorization !== `Bearer ${token}`) {
      response.writeHead(401, { 'www-authenticate': 'Bearer' }).end()
      return
    }
    if (request.method === 'DELETE') {
      response.writeHead(204).end()
      return
    }
    const message = JSON.parse(await body(request))
    if (!('id' in message)) {
      response.writeHead(202).end()
      return
    }
    let result: unknown
    if (message.method === 'initialize')
      result = {
        protocolVersion: '2025-11-25',
        serverInfo: {
          name: 'bearer-fixture',
          title: 'Bearer fixture',
          version: '1',
        },
        capabilities: { tools: {} },
      }
    else if (message.method === 'tools/list') {
      counts.discovery++
      result = {
        tools: emptyCatalog
          ? []
          : [
              {
                name: 'hello',
                description: 'Read a greeting',
                inputSchema: { type: 'object', properties: {} },
                annotations: { readOnlyHint: true },
              },
            ],
      }
    } else {
      counts.calls++
      result = {
        content: [{ type: 'text', text: 'Hello from the bearer fixture' }],
      }
    }
    json(response, 200, { jsonrpc: '2.0', id: message.id, result })
  })
  return {
    ...(await listen(test, server)),
    counts,
    acceptToken(next: string) {
      token = next
      anonymous = false
    },
  }
}

export interface OAuthKnobs {
  /** Runtime authorization lifetime; refresh issues a one-hour token. */
  expiresIn: number
  /** Refreshed material differs from the authorization grant. */
  refreshAccess?: string
  /** Exercise material expiring again before the final dispatch fence. */
  firstRefreshExpiresIn?: number
  /** Reject authorization exchanges without changing the active grant. */
  tokenError?: string
  /** Later consent issues a different credential and rejects the discovery credential. */
  replacementAccess?: string
  /** Changes OAuth authority while retaining the same consent/token endpoints. */
  issuerPath?: string
  /** Extra scopes reported by initial consent without changing the requested contract. */
  additionalScopes?: string[]
  /** Delivers a discovered catalog that Publication must reject after authorization. */
  invalidCatalog?: boolean
  /** Advertises and serves device authorization. */
  device?: boolean
  /** Rejects device requests whose outgoing scope value differs. */
  expectedDeviceScope?: string
  /** Required on MCP requests that carry a token. */
  requiredServiceHeaders?: Record<string, string>
  /** tools/list answers 503 while set. */
  discoveryBroken: boolean
  /** The authorization server advertises Client ID Metadata Document support. */
  cimd: boolean
  /** The authorization server advertises a registration endpoint. */
  dcr: boolean
}

/**
 * An MCP server that answers discovery with a Bearer challenge and serves
 * protected-resource metadata, authorization-server metadata, registration,
 * consent, and token endpoints on one origin. Consent is a page with one link.
 */
export async function oauthMcpServer(
  test: TestContext,
  access: string,
  knobs: Partial<OAuthKnobs> = {},
) {
  const state: OAuthKnobs = {
    expiresIn: 3600,
    discoveryBroken: false,
    cimd: false,
    dcr: true,
    ...knobs,
  }
  const counts = {
    tokens: 0,
    refreshes: 0,
    registrations: 0,
    discovery: 0,
    calls: 0,
    consents: 0,
  }
  let origin = ''
  const device = deviceAuthorization('mcp-device-canary', 120)
  let lastConsent: URL | undefined
  let currentAccess = access
  let currentRefresh = `${access}-refresh`
  const server = createServer(async (request, response) => {
    const url = new URL(request.url!, origin)
    if (
      url.pathname === '/mcp' &&
      request.headers.authorization &&
      Object.entries(state.requiredServiceHeaders ?? {}).some(
        ([name, value]) => request.headers[name.toLowerCase()] !== value,
      )
    ) {
      json(response, 400, { error: 'missing_service_header' })
      return
    }
    if (state.device && url.pathname === '/device') {
      const form = new URLSearchParams(await body(request))
      if (
        form.get('resource') !== `${origin}/mcp` ||
        !form.get('client_id') ||
        (state.expectedDeviceScope !== undefined && form.get('scope') !== state.expectedDeviceScope)
      ) {
        json(response, 400, { error: 'invalid_request' })
        return
      }
      return device.issue(response, origin)
    }
    if (state.device && url.pathname === '/activate') return device.activate(url, response)
    if (url.pathname === '/.well-known/oauth-protected-resource/mcp') {
      json(response, 200, {
        resource: `${origin}/mcp`,
        authorization_servers: [`${origin}${state.issuerPath ?? ''}`],
        scopes_supported: ['read', 'write'],
      })
      return
    }
    if (url.pathname === `/.well-known/oauth-authorization-server${state.issuerPath ?? ''}`) {
      json(response, 200, {
        issuer: `${origin}${state.issuerPath ?? ''}`,
        authorization_endpoint: `${origin}/authorize`,
        token_endpoint: `${origin}/token`,
        code_challenge_methods_supported: ['S256'],
        ...(state.device ? { device_authorization_endpoint: `${origin}/device` } : {}),
        client_id_metadata_document_supported: state.cimd,
        scopes_supported: ['read', 'write'],
        ...(state.dcr ? { registration_endpoint: `${origin}/register` } : {}),
      })
      return
    }
    if (url.pathname === '/register') {
      counts.registrations++
      const registration = JSON.parse(await body(request))
      json(response, 201, {
        client_id: 'dcr-client',
        token_endpoint_auth_method: 'none',
        redirect_uris: registration.redirect_uris,
      })
      return
    }
    if (url.pathname === '/authorize') {
      counts.consents++
      lastConsent = url
      const callback = new URL(url.searchParams.get('redirect_uri')!)
      callback.searchParams.set('state', url.searchParams.get('state')!)
      callback.searchParams.set('code', 'disposable-code')
      response
        .writeHead(200, { 'content-type': 'text/html' })
        .end(
          `<h1>Fixture sign-in</h1><a href="${callback.toString().replaceAll('&', '&amp;')}">Approve fixture access</a>`,
        )
      return
    }
    if (url.pathname === '/token') {
      counts.tokens++
      const form = new URLSearchParams(await body(request))
      if (form.get('grant_type') === 'refresh_token') {
        counts.refreshes++
        if (
          form.get('refresh_token') !== currentRefresh ||
          form.get('resource') !== `${origin}/mcp` ||
          !form.get('client_id')
        ) {
          json(response, 400, { error: 'invalid_grant' })
          return
        }
        currentAccess = state.refreshAccess ?? currentAccess
        currentRefresh = `${currentAccess}-refresh`
        json(response, 200, {
          access_token: currentAccess,
          token_type: 'Bearer',
          expires_in: counts.refreshes === 1 ? (state.firstRefreshExpiresIn ?? 3600) : 3600,
          refresh_token: currentRefresh,
          scope: 'read write',
        })
        return
      }
      if (state.tokenError) {
        json(response, 400, { error: state.tokenError })
        return
      }
      if (state.device && !device.approved) {
        json(response, 400, { error: 'authorization_pending' })
        return
      }
      const valid = state.device
        ? form.get('grant_type') === 'urn:ietf:params:oauth:grant-type:device_code' &&
          form.get('device_code') === 'mcp-device-canary' &&
          form.get('resource') === `${origin}/mcp` &&
          Boolean(form.get('client_id'))
        : form.get('grant_type') === 'authorization_code' &&
          form.get('code') === 'disposable-code' &&
          form.get('resource') === `${origin}/mcp` &&
          Boolean(form.get('client_id')) &&
          createHash('sha256')
            .update(form.get('code_verifier') ?? '')
            .digest('base64url') === lastConsent?.searchParams.get('code_challenge')
      if (!valid) {
        json(response, 400, { error: 'invalid_grant' })
        return
      }
      currentAccess = counts.tokens > 1 ? (state.replacementAccess ?? access) : access
      currentRefresh = `${currentAccess}-refresh`
      json(response, 200, {
        access_token: currentAccess,
        token_type: 'Bearer',
        expires_in: state.expiresIn,
        refresh_token: currentRefresh,
        scope: ['read', 'write', ...(state.additionalScopes ?? [])].join(' '),
      })
      return
    }
    if (url.pathname === '/mcp') {
      if (request.headers.authorization !== `Bearer ${currentAccess}`) {
        response
          .writeHead(401, {
            'www-authenticate': `Bearer realm="OAuth", resource_metadata="${origin}/.well-known/oauth-protected-resource/mcp", scope="read write"`,
          })
          .end()
        return
      }
      if (request.method === 'DELETE') {
        response.writeHead(204).end()
        return
      }
      const message = JSON.parse(await body(request))
      if (!('id' in message)) {
        response.writeHead(202).end()
        return
      }
      let result: unknown
      if (message.method === 'initialize')
        result = {
          protocolVersion: '2025-11-25',
          serverInfo: {
            name: 'oauth-fixture',
            title: 'OAuth fixture',
            version: '1',
          },
          capabilities: { tools: {} },
        }
      else if (message.method === 'tools/list') {
        counts.discovery++
        if (state.discoveryBroken) {
          response.writeHead(503).end('outage-body-canary')
          return
        }
        result = {
          tools: [
            {
              name: 'lookup',
              description: 'Look something up',
              inputSchema: { type: 'object', properties: {} },
              annotations: { readOnlyHint: true },
            },
            ...(state.invalidCatalog ? [{ name: 'lookup', inputSchema: { type: 'object' } }] : []),
          ],
        }
      } else {
        counts.calls++
        result = {
          content: [{ type: 'text', text: 'Hello from the OAuth fixture' }],
        }
      }
      json(response, 200, { jsonrpc: '2.0', id: message.id, result })
      return
    }
    response.writeHead(404).end()
  })
  const fixture = await listen(test, server)
  origin = fixture.origin
  return {
    ...fixture,
    knobs: state,
    counts,
    mcpUrl: `${origin}/mcp`,
    get lastConsent() {
      return lastConsent
    },
  }
}
