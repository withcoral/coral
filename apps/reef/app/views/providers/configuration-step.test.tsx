/** @vitest-environment happy-dom */
import { describe, expect, it } from 'vitest'

import { markupRoute } from '@/test-support/route'
import { ConfigurationStep } from './configuration-step'
import type {
  ConfigurationReviewView,
  ConfigurationValues,
  CredentialMethod,
  RecipeConfigurationView,
  SetupView,
  StepperResult,
} from './types'

const review: ConfigurationReviewView = {
  revision: '1',
  confirmed: false,
  authenticationMethods: ['oauth'],
  discoveryAuthorizationRequired: false,
  automaticClientAvailable: true,
  issuer: '',
  resource: '',
  differences: [],
  interruptsAccess: null,
  updating: false,
  values: {
    displayName: 'Example',
    authentication: 'oauth',
    headers: [],
    parameters: [],
    basic: {
      username: { source: 'literal', value: '' },
      password: { source: 'literal', value: '' },
    },
    credentialFields: {},
    authorizationUrl: 'https://example.test/authorize',
    tokenUrl: 'https://example.test/token',
    scopes: [],
    servers: [],
  },
  authenticationStatus: 'supported',
  documentDisplayName: 'Example',
  servers: [],
  destinations: {},
  toolCount: null,
  tools: [],
  unavailableTools: [],
  diagnostic: '',
  readable: true,
  errors: {},
}
const setup: SetupView = {
  id: 'setup-1',
  kind: 'openapi',
  state: 'awaiting_configuration',
  credentialMethod: 'oauth',
  credentialFields: [],
  authorized: false,
  grantedScope: '',
  authorization: null,
  discoveryFailure: '',
  expiresAt: '',
  sourceLabel: 'Uploaded document',
  sourceHost: '',
  serverName: '',
  allowedRoutes: [],
  connectAvailable: false,
  connectLaterAvailable: false,
}

function render(
  configuration: ConfigurationReviewView,
  recipe?: RecipeConfigurationView,
  result?: StepperResult,
  kind: SetupView['kind'] = 'openapi',
  overrides: Partial<SetupView> = {},
  fields: {
    namespace?: string
    takenNamespaces?: string[]
    takenDisplayNames?: string[]
    suggestedName?: string
  } = {},
) {
  return markupRoute(
    <ConfigurationStep
      setup={{ ...setup, ...overrides, kind, configuration }}
      form={{ csrf: 'test-csrf', requestId: 'test-request' }}
      pending={false}
      callbackUrl="https://coral.test/callback"
      recipe={recipe}
      result={result}
      {...fields}
    />,
  )
}

/** The configuration the step submits, as its action parses it. */
function submitted(html: string) {
  const container = document.createElement('div')
  container.innerHTML = html
  const field = container.querySelector<HTMLInputElement>('input[name="configuration"]')!
  return JSON.parse(field.value) as Record<string, unknown>
}

// Catches: a namespace another provider has reaching sign-in and review, where
// only Coral's refusal at the end says it cannot be used.
it.each([
  ['taken', 'github', true],
  ['free', 'github_mcp', false],
])('a %s namespace says so beside the field and blocks Next: %s', (_state, namespace, refused) => {
  const container = document.createElement('div')
  container.innerHTML = render(
    review,
    undefined,
    undefined,
    'openapi',
    {},
    {
      namespace,
      takenNamespaces: ['github'],
    },
  )
  const alerts = [...container.querySelectorAll('[role="alert"]')].map((alert) => alert.textContent)
  expect(alerts).toEqual(
    refused ? ['Another provider uses this namespace. Enter a different namespace.'] : [],
  )
  // The field names its error, so a screen reader reads the reason with the input.
  const described = container
    .querySelector('input[name="namespace"]')!
    .getAttribute('aria-describedby')
  expect(described ? container.querySelector(`#${CSS.escape(described)}`)?.textContent : null).toBe(
    refused ? 'Another provider uses this namespace. Enter a different namespace.' : null,
  )
  const next = [...container.querySelectorAll('button')].find(
    (button) => button.textContent === 'Next',
  )!
  expect(next.getAttribute('aria-disabled')).toBe(refused ? 'true' : null)
})

// Catches: a second provider with another's name, which the provider list then
// shows twice with nothing to tell them apart.
it.each([
  ['the same name', 'GitHub', true],
  ['the name in another case with spaces', ' github ', true],
  ['a different name', 'GitHub MCP', false],
])('a display name with %s says so beside the field and blocks Next', (_case, name, refused) => {
  const container = document.createElement('div')
  container.innerHTML = render(
    review,
    undefined,
    undefined,
    'openapi',
    {},
    { namespace: 'github_two', takenDisplayNames: ['GitHub'], suggestedName: name },
  )
  const message = 'Another provider uses this name. Enter a different display name.'
  const alerts = [...container.querySelectorAll('[role="alert"]')].map((alert) => alert.textContent)
  expect(alerts).toEqual(refused ? [message] : [])
  const next = [...container.querySelectorAll('button')].find(
    (button) => button.textContent === 'Next',
  )!
  expect(next.getAttribute('aria-disabled')).toBe(refused ? 'true' : null)
})

describe('configuration application defaults', () => {
  it('keeps a saved automatic client automatic when configuration is reopened', () => {
    const html = render({
      ...review,
      confirmed: true,
      application: { mode: 'automatic', clientId: 'coral-runtime-client' },
    })
    expect(html).toContain('name="application" value="automatic"')
    expect(html).not.toContain('value="coral-runtime-client"')
  })

  it('keeps a saved manual client explicit', () => {
    const html = render({
      ...review,
      confirmed: true,
      application: { mode: 'manual', clientId: 'my-client' },
    })
    expect(html).toContain('name="application" value="explicit"')
    expect(html).toContain('value="my-client"')
  })

  const recipe: RecipeConfigurationView = {
    id: 'example',
    inputs: [],
    suggestedMethods: [],
    values: {},
    application: {
      mode: 'manual',
      clientId: 'recipe-client',
      authentication: 'post',
      pkce: 'disabled',
      parameters: 'access_type=offline',
    },
  }

  it('fills the initial application from the recipe', () => {
    const html = render(review, recipe)
    expect(html).toContain('value="recipe-client"')
    expect(html).toContain('name="application" value="explicit"')
    expect(html).toContain('name="clientAuthentication" value="post"')
    expect(html).toContain('value="access_type=offline"')
  })

  it('preserves rejected edits ahead of recipe defaults', () => {
    const html = render(review, recipe, {
      error: 'Correct the configuration.',
      credential: null,
      configurationDraft: {
        values: review.values,
        application: { mode: 'manual', clientId: 'edited-client' },
      },
    })
    expect(html).toContain('value="edited-client"')
    expect(html).not.toContain('value="recipe-client"')
  })

  it('does not reapply a recipe after configuration is confirmed', () => {
    const html = render(
      {
        ...review,
        confirmed: true,
        application: { mode: 'manual', clientId: 'confirmed-client' },
      },
      recipe,
    )
    expect(html).toContain('value="confirmed-client"')
    expect(html).not.toContain('value="recipe-client"')
  })
})

describe('OAuth application registration copy', () => {
  // Sentry's shape: the document, the API and OAuth each have their own host.
  const separateHosts: ConfigurationReviewView = {
    ...review,
    automaticClientAvailable: false,
    values: {
      ...review.values,
      authorizationUrl: 'https://login.example.test/authorize',
      tokenUrl: 'https://login.example.test/token',
    },
    destinations: { listThings: 'https://api.example.test/v1/things' },
  }

  it('names the OAuth host, not the document or API host', () => {
    const html = render(separateHosts, undefined, undefined, 'openapi', {
      sourceHost: 'docs.example.test',
      serverName: 'api.example.test',
    })
    expect(html).toContain('registration with login.example.test.')
    expect(html).not.toContain('registration with api.example.test')
    expect(html).not.toContain('registration with docs.example.test')
  })

  it('names the service without an OAuth host to read', () => {
    const html = render({
      ...separateHosts,
      values: { ...separateHosts.values, tokenUrl: '' },
    })
    expect(html).toContain('registration with the service.')
  })
})

describe('Basic authentication', () => {
  // Mailgun's shape: a fixed username and one key as the password.
  const basic: ConfigurationReviewView = {
    ...review,
    authenticationMethods: ['basic'],
    values: {
      ...review.values,
      authentication: 'basic',
      basic: {
        username: { source: 'literal', value: 'api' },
        password: { source: 'field', field: 'api_key' },
      },
      credentialFields: {
        api_key: { label: 'API key', description: '', sensitive: true },
      },
    },
    errors: {
      'authentication.username': 'Send a plain value without a colon.',
    },
  }

  it('shows what Coral sends as the username and password, and the error beside it', () => {
    const html = render(basic)
    expect(html).toContain('aria-label="Username value"')
    expect(html).toContain('value="api"')
    expect(html).toContain('aria-label="Password credential"')
    expect(html).toContain('value="API key"')
    // Once beside the username, not again as a banner.
    expect(html.split('Send a plain value without a colon.')).toHaveLength(2)
  })
})

// Datadog's shape: the main server and the logs server each declare a `site`.
// Choosing the main server's site must not answer the logs server's site, and
// an unlisted `prefix` stays a question of its own.
it('resolves equally named variables per server declaration', () => {
  const html = render({
    ...review,
    authenticationMethods: ['none'],
    servers: [
      {
        declaration: '#/servers',
        tools: ['metrics'],
        options: [
          {
            url: 'https://api.{site}/v1',
            variables: [
              {
                name: 'site',
                defaultValue: 'datadoghq.com',
                enumerated: true,
                values: ['datadoghq.com', 'datadoghq.eu'],
              },
            ],
          },
        ],
      },
      {
        declaration: '#/paths/~1logs/servers',
        tools: ['logs'],
        options: [
          {
            url: 'https://{prefix}.{site}/v2',
            variables: [
              {
                name: 'site',
                defaultValue: 'logs.example.test',
                enumerated: true,
                values: ['logs.example.test', 'logs.eu.example.test'],
              },
              { name: 'prefix', defaultValue: 'http-intake', enumerated: false, values: [] },
            ],
          },
        ],
      },
    ],
    values: {
      ...review.values,
      authentication: 'none',
      servers: [
        {
          declaration: '#/servers',
          url: 'https://api.{site}/v1',
          custom: false,
          variables: { site: 'datadoghq.eu' },
        },
        {
          declaration: '#/paths/~1logs/servers',
          url: 'https://{prefix}.{site}/v2',
          custom: false,
          variables: { site: 'logs.example.test', prefix: 'intake' },
        },
      ],
    },
  })
  expect(html).toContain('value="https://api.datadoghq.eu/v1"')
  expect(html).toContain('value="https://intake.logs.example.test/v2"')
  expect(html).toContain('value="intake"')
  expect(html).not.toContain('https://intake.datadoghq.eu')
})

describe('credential preview', () => {
  const credentialFields = {
    api_key: { label: 'API key', description: '', sensitive: true },
    token: { label: 'API token', description: '', sensitive: true },
  }
  const methods: [CredentialMethod, Partial<ConfigurationValues>, string][] = [
    [
      'header',
      {
        headers: [
          { name: 'dd-api-key', source: 'field', field: 'api_key', prefix: '' },
          { name: 'dd-api-version', source: 'literal', value: '2024-01' },
        ],
      },
      'dd-api-key: \u2039API key\u203a\ndd-api-version: 2024-01',
    ],
    [
      'query',
      {
        parameters: [
          { name: 'key', source: 'field', field: 'api_key' },
          { name: 'token', source: 'field', field: 'token' },
        ],
      },
      '?key=\u2039API key\u203a&amp;token=\u2039API token\u203a',
    ],
    [
      'basic',
      {
        basic: {
          username: { source: 'field', field: 'api_key' },
          password: { source: 'field', field: 'token' },
        },
      },
      'authorization: Basic base64(\u2039API key\u203a:\u2039API token\u203a)',
    ],
  ]

  it.for(methods)(
    'shows what %s sends, naming each credential by its label',
    ([method, sent, preview]) => {
      const html = render({
        ...review,
        authenticationMethods: [method],
        values: { ...review.values, authentication: method, credentialFields, ...sent },
      })
      expect(html).toContain(preview)
    },
  )
})

describe('device configuration feedback', () => {
  const device: ConfigurationReviewView = {
    ...review,
    authenticationMethods: ['device'],
    values: { ...review.values, authentication: 'device' },
  }

  it('shows a device URL error once, beside the field', () => {
    const html = render({
      ...device,
      errors: {
        'authentication.device_authorization_url': 'Enter a valid device URL.',
      },
    })
    expect(html.match(/Enter a valid device URL\./g)).toHaveLength(1)
    expect(html).toContain('aria-invalid="true"')
  })
})

it('submits the discovered device endpoint when an MCP recipe selects device code', () => {
  const html = render(
    {
      ...review,
      authenticationMethods: ['oauth', 'device'],
      deviceAuthorizationUrl: 'https://example.test/device',
    },
    {
      id: 'device_mcp',
      inputs: [],
      suggestedMethods: ['device'],
      values: { authentication: 'device' },
    },
    undefined,
    'mcp',
  )
  expect(submitted(html)).toMatchObject({ authorizationUrl: 'https://example.test/device' })
  expect(html).not.toContain('name="application" value="automatic"')
})

it('uses the reviewed authorization-code endpoint after a rejected device draft', () => {
  const html = render(
    { ...review, deviceAuthorizationUrl: 'https://example.test/device' },
    undefined,
    {
      error: null,
      credential: null,
      configurationDraft: {
        values: {
          ...review.values,
          authentication: 'oauth',
          authorizationUrl: 'https://example.test/device',
        },
      },
    },
    'mcp',
  )
  expect(submitted(html)).toMatchObject({ authorizationUrl: 'https://example.test/authorize' })
})

it('keeps an unavailable recipe flow empty instead of selecting discovered OAuth', () => {
  const html = render(
    review,
    {
      id: 'missing_device',
      inputs: [],
      suggestedMethods: [],
      values: { authentication: '' },
    },
    undefined,
    'mcp',
  )
  expect(submitted(html)).toMatchObject({ authentication: '' })
})

it('submits recipe MCP scopes and their format while keeping discovered endpoints', () => {
  const html = render(
    review,
    {
      id: 'mcp_scopes',
      inputs: [],
      suggestedMethods: ['oauth'],
      values: {
        authentication: 'oauth',
        scopes: ['read', 'write'],
        scopeFormat: 'comma',
      },
    },
    undefined,
    'mcp',
  )
  expect(submitted(html)).toMatchObject({ scopes: ['read', 'write'], scopeFormat: 'comma' })
  expect(html).toContain('Requested scopes')
})

// Catches: long URL scopes, such as Microsoft Graph's, that all truncate to
// the same visible prefix and cannot be told apart, and a bare name that looks
// like a shortened one.
const graph = 'https://graph.example.test/'
it.each([
  [
    'a shared URL prefix',
    ['offline_access', `${graph}Mail.Send`, `${graph}User.Read`],
    ['offline_access', '…/Mail.Send', '…/User.Read'],
    `Shared prefix: ${graph}`,
  ],
  [
    'a bare name beside URL scopes',
    [`${graph}Mail.Send`, `${graph}User.Read`, 'Mail.Read'],
    ['…/Mail.Send', '…/User.Read', 'Mail.Read'],
    `Shared prefix: ${graph}`,
  ],
  [
    'a deeper shared path',
    ['https://api.example.test/auth/read', 'https://api.example.test/auth/write'],
    ['…/read', '…/write'],
    'Shared prefix: https://api.example.test/auth/',
  ],
  [
    'an unrelated host beside URL scopes',
    [`${graph}Mail.Send`, `${graph}User.Read`, 'https://mail.example.test/IMAP'],
    ['…/Mail.Send', '…/User.Read', 'https://mail.example.test/IMAP'],
    `Shared prefix: ${graph}`,
  ],
  [
    'two hosts that each share a prefix',
    [
      `${graph}User.Read`,
      `${graph}Mail.Send`,
      'https://mail.example.test/User.Read',
      'https://mail.example.test/IMAP',
    ],
    ['…/User.Read', '…/Mail.Send', '…/User.Read', '…/IMAP'],
    `Shared prefixes: ${graph}, https://mail.example.test/`,
  ],
  [
    'one scope on each host',
    ['https://a.example.test/read', 'https://b.example.test/write'],
    ['https://a.example.test/read', 'https://b.example.test/write'],
    undefined,
  ],
])('scope chips with %s show %j', (_case, scopes, labels, note) => {
  const container = document.createElement('div')
  container.innerHTML = render(review, {
    id: 'url_scopes',
    inputs: [],
    suggestedMethods: ['oauth'],
    values: { authentication: 'oauth', scopes },
  })
  const chips = [...container.querySelectorAll('button[aria-label^="Remove "]')].map(
    (remove) => remove.parentElement?.textContent,
  )
  expect(chips).toEqual(labels)
  // The input names the prefix, so a screen reader hears what "…/" stands for.
  const described = container
    .querySelector('input[role="combobox"]')
    ?.getAttribute('aria-describedby')
  expect(described ? container.querySelector(`#${CSS.escape(described)}`)?.textContent : null).toBe(
    note ?? null,
  )
  expect(submitted(container.innerHTML)).toMatchObject({ scopes })
})

it('shows and submits recipe service headers without requiring credential authentication', () => {
  const html = render(
    { ...review, authenticationMethods: ['none'] },
    {
      id: 'service_headers',
      inputs: [],
      suggestedMethods: ['none'],
      values: {
        authentication: 'none',
        requestHeaders: [{ name: 'x-service-version', value: '2022-11-28' }],
      },
    },
  )
  expect(html).toContain('Request headers')
  expect(html).toContain('value="x-service-version"')
  expect(html).toContain('value="2022-11-28"')
  expect(submitted(html)).toMatchObject({
    requestHeaders: [{ name: 'x-service-version', value: '2022-11-28' }],
  })
})

it('names the request header at fault once and marks its name invalid', () => {
  const html = render(
    {
      ...review,
      authenticationMethods: ['none'],
      errors: { 'request_headers.authorization': 'Send credentials as a sign-in method.' },
    },
    {
      id: 'service_headers',
      inputs: [],
      suggestedMethods: ['none'],
      values: {
        authentication: 'none',
        requestHeaders: [
          { name: 'authorization', value: 'Bearer 1' },
          { name: 'x-service-version', value: '2022-11-28' },
        ],
      },
    },
  )
  // The table keeps one height per row, so the error names its header below it.
  expect(html.match(/authorization: Send credentials as a sign-in method\./g)).toHaveLength(1)
  expect(html.match(/aria-invalid="true"/g)).toHaveLength(1)
  expect(html).toMatch(
    /aria-invalid="true"[^>]*value="authorization"|value="authorization"[^>]*aria-invalid="true"/,
  )
})
