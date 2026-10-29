import { create, fromBinary, toBinary, type MessageInitShape } from '@bufbuild/protobuf'
import { afterEach, describe, expect, it, vi } from 'vitest'

import {
  CredentialMethod,
  OAuthScopeFormat,
  ProviderConfigurationSchema,
  type ProviderConfigurationReviewSchema,
  ProviderSetupInputKind,
  ProviderSetupSchema,
  ProviderSetupState,
} from '@/generated/coral/management/v1/provider_pb'
import { authTestContext, localRuntimeConfig } from '@/auth/server-context.test-helper'
import { recipeDocument } from '../provider-recipes/library.server'
import { fakeManagement } from '../management.test-support'
import { callbackRedirectUri, prepareSetup, setupView } from './setup.server'

// A method a newer Coral server offers that this Reef was built without.
const NEWER_METHOD = 99 as CredentialMethod

afterEach(() => {
  vi.restoreAllMocks()
})

async function viewOf(setup: {
  inputKind?: ProviderSetupInputKind
  inputUrl?: string
  state?: ProviderSetupState
  credentialMethod?: CredentialMethod
  credentialFields?: MessageInitShape<typeof ProviderSetupSchema>['credentialFields']
  configuration?: MessageInitShape<typeof ProviderConfigurationReviewSchema>
}) {
  return setupView(
    create(ProviderSetupSchema, {
      name: 'workspaces/w/providerSetups/29a180e5-397c-4844-92de-8bbea6066235',
      inputKind: ProviderSetupInputKind.OPENAPI,
      state: ProviderSetupState.AWAITING_CONFIGURATION,
      credentialMethod: CredentialMethod.BEARER,
      ...setup,
    }),
  )
}

describe('setupView with confirmed authentication settings', () => {
  it('leaves Basic out when a part has a source this Reef does not know', async () => {
    const view = await viewOf({
      configuration: {
        confirmed: {
          displayName: 'Jira',
          authentication: {
            case: 'basicAuth',
            value: {
              username: { source: { case: undefined } },
              password: { source: { case: 'field', value: 'api_token' } },
            },
          },
        },
      },
    })

    // Saving a part Coral never stored would change the contract and sign every account out.
    expect(view.configuration?.values.authentication).toBe('')
    expect(view.configuration?.values.basic).toEqual({
      username: { source: 'literal', value: '' },
      password: { source: 'literal', value: '' },
    })
  })

  it('leaves query credentials out when a parameter has a source this Reef does not know', async () => {
    const view = await viewOf({
      configuration: {
        confirmed: {
          displayName: 'Trello',
          authentication: {
            case: 'queryCredentials',
            value: {
              parameters: {
                key: { source: { case: 'field', value: 'api_key' } },
                token: { source: { case: undefined } },
              },
            },
          },
        },
      },
    })

    expect(view.configuration?.values.authentication).toBe('')
    expect(view.configuration?.values.parameters).toEqual([])
  })

  it('lists the Credential Fields a person enters, in name order', async () => {
    const view = await viewOf({
      state: ProviderSetupState.READY,
      credentialMethod: CredentialMethod.HEADER_CREDENTIALS,
      credentialFields: {
        app_key: { label: 'Application key', description: '', sensitive: true },
        account: {
          label: 'Account email',
          description: 'The email you sign in with',
          sensitive: false,
        },
      },
    })

    expect(view.credentialFields).toEqual([
      {
        name: 'account',
        label: 'Account email',
        description: 'The email you sign in with',
        sensitive: false,
      },
      { name: 'app_key', label: 'Application key', description: '', sensitive: true },
    ])
  })
})

describe('setupView service name for sign-in copy', () => {
  const sentryDocument =
    'https://raw.githubusercontent.com/getsentry/sentry-api-schema/refs/heads/main/openapi-derefed.json'

  it('names the API host, not the host of a document published elsewhere', async () => {
    const view = await viewOf({
      inputUrl: sentryDocument,
      configuration: {
        destinations: {
          listIssues: 'https://us.sentry.io/api/0/issues/',
          listModels: 'https://us.sentry.io/api/0/seer/models/',
        },
      },
    })

    expect(view.serverName).toBe('us.sentry.io')
    expect(view.sourceHost).toBe('raw.githubusercontent.com')
  })

  it('names the API host, not a separate OAuth host', async () => {
    const view = await viewOf({
      inputUrl: 'https://docs.example.test/openapi.json',
      configuration: {
        confirmed: {
          authentication: {
            case: 'oauthAuthorizationCode',
            value: {
              authorizationUrl: 'https://login.example.test/authorize',
              tokenUrl: 'https://login.example.test/token',
            },
          },
        },
        destinations: { listThings: 'https://api.example.test/v1/things' },
      },
    })

    expect(view.serverName).toBe('api.example.test')
  })

  it('names the API host for an uploaded document', async () => {
    const view = await viewOf({
      configuration: { destinations: { listThings: 'https://api.example.test/v1/things' } },
    })

    expect(view.serverName).toBe('api.example.test')
    expect(view.sourceHost).toBe('')
  })

  it('names no host when Tools reach more than one', async () => {
    const view = await viewOf({
      inputUrl: 'https://api.example.test/openapi.json',
      configuration: {
        destinations: {
          listThings: 'https://api.example.test/v1/things',
          upload: 'https://uploads.example.test/v1/files',
        },
      },
    })

    expect(view.serverName).toBe('')
  })

  it('names no host before Coral resolves any destination', async () => {
    const view = await viewOf({ inputUrl: 'https://api.example.test/openapi.json' })

    expect(view.serverName).toBe('')
  })

  it('names the MCP server host', async () => {
    const view = await viewOf({
      inputKind: ProviderSetupInputKind.MCP,
      inputUrl: 'https://mcp.example.test/mcp',
    })

    expect(view.serverName).toBe('mcp.example.test')
    expect(view.sourceHost).toBe('mcp.example.test')
  })
})

describe('setupView with authentication methods Reef does not know', () => {
  it('leaves an unknown offered method out of the choices', async () => {
    const view = await viewOf({
      configuration: {
        authenticationMethods: [CredentialMethod.BEARER, NEWER_METHOD, CredentialMethod.NONE],
      },
    })

    expect(view.configuration?.authenticationMethods).toEqual(['bearer', 'none'])
  })

  it('shows an unknown suggested method as nothing chosen', async () => {
    // A newer server sets an authentication case this Reef has no field for (tag 99).
    const known = toBinary(
      ProviderConfigurationSchema,
      create(ProviderConfigurationSchema, { displayName: 'Jira' }),
    )
    const suggested = fromBinary(
      ProviderConfigurationSchema,
      new Uint8Array([...known, 0x9a, 0x06, 0x00]),
    )
    const view = await viewOf({
      configuration: { suggested, authenticationMethods: [CredentialMethod.BEARER, NEWER_METHOD] },
    })

    expect(view.configuration?.values).toMatchObject({ displayName: 'Jira', authentication: '' })
  })

  it('has no credential method for a setup confirmed with an unknown one', async () => {
    const view = await viewOf({
      state: ProviderSetupState.AWAITING_AUTHORIZATION,
      credentialMethod: NEWER_METHOD,
    })

    expect(view.credentialMethod).toBeNull()
  })

  it('does not warn about a setup with no credential method yet', async () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})

    const view = await viewOf({
      state: ProviderSetupState.AWAITING_DISCOVERY,
      credentialMethod: CredentialMethod.UNSPECIFIED,
    })

    expect(view.credentialMethod).toBeNull()
    expect(warn).not.toHaveBeenCalled()
  })
})

describe('operator-selected outbound transports', () => {
  it('sends private HTTP source hostnames to Coral instead of imposing a second destination policy', async () => {
    const createProviderSetup = vi.fn(() => ({}))
    const api = fakeManagement({ setups: { createProviderSetup } })
    for (const source of ['mcp', 'url']) {
      const form = new FormData()
      form.set('source', source)
      form.set('sourceUrl', 'http://sidecar:5000/source')
      await prepareSetup(api, 'workspaces/w', form)
      expect(createProviderSetup).toHaveBeenLastCalledWith(
        expect.objectContaining({
          input: {
            case: source === 'mcp' ? 'mcpUrl' : 'openapiUrl',
            value: 'http://sidecar:5000/source',
          },
        }),
        expect.anything(),
      )
    }
  })
})

it('shows a confirmed device-code configuration for owner review', async () => {
  const view = await viewOf({
    credentialMethod: CredentialMethod.OAUTH_DEVICE_CODE,
    configuration: {
      authenticationMethods: [CredentialMethod.OAUTH_DEVICE_CODE],
      confirmed: {
        displayName: 'GitHub',
        authentication: {
          case: 'oauthDeviceCode',
          value: {
            deviceAuthorizationUrl: 'https://github.com/login/device/code',
            tokenUrl: 'https://github.com/login/oauth/access_token',
            scopes: ['repo'],
            application: { mode: { case: 'clientId', value: 'public-app' } },
          },
        },
      },
    },
  })
  expect(view.credentialMethod).toBe('device')
  expect(view.configuration?.values).toMatchObject({
    authentication: 'device',
    authorizationUrl: 'https://github.com/login/device/code',
    tokenUrl: 'https://github.com/login/oauth/access_token',
    scopes: ['repo'],
  })
})

it('preserves confirmed MCP scopes and service headers while refreshing discovered endpoints', async () => {
  const authentication = {
    case: 'oauthAuthorizationCode' as const,
    value: {
      scopes: ['read'],
      authorizationUrl: 'https://example.test/authorize',
      tokenUrl: 'https://example.test/token',
    },
  }
  const view = await viewOf({
    inputKind: ProviderSetupInputKind.MCP,
    configuration: {
      suggested: { authentication },
      confirmed: {
        authentication: {
          ...authentication,
          value: {
            ...authentication.value,
            scopes: ['admin'],
            scopeFormat: OAuthScopeFormat.COMMA,
          },
        },
        requestHeaders: { 'x-tenant': 'example' },
      },
    },
  })
  expect(view.configuration?.values).toMatchObject({
    scopes: ['admin'],
    scopeFormat: 'comma',
    requestHeaders: [{ name: 'x-tenant', value: 'example' }],
    authorizationUrl: 'https://example.test/authorize',
  })
})

/** A first read of an MCP source that sends `requestHeaders`. */
function mcpSourceForm(requestHeaders: string) {
  const form = new FormData()
  form.set('source', 'mcp')
  form.set('sourceUrl', 'https://example.test/mcp')
  form.set('requestHeaders', requestHeaders)
  return form
}

it.each([
  ['one header', [{ name: 'x-tenant', value: 'team' }]],
  // An editor opened and left empty sends no headers, rather than failing the read.
  [
    'blank rows around a header',
    [
      { name: '', value: '' },
      { name: 'x-tenant', value: 'team' },
      { name: ' ', value: '' },
    ],
  ],
])('sends non-secret service headers from %s when first reading an MCP source', async (_, rows) => {
  const createProviderSetup = vi.fn(() => ({}))
  const api = fakeManagement({ setups: { createProviderSetup } })
  await prepareSetup(api, 'workspaces/w', mcpSourceForm(JSON.stringify(rows)))
  expect(createProviderSetup).toHaveBeenCalledWith(
    expect.objectContaining({ requestHeaders: { 'x-tenant': 'team' } }),
    expect.anything(),
  )
})

it.each([
  ['text that is not JSON', '{'],
  ['an object instead of a list', '{}'],
  ['a missing header', '[null]'],
  ['one header twice in different case', '[{"name":"x-a","value":"a"},{"name":"X-A","value":"b"}]'],
])('refuses service headers given as %s before reading the source', async (_, requestHeaders) => {
  const createProviderSetup = vi.fn(() => ({}))
  const api = fakeManagement({ setups: { createProviderSetup } })
  await expect(
    prepareSetup(api, 'workspaces/w', mcpSourceForm(requestHeaders)),
  ).rejects.toMatchObject({ status: 400 })
  expect(createProviderSetup).not.toHaveBeenCalled()
})

// Reef once decoded uploads as UTF-8 text, which refused every gzipped document.
it('sends an uploaded document to Coral as its exact bytes', async () => {
  const gzipped = new Uint8Array([0x1f, 0x8b, 0x08, 0x00, 0xff])
  const createProviderSetup = vi.fn(() => ({}))
  const api = fakeManagement({ setups: { createProviderSetup } })
  const form = new FormData()
  form.set('source', 'file')
  form.set('document', new File([gzipped], 'openapi.json.gz'))
  await prepareSetup(api, 'workspaces/w', form)
  expect(createProviderSetup).toHaveBeenCalledWith(
    expect.objectContaining({ input: { case: 'openapiDocument', value: gzipped } }),
    expect.anything(),
  )
})

/** Reads a recipe's bundled document as the source form posts it. */
async function prepareRecipe(recipe: string) {
  const createProviderSetup = vi.fn(() => ({}))
  const api = fakeManagement({ setups: { createProviderSetup } })
  const form = new FormData()
  form.set('source', 'recipe')
  form.set('recipe', recipe)
  // The browser can post these, but a recipe read must not use them.
  form.set('sourceUrl', 'https://documents.example.com/openapi.json')
  form.set('document', new File(['{"openapi": "3.1.0"}'], 'openapi.json'))
  return { prepared: await prepareSetup(api, 'workspaces/w', form), createProviderSetup }
}

describe('bundled recipe documents', () => {
  it('sends the text from the recipe library, not from the browser', async () => {
    const { createProviderSetup } = await prepareRecipe('gmail')
    expect(createProviderSetup).toHaveBeenCalledWith(
      expect.objectContaining({
        input: {
          case: 'openapiDocument',
          value: new TextEncoder().encode(recipeDocument('gmail')!),
        },
      }),
      expect.anything(),
    )
  })

  it('asks for the provider again when the recipe bundles no document', async () => {
    const { prepared, createProviderSetup } = await prepareRecipe('stripe')
    expect(prepared).toEqual({ problem: 'Choose the provider again.' })
    expect(createProviderSetup).not.toHaveBeenCalled()
  })
})

// Catches: a local launcher, such as Desktop, sending consent back to a
// callback that is not its own loopback listener.
describe('callbackRedirectUri without sign-in', () => {
  const request = new Request('http://127.0.0.1:5173/providers/new')
  const uri = (upstreamOAuthCallbackUrl: string) =>
    callbackRedirectUri(
      request,
      authTestContext(null, { ...localRuntimeConfig, upstreamOAuthCallbackUrl }),
    )

  it.each([
    ['', 'http://127.0.0.1:5173/provider-setup/oauth/callback'],
    [
      'http://127.0.0.1:8779/provider-oauth/callback',
      'http://127.0.0.1:8779/provider-oauth/callback',
    ],
  ])('with configured callback %j, returns %s', (configured, expected) => {
    expect(uri(configured)).toBe(expected)
  })

  it.each([
    'https://untrusted.example/provider-oauth/callback',
    'http://127.0.0.1:8779/elsewhere',
    'http://127.0.0.1:8779/provider-oauth/callback?next=/',
  ])('refuses configured callback %s', (configured) => {
    let thrown: unknown
    try {
      uri(configured)
    } catch (error) {
      thrown = error
    }
    expect(thrown).toBeInstanceOf(Response)
    expect((thrown as Response).status).toBe(503)
  })
})
