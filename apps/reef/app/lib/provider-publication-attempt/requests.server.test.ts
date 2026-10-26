import { create } from '@bufbuild/protobuf'
import { Code, ConnectError } from '@connectrpc/connect'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import {
  CredentialMethod,
  CredentialRoute_Kind,
  OAuthScopeFormat,
  ProviderPublicationOutcome,
  ProviderSetupInputKind,
  ProviderSetupSchema,
  ProviderSetupState,
  SetupAuthorizationState,
  type PublishProviderRequest,
} from '@/generated/coral/management/v1/provider_pb'
import { fakeManagement } from '@/lib/management.test-support'
import { authTestContext } from '@/auth/server-context.test-helper'
import { parseAttempt } from './model'
import { attemptAction, loadAttempt } from './requests.server'
import { readCallbackReturn, setupView } from './setup.server'

const context = authTestContext(null)

function clients(publish: (request: PublishProviderRequest) => object) {
  return fakeManagement({ providers: { publishProvider: publish } })
}

function formWith(fields: Record<string, string>) {
  const form = new FormData()
  for (const [key, value] of Object.entries(fields)) form.set(key, value)
  return form
}

describe('atomic publication', () => {
  const workspace = 'workspaces/11111111-1111-4111-8111-111111111111'
  const setup = '22222222-2222-4222-8222-222222222222'
  const requestId = '33333333-3333-4333-8333-333333333333'
  const provider = `${workspace}/providers/44444444-4444-4444-8444-444444444444`
  const target = {
    kind: 'workspace' as const,
    parent: workspace,
    workspacePath: `/${workspace}`,
    basePath: `/${workspace}/providers/new`,
  }

  function publicationForm() {
    const form = new FormData()
    for (const [key, value] of Object.entries({
      intent: 'publish',
      setup,
      requestId,
      namespace: 'weather',
      account: 'none',
      configurationRevision: '2',
    }))
      form.set(key, value)
    return form
  }

  describe('Workspace Provider publication adapter', () => {
    it('submits the reviewed shared authorization without reading a replacement from Setup', async () => {
      const reviewed = '55555555-5555-4555-8555-555555555555'
      const requests: PublishProviderRequest[] = []
      const api = clients((request) => {
        requests.push(request)
        return { outcome: ProviderPublicationOutcome.SUCCEEDED, provider }
      })
      const form = publicationForm()
      form.set('account', 'fixed')
      form.set('accountGrant', reviewed)
      await attemptAction(api, new Request('https://reef.test'), context, form, target)
      expect(requests).toHaveLength(1)
      expect(requests[0].credentialRoute).toMatchObject({
        kind: CredentialRoute_Kind.FIXED,
      })
      expect(requests[0].accountGrant).toBe(reviewed)
    })

    it('publishes credential-free Add through one domain call and uses its Provider identity', async () => {
      const requests: PublishProviderRequest[] = []
      const api = clients((request) => {
        requests.push(request)
        return { outcome: ProviderPublicationOutcome.SUCCEEDED, provider }
      })
      const response = await attemptAction(
        api,
        new Request('https://reef.test'),
        context,
        publicationForm(),
        target,
      )
      expect(requests).toHaveLength(1)
      expect(requests[0]).toMatchObject({
        parent: workspace,
        setup: `${workspace}/providerSetups/${setup}`,
        requestId,
        namespace: 'weather',
        credentialRoute: { kind: CredentialRoute_Kind.NONE },
      })
      expect(response).toBeInstanceOf(Response)
      expect((response as Response).headers.get('Location')).toBe(`/${provider}`)
    })

    it('preserves the frozen submission after a lost publication response', async () => {
      const requests: PublishProviderRequest[] = []
      const api = clients((request) => {
        requests.push(request)
        if (requests.length === 1) throw new ConnectError('response lost', Code.Unavailable)
        return { outcome: ProviderPublicationOutcome.SUCCEEDED, provider }
      })
      const lost = await attemptAction(
        api,
        new Request('https://reef.test'),
        context,
        publicationForm(),
        target,
      )
      const retryUrl = new URL((lost as Response).headers.get('Location')!, 'https://reef.test')
      expect(retryUrl.searchParams.get('requestId')).toBe(requestId)
      expect(retryUrl.searchParams.get('namespace')).toBe('weather')
      expect(retryUrl.searchParams.get('configurationRevision')).toBe('2')
      const retry = new FormData()
      retry.set('intent', 'publish')
      for (const [key, value] of retryUrl.searchParams) retry.append(key, value)
      await attemptAction(api, new Request(retryUrl), context, retry, target)
      expect(requests).toHaveLength(2)
      expect(requests[1]).toEqual(requests[0])
    })
  })
})

// Catches: a Program's link that names only its Setup losing the namespace and
// route the Program asked for, or a route this Reef does not know making the
// Program's Setup look like one a person started.
describe("loading a Program's link", () => {
  const parent = 'workspaces/11111111-1111-4111-8111-111111111111'
  const id = '29a180e5-397c-4844-92de-8bbea6066235'
  it.each([
    [CredentialRoute_Kind.NONE, { namespace: 'weather', route: 'none' }],
    [CredentialRoute_Kind.CALLER, { namespace: 'weather', route: 'caller' }],
    [CredentialRoute_Kind.FIXED, { namespace: 'weather', route: 'fixed' }],
    [99 as CredentialRoute_Kind, { namespace: 'weather', route: null }],
  ])('reads route %s and the namespace from the Setup', async (kind, request) => {
    const api = fakeManagement({
      setups: {
        getProviderSetup: () =>
          create(ProviderSetupSchema, {
            name: `${parent}/providerSetups/${id}`,
            inputKind: ProviderSetupInputKind.OPENAPI,
            state: ProviderSetupState.READY,
            credentialMethod: CredentialMethod.BEARER,
            request: { namespace: 'weather', credentialRoute: { kind } },
          }),
      },
    })
    const loaded = await loadAttempt(
      api,
      parent,
      parseAttempt('workspace', new URLSearchParams({ setup: id })),
    )
    expect(loaded.setup?.request).toEqual(request)
  })
})

describe('recipe preparation', () => {
  const setup = '29a180e5-397c-4844-92de-8bbea6066235'
  const parent = 'workspaces/11111111-1111-4111-8111-111111111111'
  const basePath = '/providers/new'
  const createProviderSetup = vi.fn()
  const confirmProviderSetupConfiguration = vi.fn()
  const api = fakeManagement({ setups: { createProviderSetup, confirmProviderSetupConfiguration } })

  beforeEach(() => vi.resetAllMocks())

  const prepare = async (kind: 'workspace' | 'update') => {
    createProviderSetup.mockResolvedValue({
      name: `${parent}/providerSetups/${setup}`,
    })
    const response = await attemptAction(
      api,
      new Request(`http://127.0.0.1${basePath}`, { method: 'POST' }),
      context,
      formWith({
        intent: 'prepare',
        source: 'url',
        sourceUrl: 'https://api.example.com/openapi.json',
        recipe: 'stripe',
        etag: 'v1',
      }),
      kind === 'update'
        ? {
            kind,
            parent,
            provider: `${parent}/providers/stripe`,
            successPath: '/',
            basePath,
            account: 'none',
          }
        : { kind, parent, workspacePath: '/', basePath },
    )
    if (!(response instanceof Response)) throw new Error('Expected a redirect')
    return new URL(response.headers.get('Location')!, 'http://127.0.0.1').searchParams
  }

  describe('prepare', () => {
    it('carries the chosen recipe into a new provider', async () => {
      expect((await prepare('workspace')).get('recipe')).toBe('stripe')
    })

    // Update keeps the saved configuration; a recipe must not fill it in again.
    it('drops the recipe when updating a provider', async () => {
      const params = await prepare('update')
      expect(params.get('setup')).toBe(setup)
      expect(params.has('recipe')).toBe(false)
    })
  })

  describe('configure with recipe answers', () => {
    const configuration = {
      displayName: 'Stripe',
      authentication: 'none',
      name: '',
      authorizationUrl: '',
      tokenUrl: '',
      scopes: [],
      servers: [],
    }

    async function rejectedDraft(recipeInputs: string) {
      confirmProviderSetupConfiguration.mockResolvedValue({
        configuration: { errors: { displayName: 'Enter a name.' } },
      })
      const result = await attemptAction(
        api,
        new Request(`http://127.0.0.1${basePath}?setup=${setup}&namespace=stripe`, {
          method: 'POST',
        }),
        context,
        formWith({
          intent: 'configure',
          setup,
          configurationRevision: '1',
          configuration: JSON.stringify(configuration),
          recipeInputs,
        }),
        { kind: 'workspace', parent, workspacePath: '/', basePath },
      )
      if (result instanceof Response || !('data' in result))
        throw new Error('Expected a rejected draft')
      return result.data as {
        configurationDraft: { recipeInputs?: Record<string, string> }
      }
    }

    it('returns the answers with a rejected draft, but never sends them to Coral', async () => {
      const draft = await rejectedDraft(JSON.stringify({ region: 'eu', site: 'acme' }))
      expect(draft.configurationDraft.recipeInputs).toEqual({
        region: 'eu',
        site: 'acme',
      })
      const request = confirmProviderSetupConfiguration.mock.calls[0][0]
      expect(JSON.stringify(request.configuration)).not.toContain('acme')
    })

    // Answers come from the browser, so only answer-shaped entries return to the page.
    it('drops entries that are not answers', async () => {
      const draft = await rejectedDraft(
        JSON.stringify({
          region: 'eu',
          Region: 'us',
          count: 3,
          long: 'x'.repeat(2049),
        }),
      )
      expect(draft.configurationDraft.recipeInputs).toEqual({ region: 'eu' })
    })

    it('keeps at most 64 answers', async () => {
      const answers = Object.fromEntries(Array.from({ length: 70 }, (_, i) => [`input_${i}`, 'x']))
      const draft = await rejectedDraft(JSON.stringify(answers))
      expect(Object.keys(draft.configurationDraft.recipeInputs ?? {})).toHaveLength(64)
    })

    it.each([
      ['text that is not JSON', '{'],
      ['a list', '["eu"]'],
      ['an oversized field', JSON.stringify({ region: 'x'.repeat(16384) })],
    ])('ignores %s', async (_, recipeInputs) => {
      const draft = await rejectedDraft(recipeInputs)
      expect(draft.configurationDraft).not.toHaveProperty('recipeInputs')
    })
  })
})

describe('configure', () => {
  const setup = '29a180e5-397c-4844-92de-8bbea6066235'
  const parent = 'workspaces/11111111-1111-4111-8111-111111111111'
  const basePath = '/providers/new'
  const confirmProviderSetupConfiguration = vi.fn()
  const api = fakeManagement({ setups: { confirmProviderSetupConfiguration } })

  beforeEach(() => vi.resetAllMocks())

  async function confirmed(
    values: Record<string, unknown>,
    application: Record<string, string> = {},
  ) {
    confirmProviderSetupConfiguration.mockResolvedValue({ configuration: { errors: {} } })
    await attemptAction(
      api,
      new Request(`http://127.0.0.1${basePath}?setup=${setup}&namespace=example`, {
        method: 'POST',
      }),
      context,
      formWith({
        intent: 'configure',
        setup,
        configurationRevision: '1',
        configuration: JSON.stringify({
          displayName: 'Example',
          authentication: 'none',
          headers: [],
          parameters: [],
          basic: {
            username: { source: 'literal', value: '' },
            password: { source: 'literal', value: '' },
          },
          credentialFields: {},
          authorizationUrl: '',
          tokenUrl: '',
          scopes: [],
          servers: [],
          ...values,
        }),
        ...application,
      }),
      { kind: 'workspace', parent, workspacePath: '/', basePath },
    )
    return confirmProviderSetupConfiguration.mock.calls[0][0].configuration
  }

  it.each(['oauth', 'device'])(
    'sends %s scope encoding independently of service headers',
    async (authentication) => {
      const configuration = await confirmed(
        {
          authentication,
          scopes: ['read', 'write'],
          scopeFormat: 'comma',
          requestHeaders: [{ name: 'x-service-version', value: '2022-11-28' }],
        },
        { application: 'explicit', clientId: 'manual-client' },
      )
      expect(configuration.authentication.value).toMatchObject({
        scopes: ['read', 'write'],
        scopeFormat: OAuthScopeFormat.COMMA,
      })
      expect(configuration.requestHeaders).toEqual({ 'x-service-version': '2022-11-28' })
    },
  )

  it.each([
    { authentication: 'oauth', scopeFormat: 'unknown' },
    {
      requestHeaders: [
        { name: 'x-tenant', value: 'one' },
        { name: 'X-Tenant', value: 'two' },
      ],
    },
    { requestHeaders: [{ name: 'x-tenant', value: 123 }] },
  ])('refuses malformed settings without sending a partial configuration: %j', async (values) => {
    await expect(confirmed(values)).rejects.toMatchObject({ status: 400 })
    expect(confirmProviderSetupConfiguration).not.toHaveBeenCalled()
  })

  // Reef writes a configuration to Coral and later shows Coral's copy as form values.
  // Each method must survive that round trip unchanged.
  it.each<[string, Record<string, unknown>, Record<string, string>]>([
    [
      'header credentials',
      {
        authentication: 'header',
        headers: [
          { name: 'authorization', source: 'field', field: 'api_key', prefix: 'Token token=' },
          { name: 'x-client', source: 'literal', value: 'coral' },
        ],
        credentialFields: { api_key: { label: 'API key', description: '', sensitive: true } },
      },
      {},
    ],
    [
      'Basic authentication',
      {
        authentication: 'basic',
        basic: {
          username: { source: 'field', field: 'email' },
          password: { source: 'literal', value: 'x' },
        },
        credentialFields: {
          email: { label: 'Atlassian account email', description: '', sensitive: false },
        },
      },
      {},
    ],
    [
      'query credentials',
      {
        authentication: 'query',
        parameters: [
          { name: 'api_key', source: 'field', field: 'key' },
          { name: 'format', source: 'literal', value: 'json' },
        ],
        credentialFields: { key: { label: 'API key', description: '', sensitive: true } },
      },
      {},
    ],
    [
      'OAuth',
      {
        authentication: 'oauth',
        authorizationUrl: 'https://login.example.com/authorize',
        tokenUrl: 'https://login.example.com/token',
        scopes: ['read', 'write'],
      },
      { application: 'explicit', clientId: 'manual-client' },
    ],
  ])('shows sent %s back as the same form values', async (_, values, application) => {
    const configuration = await confirmed(values, application)
    const view = setupView(
      create(ProviderSetupSchema, {
        name: `${parent}/providerSetups/${setup}`,
        inputKind: ProviderSetupInputKind.OPENAPI,
        state: ProviderSetupState.AWAITING_CONFIGURATION,
        configuration: { confirmed: configuration },
      }),
    )
    expect(view.configuration?.values).toMatchObject(values)
    if (application.clientId)
      expect(view.configuration?.application).toMatchObject({
        mode: 'manual',
        clientId: application.clientId,
      })
  })

  it('sends no field presentation for a method without header credentials', async () => {
    const configuration = await confirmed({
      authentication: 'bearer',
      credentialFields: { api_key: { label: 'API key', description: '', sensitive: true } },
    })
    expect(configuration.credentialFields).toEqual({})
  })

  it('sends no settings for a method not chosen yet', async () => {
    const configuration = await confirmed({ authentication: '' })
    expect(configuration.authentication.case).toBeUndefined()
  })

  // Coral refuses a configuration in three ways; each returns the edited
  // draft to the page, and none returns the client secret.
  it.each([
    [
      'returned field errors',
      'access_type=offline',
      () =>
        confirmProviderSetupConfiguration.mockResolvedValue({
          configuration: { errors: { 'authentication.token_url': 'Enter a valid token URL.' } },
        }),
      400,
    ],
    [
      'a thrown OAuth policy error',
      'state=override',
      () =>
        confirmProviderSetupConfiguration.mockRejectedValue(
          new ConnectError('reserved parameter', Code.InvalidArgument),
        ),
      400,
    ],
    [
      'an application-secret precondition',
      'access_type=offline',
      () =>
        confirmProviderSetupConfiguration.mockRejectedValue(
          new ConnectError('client secret required', Code.FailedPrecondition),
        ),
      409,
    ],
    ['a repeated authorization parameter', 'prompt=login&prompt=consent', () => {}, 400],
  ])(
    'returns the draft without the client secret after %s',
    async (_, parameters, refuse, status) => {
      refuse()
      const secret = 'write-only-secret'
      const result = await attemptAction(
        api,
        new Request(`http://127.0.0.1${basePath}?setup=${setup}&namespace=edited_namespace`, {
          method: 'POST',
        }),
        context,
        formWith({
          intent: 'configure',
          setup,
          configurationRevision: '1',
          configuration: JSON.stringify({
            displayName: 'My edited provider',
            authentication: 'oauth',
            headers: [],
            parameters: [],
            basic: {
              username: { source: 'literal', value: '' },
              password: { source: 'literal', value: '' },
            },
            credentialFields: {},
            authorizationUrl: 'https://login.example.com/authorize',
            tokenUrl: 'not-a-url',
            scopes: ['read', 'write'],
            servers: [],
          }),
          application: 'explicit',
          clientId: 'edited-client',
          clientAuthentication: 'post',
          clientSecret: secret,
          pkce: 'disabled',
          authorizationParameters: parameters,
        }),
        { kind: 'workspace', parent, workspacePath: '/', basePath },
      )
      if (result instanceof Response || !('data' in result)) throw new Error('Expected a draft')
      expect(result.init?.status).toBe(status)
      expect((result.data as { configurationDraft?: unknown }).configurationDraft).toMatchObject({
        namespace: 'edited_namespace',
        values: {
          displayName: 'My edited provider',
          authorizationUrl: 'https://login.example.com/authorize',
          tokenUrl: 'not-a-url',
          scopes: ['read', 'write'],
        },
        application: {
          mode: 'manual',
          clientId: 'edited-client',
          authentication: 'post',
          pkce: 'disabled',
          parameters,
        },
      })
      expect(JSON.stringify(result.data)).not.toContain(secret)
    },
  )
})

// Catches: an account answer that Coral would refuse at publication passing
// the account question, because Reef judged the route from the method.
describe('account answer', () => {
  const setup = '29a180e5-397c-4844-92de-8bbea6066235'
  const parent = 'workspaces/11111111-1111-4111-8111-111111111111'
  const basePath = '/providers/new'
  const getProviderSetup = vi.fn()
  const api = fakeManagement({ setups: { getProviderSetup } })
  const { CALLER, FIXED } = CredentialRoute_Kind

  beforeEach(() => vi.resetAllMocks())

  it.each([
    ['an individual account for a credential', 'caller', [CALLER, FIXED], 302],
    ['no account for a credential', 'none', [CALLER, FIXED], 400],
    // Coral reports no routes before confirmation, and Publication decides.
    ['an answer before the configuration is confirmed', 'caller', [], 302],
  ] as const)('takes %s only when Coral allows the route', async (_, account, allowed, status) => {
    getProviderSetup.mockResolvedValue(
      create(ProviderSetupSchema, {
        name: `${parent}/providerSetups/${setup}`,
        // The method says nothing here: only the routes Coral allows decide.
        credentialMethod: CredentialMethod.BEARER,
        allowedCredentialRoutes: allowed.map((kind: CredentialRoute_Kind) => ({ kind })),
      }),
    )
    const answered = await attemptAction(
      api,
      new Request(`http://127.0.0.1${basePath}?setup=${setup}&namespace=example`, {
        method: 'POST',
      }),
      context,
      formWith({ intent: 'account', setup, account }),
      { kind: 'workspace', parent, workspacePath: '/', basePath },
    ).catch((response: unknown) => response)
    expect(answered).toBeInstanceOf(Response)
    expect((answered as Response).status).toBe(status)
  })
})

describe('secret entry', () => {
  const setup = '29a180e5-397c-4844-92de-8bbea6066235'
  const parent = 'workspaces/11111111-1111-4111-8111-111111111111'
  const basePath = '/providers/new'
  const authorizeProviderSetup = vi.fn()
  const api = fakeManagement({ setups: { authorizeProviderSetup } })

  beforeEach(() => vi.resetAllMocks())

  const submit = async (form: FormData) => {
    return attemptAction(
      api,
      new Request(`http://127.0.0.1${basePath}?setup=${setup}&namespace=example`, {
        method: 'POST',
      }),
      context,
      form,
      { kind: 'workspace', parent, workspacePath: '/', basePath },
    )
  }

  const entry = (values: [string, string][]) => {
    const form = new FormData()
    for (const [key, value] of [
      ['intent', 'secret'],
      ['setup', setup],
      ['configurationRevision', '2'],
    ])
      form.set(key, value)
    for (const [field, value] of values) form.append(`credential.${field}`, value)
    return form
  }

  it('sends each Credential Field value by name, and nothing else from the form', async () => {
    authorizeProviderSetup.mockResolvedValue({})

    await submit(
      entry([
        ['api_key', 'api-canary'],
        ['app_key', 'app-canary'],
      ]),
    )

    const [request] = authorizeProviderSetup.mock.calls[0]
    expect(request.credentialValues).toEqual({ api_key: 'api-canary', app_key: 'app-canary' })
    expect(request.configurationRevision).toBe(2n)
  })

  // Repeating one field would otherwise carry values past the field bound.
  it('refuses a form that repeats one Credential Field', async () => {
    await expect(
      submit(
        entry([
          ['api_key', 'first'],
          ['api_key', 'second'],
        ]),
      ),
    ).rejects.toMatchObject({ status: 400 })
    expect(authorizeProviderSetup).not.toHaveBeenCalled()
  })
})

describe('consent issuance', () => {
  const name = 'workspaces/11111111-1111-4111-8111-111111111111'
  const setup = '22222222-2222-4222-8222-222222222222'
  const basePath = `/${name}/providers/new`
  const target = { kind: 'workspace' as const, parent: name, workspacePath: `/${name}`, basePath }

  // Catches: Desktop opening consent in the system browser but reloading a page
  // other than the one the callback returns this browser to.
  it.each(['oauth-start', 'oauth-resume'])(
    'names the page the %s callback returns to for Desktop',
    async (intent) => {
      const consent = 'https://provider.test/consent?state=opaque'
      const pending = {
        name: `${name}/providerSetups/${setup}`,
        authorization: {
          state: SetupAuthorizationState.PENDING,
          consentNavigation: { url: consent },
        },
      }
      const api = fakeManagement({
        setups: {
          startProviderSetupAuthorization: () => pending,
          getProviderSetup: () => pending,
        },
      })
      const response = await attemptAction(
        api,
        new Request(`http://127.0.0.1:3000${basePath}?setup=${setup}&namespace=consent`),
        context,
        formWith({ intent, setup, configurationRevision: '1', application: 'automatic' }),
        target,
      )
      if (!(response instanceof Response)) throw new Error('expected a consent redirect')
      expect(response.headers.get('Location')).toBe(consent)
      const callback = await readCallbackReturn(
        new Request('http://127.0.0.1:3000/provider-setup/oauth/callback', {
          headers: { cookie: response.headers.get('Set-Cookie')!.split(';')[0] },
        }),
        pending.name,
        context,
      )
      const progress = response.headers.get('X-Coral-Authorization-Return-To')
      expect(progress).toMatch(new RegExp(`^${basePath}\\?setup=${setup}&`))
      expect(callback?.returnTo).toBe(progress)
    },
  )

  // Catches: a sign-in that read the operations returning as the account
  // question's answer because the page URL, such as an agent's link, already
  // named a route. Reef then adds the provider without asking.
  it.each([
    ['oauth-start', 'the account question', { account: 'fixed' }, '1'],
    ['oauth-resume', 'the account question', { account: 'fixed' }, '1'],
    ['oauth-start', 'discovery', {}, null],
    ['oauth-resume', 'discovery', {}, null],
  ])(
    'returns %s from %s as an answer only when the question started it',
    async (intent, _, fields, review) => {
      const pending = {
        name: `${name}/providerSetups/${setup}`,
        authorization: {
          state: SetupAuthorizationState.PENDING,
          consentNavigation: { url: 'https://provider.test/consent?state=opaque' },
        },
      }
      const api = fakeManagement({
        setups: {
          startProviderSetupAuthorization: () => pending,
          getProviderSetup: () => pending,
        },
      })
      const response = await attemptAction(
        api,
        new Request(
          `http://127.0.0.1:3000${basePath}?setup=${setup}&namespace=weather` +
            '&operationsAccepted=1&account=fixed&returnFromConnect=1',
        ),
        context,
        formWith({
          intent,
          setup,
          configurationRevision: '1',
          application: 'automatic',
          ...fields,
        }),
        target,
      )
      if (!(response instanceof Response)) throw new Error('expected a consent redirect')
      const progress = new URL(
        response.headers.get('X-Coral-Authorization-Return-To')!,
        'http://127.0.0.1:3000',
      ).searchParams
      expect(progress.get('account')).toBe('fixed')
      expect(progress.get('returnFromConnect')).toBe(review)
    },
  )

  // Catches: the secret an agent's addProvider cannot give being dropped
  // before Coral, so a confidential application can never sign in.
  it('sends a confidential application secret with the sign-in', async () => {
    const pending = {
      name: `${name}/providerSetups/${setup}`,
      authorization: {
        state: SetupAuthorizationState.PENDING,
        consentNavigation: { url: 'https://provider.test/consent?state=opaque' },
      },
    }
    const startProviderSetupAuthorization = vi.fn(() => pending)
    const api = fakeManagement({
      setups: { startProviderSetupAuthorization, getProviderSetup: () => pending },
    })
    await attemptAction(
      api,
      new Request(`http://127.0.0.1:3000${basePath}?setup=${setup}&namespace=consent`),
      context,
      formWith({
        intent: 'oauth-start',
        setup,
        configurationRevision: '1',
        application: 'explicit',
        clientId: 'owner-app',
        clientAuthentication: 'post',
        clientSecret: 'application-secret',
      }),
      target,
    )
    const [request] = startProviderSetupAuthorization.mock.calls[0] as unknown as [
      { application: { mode: { value: string }; authentication: { method: unknown } } },
    ]
    expect(request.application.mode.value).toBe('owner-app')
    expect(request.application.authentication.method).toMatchObject({
      case: 'clientSecretPost',
      value: { secret: 'application-secret' },
    })
  })

  it.each(['oauth-start', 'oauth-resume'])(
    'keeps %s refusal in the editable attempt instead of an unhandled rejection',
    async (intent) => {
      const pending = {
        name: `${name}/providerSetups/${setup}`,
        authorization: { state: SetupAuthorizationState.PENDING, consentRejected: true },
      }
      const api = fakeManagement({
        setups: {
          startProviderSetupAuthorization: () => pending,
          getProviderSetup: () => pending,
        },
      })
      const response = await attemptAction(
        api,
        new Request(`http://127.0.0.1:3000${basePath}?setup=${setup}&namespace=consent`),
        context,
        formWith({ intent, setup, configurationRevision: '1', application: 'automatic' }),
        target,
      )
      expect(response).toMatchObject({
        data: { error: expect.stringContaining('Coral cannot open this sign-in address.') },
        init: { status: 409 },
      })
    },
  )
})

describe('Edit configuration Save', () => {
  const setup = '29a180e5-397c-4844-92de-8bbea6066235'
  const requestId = '33333333-3333-4333-8333-333333333333'
  const parent = 'workspaces/11111111-1111-4111-8111-111111111111'
  const provider = `${parent}/providers/records`
  const basePath = '/providers/records/update'
  const confirmProviderSetupConfiguration = vi.fn()
  const publishProvider = vi.fn()
  const api = fakeManagement({
    setups: { confirmProviderSetupConfiguration },
    providers: { publishProvider },
  })

  beforeEach(() => vi.resetAllMocks())

  async function save(state: ProviderSetupState, interruptsAccess: boolean | undefined) {
    confirmProviderSetupConfiguration.mockResolvedValue(
      create(ProviderSetupSchema, {
        name: `${parent}/providerSetups/${setup}`,
        inputKind: ProviderSetupInputKind.OPENAPI,
        state,
        credentialMethod: CredentialMethod.NONE,
        allowedCredentialRoutes: [{ kind: CredentialRoute_Kind.NONE }],
        configuration: { revision: 3n, errors: {}, interruptsAccess },
      }),
    )
    publishProvider.mockResolvedValue({ outcome: ProviderPublicationOutcome.SUCCEEDED })
    const response = await attemptAction(
      api,
      // Back from an account step leaves its answer in the URL.
      new Request(
        `http://127.0.0.1${basePath}?setup=${setup}&task=configuration&etag=v1&account=caller`,
        { method: 'POST' },
      ),
      context,
      formWith({
        intent: 'configure',
        setup,
        requestId,
        configurationRevision: '2',
        configuration: JSON.stringify({
          displayName: 'Renamed',
          authentication: 'none',
          headers: [],
          parameters: [],
          basic: {
            username: { source: 'literal', value: '' },
            password: { source: 'literal', value: '' },
          },
          credentialFields: {},
          authorizationUrl: '',
          tokenUrl: '',
          scopes: [],
          servers: [],
        }),
      }),
      {
        kind: 'update',
        parent,
        provider,
        successPath: '/providers/records',
        basePath,
        account: 'none',
      },
    )
    if (!(response instanceof Response)) throw new Error('Expected a redirect')
    return new URL(response.headers.get('Location')!, 'http://127.0.0.1')
  }

  // Catches: Save stopping on a page that only repeats the form, or publishing
  // a change that needs a sign-in or consent first.
  it.each([
    ['nothing more', ProviderSetupState.READY, false, true],
    ['consent', ProviderSetupState.READY, true, false],
    ['an unchecked access change', ProviderSetupState.READY, undefined, false],
    ['a sign-in', ProviderSetupState.AWAITING_AUTHORIZATION, false, false],
  ])('publishes at once only when Coral needs %s', async (_, state, interrupts, publishes) => {
    const location = await save(state, interrupts)
    if (publishes) {
      expect(location.pathname).toBe('/providers/records')
      expect(publishProvider).toHaveBeenCalledOnce()
      expect(publishProvider.mock.calls[0][0]).toMatchObject({
        provider,
        requestId,
        etag: 'v1',
        configurationRevision: 3n,
        allowIncompatibleConfiguration: false,
      })
      // Save keeps the account setting.
      expect(publishProvider.mock.calls[0][0].credentialRoute).toBeUndefined()
    } else {
      expect(publishProvider).not.toHaveBeenCalled()
      expect(location.pathname).toBe(basePath)
      expect(location.searchParams.get('task')).toBe('configuration')
      expect(location.searchParams.has('configure')).toBe(false)
    }
  })
})

describe('Edit configuration Cancel', () => {
  const setup = '29a180e5-397c-4844-92de-8bbea6066235'
  const parent = 'workspaces/11111111-1111-4111-8111-111111111111'
  const settings = '/providers/records/settings'
  const basePath = '/providers/records/update'

  // Catches: a discard through a link leaving the Setup open, or a crafted
  // leaveTo sending you off this origin.
  it.each([
    ['without a link', undefined, settings],
    ['to the link', '/workspaces/demo/providers?view=all', '/workspaces/demo/providers?view=all'],
    ['never off this origin', '//evil.example/path', settings],
    ['never to an absolute URL', 'https://evil.example/', settings],
    ['never to a backslash path', '/\\evil.example', settings],
  ])('cancels the Setup and goes %s', async (_, leaveTo, location) => {
    const cancelProviderSetup = vi.fn().mockResolvedValue({})
    const response = await attemptAction(
      fakeManagement({ setups: { cancelProviderSetup } }),
      new Request(`http://127.0.0.1${basePath}?setup=${setup}&task=configuration&etag=v1`, {
        method: 'POST',
      }),
      context,
      formWith({ intent: 'cancel', setup, ...(leaveTo ? { leaveTo } : {}) }),
      {
        kind: 'update',
        parent,
        provider: `${parent}/providers/records`,
        successPath: settings,
        basePath,
        account: 'none',
      },
    )
    if (!(response instanceof Response)) throw new Error('Expected a redirect')
    expect(cancelProviderSetup).toHaveBeenCalledOnce()
    expect(cancelProviderSetup.mock.calls[0][0]).toMatchObject({
      name: `${parent}/providerSetups/${setup}`,
    })
    expect(response.headers.get('Location')).toBe(location)
  })
})
