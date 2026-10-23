import { authRouteTestArgs, authTestContext } from '@/auth/server-context.test-helper'
import { loader as oauthCallback } from '@/routes/provider-setup-oauth-callback'
import { commitCallbackReturn } from './provider-publication-attempt/setup.server'
import { fakeManagement, fakeManagementTransport } from './management.test-support'
import { Code, ConnectError } from '@connectrpc/connect'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import {
  AccountConnectionState,
  ProviderAccountState,
} from '@/generated/coral/management/v1/account_pb'
import {
  CredentialMethod,
  CredentialRoute_Kind,
  SetupAuthorizationState,
} from '@/generated/coral/management/v1/provider_pb'
import {
  loadAccounts,
  authorizationAction,
  loadAuthorization,
  loadUserAccount,
  userAccountAction,
  connectYourAccountAction,
  loadUserAccounts,
} from './accounts.server'

const mocks = vi.hoisted(() => ({
  workspace: vi.fn(),
  getProvider: vi.fn(),
  getAgent: vi.fn(),
  getProviderAccount: vi.fn(),
  listUserAccounts: vi.fn(),
  startProviderAccountAuthorization: vi.fn(),
  getProviderAccountAuthorization: vi.fn(),
  submitProviderAccountSecret: vi.fn(),
  cancelProviderAccountAuthorization: vi.fn(),
  resolveAccountConnectionRequest: vi.fn(),
  completeProviderSetupAuthorization: vi.fn(),
}))
const handlers = {
  providers: { getProvider: mocks.getProvider },
  agents: { getAgent: mocks.getAgent },
  accounts: {
    getProviderAccount: mocks.getProviderAccount,
    listUserAccounts: mocks.listUserAccounts,
    startProviderAccountAuthorization: mocks.startProviderAccountAuthorization,
    getProviderAccountAuthorization: mocks.getProviderAccountAuthorization,
    submitProviderAccountSecret: mocks.submitProviderAccountSecret,
    cancelProviderAccountAuthorization: mocks.cancelProviderAccountAuthorization,
    resolveAccountConnectionRequest: mocks.resolveAccountConnectionRequest,
    completeProviderSetupAuthorization: mocks.completeProviderSetupAuthorization,
  },
}
const api = fakeManagement(handlers)
const transport = fakeManagementTransport(handlers)
vi.mock('./workspace-route.server', () => ({
  authorizedWorkspace: mocks.workspace,
}))
vi.mock('./coral-request.server', () => ({
  managementTransportForRequest: () => transport,
}))

const workspace = '29a180e5-397c-4844-92de-8bbea6066235'
const providerId = '11111111-1111-4111-8111-111111111111'
const agentId = '22222222-2222-4222-8222-222222222222'
const base = `/workspaces/${workspace}`
const context = authTestContext(null)

afterEach(() => vi.unstubAllGlobals())

beforeEach(() => {
  vi.resetAllMocks()
  mocks.workspace.mockResolvedValue({
    api,
    workspace: {
      name: `workspaces/${workspace}`,
      path: base,
      effectiveRole: 'owner',
      state: 'active',
    },
  })
  mocks.getProvider.mockResolvedValue({
    name: `workspaces/${workspace}/providers/${providerId}`,
    displayName: 'GitHub',
    namespace: 'github',
    credentialRoute: { kind: CredentialRoute_Kind.FIXED },
  })
  mocks.getAgent.mockResolvedValue({ displayName: 'Nightly reconciler' })
  mocks.getProviderAccount.mockResolvedValue({ requestedScopes: [] })
})

function load(query: string) {
  return loadAccounts(
    new Request(`http://reef.test${base}/providers/${providerId}/accounts?${query}`),
    context,
    { workspace, provider: providerId },
  )
}

const provider = `workspaces/${workspace}/providers/${providerId}`
const connectBase = `${base}/accounts/${providerId}`
const run = `${base}/runs/44444444-4444-4444-8444-444444444444?request=original-request`
const authorizationId = '55555555-5555-4555-8555-555555555555'
const authorizationResource = `workspaces/${workspace}/accountAuthorizations/${authorizationId}`

function separateAccounts(account: object) {
  mocks.getProvider.mockResolvedValue({
    name: provider,
    displayName: 'Linear',
    namespace: 'linear',
    credentialRoute: { kind: CredentialRoute_Kind.CALLER },
  })
  mocks.getProviderAccount.mockResolvedValue({
    requestedScopes: [],
    credentialFields: {},
    ...account,
  })
}
function connectRequest(returnTo?: string) {
  const query = returnTo === undefined ? '' : `?returnTo=${encodeURIComponent(returnTo)}`
  return new Request(`http://localhost${connectBase}${query}`, {
    method: 'POST',
  })
}
function connectForm(intent: string, fields: Record<string, string> = {}) {
  const form = new FormData()
  form.set('intent', intent)
  form.set('requestId', '66666666-6666-4666-8666-666666666666')
  for (const [key, value] of Object.entries(fields)) form.set(key, value)
  return form
}
async function signInCallback(consent: Response) {
  mocks.completeProviderSetupAuthorization.mockResolvedValue({
    workflow: { case: 'accountAuthorization', value: authorizationResource },
  })
  return oauthCallback(
    authRouteTestArgs(
      new Request('http://localhost/provider-setup/oauth/callback?state=opaque&code=dummy-code', {
        headers: { cookie: consent.headers.get('Set-Cookie')!.split(';')[0] },
      }),
      {},
      null,
    ),
  )
}
function signedIn() {
  mocks.getProviderAccountAuthorization.mockResolvedValue({
    name: authorizationResource,
    state: SetupAuthorizationState.SUCCEEDED,
    target: { case: 'userProvider', value: provider },
    credentialFields: {},
    grantedScope: '',
  })
}
async function redirectOf(promise: Promise<unknown>) {
  const thrown = await promise.then(
    () => undefined,
    (error: unknown) => error,
  )
  expect(thrown).toBeInstanceOf(Response)
  return (thrown as Response).headers.get('Location')
}

it('connects in one click and returns to the waiting Run after sign-in', async () => {
  separateAccounts({
    canConnect: true,
    credentialMethod: CredentialMethod.OAUTH_AUTHORIZATION_CODE,
  })
  mocks.startProviderAccountAuthorization.mockResolvedValue({
    name: authorizationResource,
    consentNavigation: { url: 'https://provider.test/consent?state=opaque' },
  })
  mocks.getProviderAccountAuthorization.mockResolvedValue({
    name: authorizationResource,
    state: SetupAuthorizationState.PENDING,
    consentNavigation: { url: 'https://provider.test/consent?state=opaque' },
  })
  const consent = await userAccountAction(
    connectRequest(run),
    context,
    { workspace, provider: providerId },
    connectForm('connect'),
  )
  expect(consent.headers.get('Location')).toBe('https://provider.test/consent?state=opaque')
  expect(mocks.startProviderAccountAuthorization).toHaveBeenCalledWith(
    expect.objectContaining({ target: { case: 'provider', value: provider } }),
    expect.anything(),
  )
  const callback = await signInCallback(consent)
  const progress = callback.headers.get('Location')!
  expect(progress).toBe(consent.headers.get('X-Coral-Authorization-Return-To'))
  signedIn()
  expect(
    await redirectOf(
      loadAuthorization(new Request(new URL(progress, 'http://localhost')), context, {
        workspace,
        authorization: authorizationId,
      }),
    ),
  ).toBe(run)
})

it('lands on your connected account when no Run started the connection', async () => {
  separateAccounts({
    canConnect: true,
    credentialMethod: CredentialMethod.OAUTH_AUTHORIZATION_CODE,
  })
  mocks.startProviderAccountAuthorization.mockResolvedValue({
    name: authorizationResource,
    consentNavigation: { url: 'https://provider.test/consent' },
  })
  mocks.getProviderAccountAuthorization.mockResolvedValue({
    name: authorizationResource,
    state: SetupAuthorizationState.PENDING,
    consentNavigation: { url: 'https://provider.test/consent' },
  })
  const consent = await userAccountAction(
    connectRequest(),
    context,
    { workspace, provider: providerId },
    connectForm('connect'),
  )
  const progress = (await signInCallback(consent)).headers.get('Location')!
  signedIn()
  const landing = await redirectOf(
    loadAuthorization(new Request(new URL(progress, 'http://localhost')), context, {
      workspace,
      authorization: authorizationId,
    }),
  )
  expect(landing).toBe(connectBase)
  separateAccounts({
    account: {
      id: 'account',
      state: ProviderAccountState.READY,
      canManage: true,
    },
    canReconnect: true,
    credentialMethod: CredentialMethod.OAUTH_AUTHORIZATION_CODE,
  })
  const view = await loadUserAccount(new Request(`http://localhost${landing}`), context, {
    workspace,
    provider: providerId,
  })
  expect(view).toMatchObject({
    state: 'connected',
    returnTo: null,
  })
})

it('reconnects your incompatible account instead of starting another one', async () => {
  separateAccounts({
    account: {
      id: 'account',
      state: ProviderAccountState.RECONNECT_REQUIRED,
      canManage: true,
    },
    canReconnect: true,
    credentialMethod: CredentialMethod.OAUTH_AUTHORIZATION_CODE,
  })
  mocks.startProviderAccountAuthorization.mockResolvedValue({
    name: authorizationResource,
  })
  const response = await userAccountAction(
    connectRequest(run),
    context,
    { workspace, provider: providerId },
    connectForm('connect'),
  )
  expect(mocks.startProviderAccountAuthorization).toHaveBeenCalledWith(
    expect.objectContaining({
      target: {
        case: 'reconnect',
        value: expect.objectContaining({ provider, account: 'account' }),
      },
    }),
    expect.anything(),
  )
  // Without consent navigation, as for a device code, the progress page takes over.
  const progress = new URL(response.headers.get('Location')!, 'http://localhost')
  expect(progress.pathname).toBe(`${base}/account-authorizations/${authorizationId}`)
  expect(progress.searchParams.get('returnTo')).toBe(run)
})

it('enters secret credentials on the connect page without echoing them', async () => {
  separateAccounts({
    canConnect: true,
    credentialMethod: CredentialMethod.BEARER,
  })
  const shown = await userAccountAction(
    connectRequest(run),
    context,
    { workspace, provider: providerId },
    connectForm('connect'),
  )
  expect(shown.headers.get('Location')).toBe(`${connectBase}?returnTo=${encodeURIComponent(run)}`)
  expect(mocks.startProviderAccountAuthorization).not.toHaveBeenCalled()
  mocks.startProviderAccountAuthorization.mockResolvedValue({
    name: authorizationResource,
  })
  const entered = await userAccountAction(
    connectRequest(run),
    context,
    { workspace, provider: providerId },
    connectForm('secret', { 'credential.token': 'dummy-token' }),
  )
  expect(mocks.submitProviderAccountSecret).toHaveBeenCalledWith(
    expect.objectContaining({ name: authorizationResource }),
    expect.anything(),
  )
  const progress = entered.headers.get('Location')!
  expect(progress).not.toContain('dummy-token')
  expect(new URL(progress, 'http://localhost').searchParams.get('returnTo')).toBe(run)
})

it('shows a rejected secret entry on its progress page instead of an error page', async () => {
  separateAccounts({
    canConnect: true,
    credentialMethod: CredentialMethod.BEARER,
  })
  mocks.startProviderAccountAuthorization.mockResolvedValue({
    name: authorizationResource,
  })
  mocks.submitProviderAccountSecret.mockRejectedValue(
    new ConnectError('settled', Code.FailedPrecondition),
  )
  const response = await userAccountAction(
    connectRequest(run),
    context,
    { workspace, provider: providerId },
    connectForm('secret', { 'credential.token': 'dummy-token' }),
  )
  const progress = new URL(response.headers.get('Location')!, 'http://localhost')
  expect(progress.pathname).toBe(`${base}/account-authorizations/${authorizationId}`)
  expect(progress.searchParams.get('returnTo')).toBe(run)
})

it('shows the connected account after sign-in started from its own account page', async () => {
  mocks.getProviderAccountAuthorization.mockResolvedValue({
    name: authorizationResource,
    state: SetupAuthorizationState.PENDING,
    consentNavigation: { url: 'https://provider.test/consent' },
  })
  const form = new FormData()
  form.set('intent', 'navigate')
  const consent = await authorizationAction(
    new Request(`http://localhost${base}/account-authorizations/${authorizationId}`, {
      method: 'POST',
    }),
    context,
    { workspace, authorization: authorizationId },
    form,
  )
  const progress = (await signInCallback(consent)).headers.get('Location')!
  signedIn()
  // No run started this sign-in, so the page itself confirms the connection.
  const view = await loadAuthorization(
    new Request(new URL(progress, 'http://localhost')),
    context,
    {
      workspace,
      authorization: authorizationId,
    },
  )
  expect(view).toMatchObject({ state: 'succeeded', returnTo: `/workspaces/${workspace}/activity` })
})

it('replaces the secret of a ready account on its progress page', async () => {
  separateAccounts({
    account: {
      id: 'account',
      state: ProviderAccountState.READY,
      canManage: true,
    },
    canReconnect: true,
    credentialMethod: CredentialMethod.BEARER,
  })
  mocks.startProviderAccountAuthorization.mockResolvedValue({
    name: authorizationResource,
  })
  const response = await userAccountAction(
    connectRequest(),
    context,
    { workspace, provider: providerId },
    connectForm('connect'),
  )
  expect(mocks.startProviderAccountAuthorization).toHaveBeenCalledWith(
    expect.objectContaining({
      target: {
        case: 'reconnect',
        value: expect.objectContaining({ provider, account: 'account' }),
      },
    }),
    expect.anything(),
  )
  const progress = new URL(response.headers.get('Location')!, 'http://localhost')
  expect(progress.pathname).toBe(`${base}/account-authorizations/${authorizationId}`)
})

it('keeps your own account off the provider account page', async () => {
  mocks.getProvider.mockResolvedValue({
    name: provider,
    displayName: 'Linear',
    credentialRoute: { kind: CredentialRoute_Kind.CALLER },
  })
  await expect(load('scope=user')).rejects.toMatchObject({ status: 404 })
})

it('refuses the provider account page to a workspace member who is not an owner', async () => {
  mocks.workspace.mockResolvedValue({
    api,
    workspace: {
      name: `workspaces/${workspace}`,
      path: base,
      effectiveRole: 'member',
    },
  })
  await expect(load('')).rejects.toMatchObject({ status: 403 })
})

function loadAuthorizationReturningTo(returnTo: string) {
  return loadAuthorization(
    new Request(`http://localhost${base}?returnTo=${encodeURIComponent(returnTo)}`),
    context,
    { workspace, authorization: authorizationId },
  )
}

it('starts another attempt from your account page, keeping only a Run to return to', async () => {
  mocks.getProviderAccountAuthorization.mockResolvedValue({
    name: authorizationResource,
    state: SetupAuthorizationState.FAILED,
    failureCode: 'oauth_code_rejected',
    target: { case: 'userProvider', value: provider },
    credentialFields: {},
    grantedScope: '',
  })
  expect((await loadAuthorizationReturningTo(run)).retryPath).toBe(
    `${connectBase}?returnTo=${encodeURIComponent(run)}`,
  )
  expect((await loadAuthorizationReturningTo(connectBase)).retryPath).toBe(connectBase)
})

it('offers no personal connection for a provider that shares one account', async () => {
  const response = await loadUserAccount(new Request(`http://localhost${connectBase}`), context, {
    workspace,
    provider: providerId,
  }).catch((error: unknown) => error)
  expect(response).toBeInstanceOf(Response)
  expect((response as Response).status).toBe(404)
  expect(mocks.getProviderAccount).not.toHaveBeenCalled()
})

it('lists your accounts and the run that waits on one', async () => {
  mocks.listUserAccounts.mockResolvedValue({
    accounts: [
      {
        provider,
        providerDisplayName: 'Linear',
        account: { canConnect: true, canReconnect: false },
        pendingConnection: {
          state: AccountConnectionState.PENDING,
          expireTime: { seconds: 1_800_000_000n, nanos: 0 },
        },
      },
      {
        provider: `workspaces/${workspace}/providers/${agentId}`,
        providerDisplayName: 'GitHub',
        account: {
          account: {
            id: 'account',
            state: ProviderAccountState.READY,
            canManage: true,
          },
          canConnect: false,
          canReconnect: true,
        },
      },
    ],
  })
  const view = await loadUserAccounts(new Request(`http://localhost${base}/accounts`), context, {
    workspace,
  })
  expect(view.accounts).toEqual([
    expect.objectContaining({
      name: 'Linear',
      state: 'not_connected',
      canConnect: true,
      waitingUntil: '2027-01-15T08:00:00.000Z',
      connectPath: `${connectBase}?returnTo=${encodeURIComponent(`${base}/accounts`)}`,
      detailPath: connectBase,
    }),
    expect.objectContaining({
      name: 'GitHub',
      state: 'connected',
      accountId: 'account',
      canDisconnect: true,
      waitingUntil: undefined,
    }),
  ])
})

it('does not redirect a successful shared or agent connection', async () => {
  mocks.getProviderAccountAuthorization.mockResolvedValue({
    name: authorizationResource,
    state: SetupAuthorizationState.SUCCEEDED,
    target: { case: 'sharedProvider', value: provider },
    credentialFields: {},
    grantedScope: '',
  })
  const view = await loadAuthorization(
    new Request(`http://localhost${base}?returnTo=${encodeURIComponent(run)}`),
    context,
    { workspace, authorization: authorizationId },
  )
  expect(view).toMatchObject({ state: 'succeeded', returnTo: run })
})

it.each(['navigate', 'secret'] as const)(
  'shows the exact expired authorization when %s loses an expiry race',
  async (intent) => {
    const authorization = '55555555-5555-4555-8555-555555555555'
    const name = `workspaces/${workspace}/accountAuthorizations/${authorization}`
    mocks.getProviderAccountAuthorization.mockResolvedValue({
      name,
      state: SetupAuthorizationState.EXPIRED,
    })
    mocks.submitProviderAccountSecret.mockRejectedValue(
      new ConnectError('expired', Code.FailedPrecondition),
    )
    const request = new Request(
      `http://127.0.0.1:5173${base}/account-authorizations/${authorization}?returnTo=%2Factivity%3Frequest%3Doriginal`,
    )
    const form = new FormData()
    form.set('intent', intent)
    form.set('credential.token', 'dummy-token')
    const response = await authorizationAction(request, context, { workspace, authorization }, form)
    expect(response.headers.get('Location')).toBe(
      `${base}/account-authorizations/${authorization}?returnTo=%2Factivity%3Frequest%3Doriginal`,
    )
    expect(mocks.startProviderAccountAuthorization).not.toHaveBeenCalled()
  },
)

it.each([
  '/safe/..//evil.test',
  '/another-authorization?returnTo=https%3A%2F%2Fevil.test',
  '/another-authorization?returnTo=%2F..%2F%2Fevil.test',
])(
  'rebuilds the account callback from its exact workflow instead of cookie URL authority: %s',
  async (returnTo) => {
    const authorization = '55555555-5555-4555-8555-555555555555'
    const name = `workspaces/${workspace}/accountAuthorizations/${authorization}`
    const cookie = await commitCallbackReturn({ setup: name, returnTo }, context)
    mocks.completeProviderSetupAuthorization.mockResolvedValue({
      workflow: { case: 'accountAuthorization', value: name },
    })
    const callback = await oauthCallback(
      authRouteTestArgs(
        new Request('http://localhost/provider-setup/oauth/callback?state=opaque&code=dummy-code', {
          headers: { cookie: cookie.split(';')[0] },
        }),
        {},
        null,
      ),
    )
    expect(callback.headers.get('Location')).toBe(`${base}/account-authorizations/${authorization}`)
  },
)

it('does not hide a pending authorization rejection behind an expiry redirect', async () => {
  const error = new ConnectError('not available', Code.FailedPrecondition)
  mocks.submitProviderAccountSecret.mockRejectedValue(error)
  mocks.getProviderAccountAuthorization.mockResolvedValue({
    state: SetupAuthorizationState.PENDING,
  })
  const form = new FormData()
  form.set('intent', 'secret')
  form.set('credential.token', 'dummy-token')
  await expect(
    authorizationAction(
      new Request(`http://localhost${base}?returnTo=%2Factivity`),
      context,
      {
        workspace,
        authorization: '55555555-5555-4555-8555-555555555555',
      },
      form,
    ),
  ).rejects.toMatchObject({
    code: Code.FailedPrecondition,
    rawMessage: 'not available',
  })
})

it('keeps the original selection when you cancel instead of completing', async () => {
  const request = new Request(
    `http://127.0.0.1:5173${base}?returnTo=%2Factivity%3Frequest%3Doriginal`,
  )
  const form = new FormData()
  form.set('intent', 'cancel')
  const canceled = await authorizationAction(
    request,
    context,
    { workspace, authorization: '55555555-5555-4555-8555-555555555555' },
    form,
  )
  expect(canceled.headers.get('Location')).toBe(
    `${base}/account-authorizations/55555555-5555-4555-8555-555555555555?returnTo=%2Factivity%3Frequest%3Doriginal`,
  )
})

it('places a provider account under its provider', async () => {
  const view = await load('scope=shared')
  expect(view.parents).toEqual([
    { label: 'Providers', to: `${base}/providers` },
    { label: 'GitHub', to: `${base}/providers/${providerId}` },
  ])
  expect(view.title).toBe('Account')
})

it('places an agent account under its agent, not the provider', async () => {
  const view = await load(`scope=agent&agent=${agentId}`)
  expect(view.parents).toEqual([
    { label: 'Agents', to: `${base}/agents` },
    { label: 'Nightly reconciler', to: `${base}/agents/${agentId}` },
  ])
  expect(view.title).toBe('GitHub account')
})

// safe-path.server.test.ts owns absolute, protocol-relative, traversal, empty and
// overlong targets. These rows are the ones safeReturnTo adds on top.
it.each([
  'coral-app://app/activity',
  'coral-app://elsewhere/activity',
  '/%2e%2e//evil.test',
  '/\\\\evil.test',
  '/activity?request=%ZZ',
  '/activity?request=%0aevil',
  '/activity?request=\nevil',
  '/%2f%2fevil.test',
])('does not hand an unsafe or malformed return target to the browser: %s', async (returnTo) => {
  separateAccounts({
    canConnect: true,
    credentialMethod: CredentialMethod.OAUTH_AUTHORIZATION_CODE,
  })
  const view = await loadUserAccount(
    new Request(`http://reef.test${connectBase}?returnTo=${encodeURIComponent(returnTo)}`),
    context,
    { workspace, provider: providerId },
  )
  expect(view.returnTo).toBeNull()
  expect(view.path).toBe(connectBase)
})

it('names the provider, not the agent, during Agent account authorization', async () => {
  const authorization = '55555555-5555-4555-8555-555555555555'
  mocks.getProviderAccountAuthorization.mockResolvedValue({
    name: `workspaces/${workspace}/accountAuthorizations/${authorization}`,
    state: SetupAuthorizationState.PENDING,
    target: {
      case: 'agentAccount',
      value: {
        provider: `workspaces/${workspace}/providers/${providerId}`,
        agent: `workspaces/${workspace}/agents/${agentId}`,
      },
    },
    credentialFields: {},
    grantedScope: '',
  })
  const view = await loadAuthorization(new Request(`http://localhost${base}`), context, {
    workspace,
    authorization,
  })
  expect(view).toMatchObject({
    providerName: 'GitHub',
    scope: 'agent',
    returnTo: `/workspaces/${workspace}/activity`,
  })
  expect(view.parents.at(-1)?.label).toBe('Nightly reconciler')
})

// Catches: the agent's connect dialog stranding you on the authorization page after sign-in.
it('returns a succeeded Agent account authorization to the page that started it', async () => {
  const authorization = '55555555-5555-4555-8555-555555555555'
  mocks.getProviderAccountAuthorization.mockResolvedValue({
    name: `workspaces/${workspace}/accountAuthorizations/${authorization}`,
    state: SetupAuthorizationState.SUCCEEDED,
    target: {
      case: 'agentAccount',
      value: {
        provider: `workspaces/${workspace}/providers/${providerId}`,
        agent: `workspaces/${workspace}/agents/${agentId}`,
      },
    },
    credentialFields: {},
    grantedScope: '',
  })
  const agentPage = `/workspaces/${workspace}/agents/${agentId}`
  const request = new Request(
    `http://localhost${base}?${new URLSearchParams({ returnTo: agentPage })}`,
  )
  const thrown = await loadAuthorization(request, context, { workspace, authorization }).catch(
    (error: unknown) => error,
  )
  expect(thrown).toBeInstanceOf(Response)
  expect((thrown as Response).headers.get('Location')).toBe(agentPage)
})

// Catches: an intent the page no longer offers reaching Coral as a change.
it.each(['bind', 'unbind', 'fixed', 'route', 'reconnect'])(
  'refuses the retired %s intent on your account page without starting a connection',
  async (intent) => {
    separateAccounts({
      account: {
        id: 'account',
        state: ProviderAccountState.READY,
        canManage: true,
      },
      canReconnect: true,
      credentialMethod: CredentialMethod.OAUTH_AUTHORIZATION_CODE,
    })
    const refused = await userAccountAction(
      connectRequest(),
      context,
      { workspace, provider: providerId },
      connectForm(intent, { account: 'account' }),
    ).then(
      () => undefined,
      (error: unknown) => error,
    )
    expect(refused).toBeInstanceOf(Response)
    expect((refused as Response).status).toBe(400)
    expect(mocks.startProviderAccountAuthorization).not.toHaveBeenCalled()
  },
)

// Catches: connecting your own account in a dialog starting a second account in
// place of a reconnect, a sign-in that does not return to the page you came from,
// or a start for a provider whose calls use the shared account.
describe('connecting your account in a dialog', () => {
  const page = `${base}/accounts/${providerId}`
  const connect = (returnTo?: string) =>
    connectYourAccountAction(
      new Request(
        `http://localhost${page}.data${returnTo ? `?${new URLSearchParams({ returnTo })}` : ''}`,
        { method: 'POST' },
      ),
      context,
      { workspace, provider: providerId },
      connectForm('connectAccount'),
    )

  it.each([
    ['a new account', { canConnect: true }, { case: 'provider', value: provider }],
    [
      'a reconnect',
      {
        account: { id: 'account', state: ProviderAccountState.RECONNECT_REQUIRED, canManage: true },
        canReconnect: true,
      },
      { case: 'reconnect', value: expect.objectContaining({ provider, account: 'account' }) },
    ],
  ])('starts %s', async (_, account, target) => {
    separateAccounts({ ...account, credentialMethod: CredentialMethod.OAUTH_AUTHORIZATION_CODE })
    mocks.startProviderAccountAuthorization.mockResolvedValue({ name: authorizationResource })
    await connect()
    expect(mocks.startProviderAccountAuthorization).toHaveBeenCalledWith(
      expect.objectContaining({ target }),
      expect.anything(),
    )
  })

  it.each([
    [
      'settings',
      `${base}/providers/${providerId}/settings`,
      `${base}/providers/${providerId}/settings`,
    ],
    ['Accounts', `${base}/accounts`, `${base}/accounts`],
    ['your account page', undefined, page],
  ])('returns a sign-in from %s to that page', async (_, returnTo, expected) => {
    separateAccounts({
      canConnect: true,
      credentialMethod: CredentialMethod.OAUTH_AUTHORIZATION_CODE,
    })
    mocks.startProviderAccountAuthorization.mockResolvedValue({ name: authorizationResource })
    const result = (await connect(returnTo)) as unknown as { data: { progress: string } }
    const progress = new URL(result.data.progress, 'http://localhost')
    expect(progress.pathname).toBe(`${base}/account-authorizations/${authorizationId}/progress`)
    expect(progress.searchParams.get('returnTo')).toBe(expected)
  })

  it('starts nothing when Coral refuses the connection', async () => {
    separateAccounts({ canConnect: false, canReconnect: false })
    const result = (await connect()) as unknown as { data: { error: string } }
    expect(result.data.error).toBe('You cannot connect this account now.')
    expect(mocks.startProviderAccountAuthorization).not.toHaveBeenCalled()
  })

  it('has no account of yours to connect when calls use the shared account', async () => {
    await expect(connect()).rejects.toMatchObject({ status: 404 })
    expect(mocks.startProviderAccountAuthorization).not.toHaveBeenCalled()
  })
})
