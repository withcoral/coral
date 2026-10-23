import { randomUUID } from 'node:crypto'
import { data, redirect, type RouterContextProvider } from 'react-router'
import { credentialFieldViews, credentialValuesFrom } from './credential-fields.server'
import { redirectToProviderConsent } from './provider-consent.server'
import { safeInternalPath } from '@/auth/safe-path.server'
import { ConnectError, Code } from '@connectrpc/connect'
import {
  ProviderAccountScope,
  AccountConnectionState,
  ProviderAccountState,
  type ProviderAccount,
} from '@/generated/coral/management/v1/account_pb'
import {
  CredentialMethod,
  CredentialRoute_Kind,
  SetupAuthorizationState,
  type Provider,
} from '@/generated/coral/management/v1/provider_pb'
import {
  actionFailure,
  agentName,
  field,
  formContext,
  management,
  privateHeaders,
  resourcePath,
  workspaceName,
} from './management.server'
import { authorizedWorkspace } from './workspace-route.server'
import { blockerLabel, providerName, routeChoice, uuidOr404 } from './providers.server'
import type { AgentProviderView } from '@/views/management/agent-providers'
import type { Crumb } from '@/views/management/types'
import {
  callbackRedirectUri,
  commitCallbackReturn,
} from './provider-publication-attempt/setup.server'
import type {
  AccountView,
  AccountsView,
  AuthorizationView,
  ConnectionState,
  UserAccountView,
  PersonalAccountState,
  UserAccountsView,
} from '@/views/accounts/types'

export const accountHeaders = { ...privateHeaders, 'Referrer-Policy': 'no-referrer' }

/** Navigation only: never resolve an account or a replacement request from returnTo. */
function safeReturnTo(candidate: string | null): string | null {
  try {
    const decoded = decodeURIComponent(candidate ?? '')
    if (
      candidate &&
      safeInternalPath(candidate) === candidate &&
      safeInternalPath(decoded) !== '/' &&
      !decoded.includes('\\') &&
      ![...decoded].some(
        (character) => character.charCodeAt(0) < 32 || character.charCodeAt(0) === 127,
      )
    )
      return candidate
  } catch {
    // Malformed percent encoding is not a navigation target.
  }
  return null
}
/** Only a supplied return target travels in the URL; its absence shows the Workspace's
 * Activity. */
function accountNavigation(candidate: string | null, path: string, workspace: string) {
  const supplied = safeReturnTo(candidate)
  return {
    path: supplied ? `${path}?${new URLSearchParams({ returnTo: supplied })}` : path,
    returnTo: supplied ?? `${resourcePath(workspace)}/activity`,
  }
}
export function accountFailure(error: unknown): never {
  if (error instanceof Response) throw error
  // Neither transport errors nor malformed callback/secret values are logged or reflected.
  const status =
    error instanceof ConnectError
      ? ((
          {
            [Code.NotFound]: 404,
            [Code.PermissionDenied]: 403,
            [Code.Unauthenticated]: 401,
            [Code.InvalidArgument]: 400,
            [Code.FailedPrecondition]: 409,
            [Code.Aborted]: 409,
          } as Partial<Record<Code, number>>
        )[error.code] ?? 503)
      : 503
  throw new Response(
    status === 404
      ? 'Not found'
      : 'Coral could not complete this action. Reload the page. Check the current settings before you try again.',
    { status, headers: accountHeaders },
  )
}
function accountView(value: ProviderAccount): AccountView {
  const state = (
    {
      [ProviderAccountState.READY]: 'ready',
      [ProviderAccountState.RECONNECT_REQUIRED]: 'reconnect',
      [ProviderAccountState.DISCONNECTED]: 'disconnected',
    } as Partial<Record<ProviderAccountState, AccountView['state']>>
  )[value.state]
  if (!state)
    throw new Response('Unrecognized account state', { status: 503, headers: accountHeaders })
  return {
    id: value.id,
    state,
    automaticRefresh: value.automaticRefresh,
    grantedScope: value.grantedScope,
    canManage: value.canManage,
    connectedAt: timestamp(value.connectedTime),
    configuredByUser: value.configuredByUser,
  }
}
/** The exact authorization owns the path; optional return navigation grants no authority. */
export function authorizationPath(name: string, returnTo?: string | null) {
  const match = /^(workspaces\/[0-9a-f-]{36})\/accountAuthorizations\/([0-9a-f-]{36})$/.exec(name)
  if (!match) throw new Response('Invalid authorization', { status: 503, headers: accountHeaders })
  const path = `/${match[1]}/account-authorizations/${match[2]}`
  return returnTo === undefined ? path : accountNavigation(returnTo, path, match[1]).path
}
/** Where the agent page's connect dialog checks an authorization while you sign in. */
export function authorizationProgressPath(name: string, returnTo: string) {
  const [path, query] = authorizationPath(name, returnTo).split('?')
  return `${path}/progress?${query}`
}
export function authorizationName(params: { workspace: string; authorization: string }) {
  return `${workspaceName(params.workspace)}/accountAuthorizations/${uuidOr404(params.authorization)}`
}
export async function loadAuthorization(
  request: Request,
  context: Readonly<RouterContextProvider>,
  params: { workspace: string; authorization: string },
): Promise<AuthorizationView> {
  const api = management(request, context)
  const value = await api.accounts.getProviderAccountAuthorization({
    name: authorizationName(params),
  })
  const state = (
    {
      [SetupAuthorizationState.PENDING]: 'pending',
      [SetupAuthorizationState.EXCHANGING]: 'exchanging',
      [SetupAuthorizationState.SUCCEEDED]: 'succeeded',
      [SetupAuthorizationState.FAILED]: 'failed',
      [SetupAuthorizationState.CANCELED]: 'canceled',
      [SetupAuthorizationState.EXPIRED]: 'expired',
    } as Partial<Record<SetupAuthorizationState, AuthorizationView['state']>>
  )[value.state]
  if (!state)
    throw new Response('Unrecognized authorization state', { status: 503, headers: accountHeaders })
  const path = authorizationPath(value.name)
  const target = value.target
  let parents: Crumb[]
  let providerParents: Crumb[] | undefined
  let scope: AuthorizationView['scope']
  let retryPath: string | undefined
  switch (target.case) {
    case 'agentAccount':
      parents = await accountParents(api, target.value)
      providerParents = await accountParents(api, { provider: target.value.provider })
      scope = 'agent'
      break
    case 'sharedProvider':
      parents = await accountParents(api, { provider: target.value })
      scope = 'shared'
      break
    case 'userProvider': {
      // Your own account sits under Accounts, not under its provider.
      const [provider] = (await accountParents(api, { provider: target.value })).slice(1)
      const accounts = `/${target.value.split('/').slice(0, 2).join('/')}/accounts`
      parents = [
        { label: 'Accounts', to: accounts },
        ...(provider ? [{ label: provider.label, to: userAccountPath(target.value) }] : []),
      ]
      scope = 'user'
      // A new attempt keeps the Run to return to, but not this page's own confirmation.
      {
        const started = safeReturnTo(new URL(request.url).searchParams.get('returnTo'))
        const page = userAccountPath(target.value)
        retryPath = userAccountPath(
          target.value,
          started && started.split('?')[0] !== page ? started : null,
        )
      }
      break
    }
    default:
      throw new Response('Unrecognized account target', { status: 503, headers: accountHeaders })
  }
  const returnTo = new URL(request.url).searchParams.get('returnTo')
  // A personal or agent connection returns to where it started: the Run, Accounts, its
  // connect page, or the agent's page.
  const started = safeReturnTo(returnTo)
  if (state === 'succeeded' && (scope === 'user' || scope === 'agent') && started)
    throw redirect(started, { headers: accountHeaders })
  return {
    ...accountNavigation(returnTo, path, workspaceName(params.workspace)),
    providerName: (providerParents ?? parents)[1]?.label || 'Unavailable provider',
    parents,
    scope,
    retryPath,
    state,
    userCode: value.userCode,
    oauth:
      value.userCode !== undefined || Boolean(value.consentNavigation) || value.consentRejected,
    applicationFailure: value.failureCode === 'oauth_client_configuration',
    failure: value.failureCode,
    automaticRefresh: value.automaticRefresh,
    grantedScope: value.grantedScope,
    credentialFields: credentialFieldViews(value.credentialFields),
    form: formContext(context),
  }
}
/** An account page sits under the agent it serves, or else under its provider. */
async function accountParents(
  api: ReturnType<typeof management>,
  target: { provider: string; agent?: string },
  provider?: Provider,
): Promise<Crumb[]> {
  const workspace = resourcePath(target.provider.split('/').slice(0, 2).join('/'))
  const list = target.agent
    ? { label: 'Agents', to: `${workspace}/agents` }
    : { label: 'Providers', to: `${workspace}/providers` }
  try {
    // Agent accounts are connected from the agent's own page.
    if (target.agent)
      return [
        list,
        {
          label: (await api.agents.getAgent({ name: target.agent })).displayName,
          to: resourcePath(target.agent),
        },
      ]
    const parent = {
      label: (provider ?? (await api.providers.getProvider({ name: target.provider }))).displayName,
      to: resourcePath(target.provider),
    }
    return [list, parent]
  } catch (error) {
    // A removed parent still leaves its list to return to.
    if (error instanceof ConnectError && error.code === Code.NotFound) return [list]
    throw error
  }
}
function timestamp(value?: { seconds: bigint; nanos: number }) {
  return value ? new Date(Number(value.seconds) * 1000 + value.nanos / 1e6).toISOString() : ''
}

async function accountsContext(
  request: Request,
  context: Readonly<RouterContextProvider>,
  params: { workspace: string; provider: string },
) {
  const api = management(request, context)
  const query = new URL(request.url).searchParams
  const scopeQuery = query.get('scope')
  if (scopeQuery && !['user', 'shared', 'agent'].includes(scopeQuery))
    throw new Response('Not found', { status: 404, headers: accountHeaders })
  const { workspace: ws } = await authorizedWorkspace(request, context, params.workspace)
  const provider = await api.providers.getProvider({ name: providerName(ws.name, params.provider) })
  const route = routeChoice(provider.credentialRoute)
  if (!route)
    throw new Response('Unrecognized account setting', { status: 503, headers: accountHeaders })
  // Your own account lives under Accounts; this page serves shared and Agent accounts.
  if (scopeQuery === 'user')
    throw new Response('Not found', { status: 404, headers: accountHeaders })
  const owner = ws.effectiveRole === 'owner'
  const scope = (scopeQuery ?? 'shared') as AccountsView['scope']
  if (!owner) throw new Response('Forbidden', { status: 403, headers: accountHeaders })
  // User scope always means the authenticated User. Never accept another User's identifier.
  const agent = scope === 'agent' ? agentName(params.workspace, query.get('agent') ?? '') : ''
  const providerPath = `${ws.path}/providers/${params.provider}`
  const path = agent
    ? agentAccountPath(provider.name, agent)
    : `${providerPath}/accounts?scope=${scope}`
  return {
    api,
    workspace: ws,
    parent: ws.name,
    provider: provider.name,
    parents: await accountParents(api, { provider: provider.name, agent }, provider),
    title: agent ? `${provider.displayName} account` : 'Account',
    path,
    active: ws.state === 'active',
    scope,
    route,
    agent,
  }
}
function agentAccountPath(provider: string, agent: string) {
  return `/${provider}/accounts?scope=agent&agent=${encodeURIComponent(agent.split('/').at(-1)!)}`
}

/** One complete snapshot; no client-side joins or repeated paginated scans. */
export async function loadAgentProviders(
  request: Request,
  context: Readonly<RouterContextProvider>,
  agent: string,
): Promise<AgentProviderView[]> {
  const snapshot = await management(request, context).accounts.listAgentProviders({ parent: agent })
  return snapshot.providers.map((inspection) => {
    const provider = inspection.provider
    if (!provider)
      throw new Response('Missing provider inspection', { status: 503, headers: accountHeaders })
    const use = inspection.accountUse
    let account: AgentProviderView['account']
    switch (use.case) {
      case 'noAccount':
        account = { kind: 'none' }
        break
      case 'sharedAccount':
        account = { kind: 'shared', path: `/${provider.name}/accounts?scope=shared` }
        break
      case 'separateAccount':
        account = {
          kind: 'separate',
          path: agentAccountPath(provider.name, agent),
          state: use.value.account ? accountView(use.value.account).state : 'disconnected',
          canConnect: use.value.canConnect,
        }
        break
      default:
        throw new Response('Unrecognized account setting', {
          status: 503,
          headers: accountHeaders,
        })
    }
    return {
      provider: provider.name,
      name: provider.displayName,
      namespace: provider.namespace,
      account,
      blockers: provider.blockers.map(blockerLabel),
    }
  })
}

export async function loadAccounts(
  request: Request,
  context: Readonly<RouterContextProvider>,
  params: { workspace: string; provider: string },
): Promise<AccountsView> {
  const { api, ...view } = await accountsContext(request, context, params)
  const current = await api.accounts.getProviderAccount({
    parent: view.parent,
    provider: view.provider,
    scope: {
      user: ProviderAccountScope.USER,
      shared: ProviderAccountScope.SHARED,
      agent: ProviderAccountScope.AGENT,
    }[view.scope],
    agent: view.agent,
  })
  return {
    workspace: view.workspace,
    parents: view.parents,
    title: view.title,
    path: view.path,
    scope: view.scope,
    active: view.active,
    route: view.route,
    account: current.account ? accountView(current.account) : undefined,
    scopes: current.requestedScopes,
    needsCredential: current.credentialMethod !== CredentialMethod.NONE,
    usesScope: view.scope === 'shared' ? view.route === 'fixed' : view.route === 'caller',
    canConnect: current.canConnect,
    canReconnect: current.canReconnect,
    form: formContext(context),
  }
}
export async function accountsAction(
  request: Request,
  context: Readonly<RouterContextProvider>,
  params: { workspace: string; provider: string },
  form: FormData,
) {
  const v = await accountsContext(request, context, params)
  const intent = field(form, 'intent')
  switch (intent) {
    case 'start':
    case 'reconnect': {
      const target =
        intent === 'reconnect'
          ? {
              case: 'reconnect' as const,
              value: { provider: v.provider, account: field(form, 'account') },
            }
          : v.scope === 'shared'
            ? { case: 'sharedProvider' as const, value: v.provider }
            : v.scope === 'agent'
              ? {
                  case: 'agentAccount' as const,
                  value: { provider: v.provider, agent: v.agent },
                }
              : { case: 'provider' as const, value: v.provider }
      const authorization = await v.api.accounts.startProviderAccountAuthorization({
        parent: v.parent,
        requestId: field(form, 'requestId'),
        redirectUri: callbackRedirectUri(request, context),
        target,
      })
      return redirect(authorizationPath(authorization.name), { headers: accountHeaders })
    }
    case 'disconnect':
      await v.api.accounts.disconnectProviderAccount({
        parent: v.parent,
        provider: v.provider,
        account: field(form, 'account'),
      })
      break
    default:
      throw new Response('Invalid action', { status: 400, headers: accountHeaders })
  }
  return data(
    { error: null, message: 'Account disconnected.', credential: null },
    { headers: accountHeaders },
  )
}
/** Whether an account can make calls; the same for your own and a shared one. */
export function accountConnectionState(account?: ProviderAccount): PersonalAccountState {
  const state = account ? accountView(account).state : 'disconnected'
  return state === 'ready'
    ? 'connected'
    : state === 'reconnect'
      ? 'needs_reconnect'
      : 'not_connected'
}
/** Your own account's state for each separate-account provider, by provider name. */
export async function personalAccountStates(
  api: ReturnType<typeof management>,
  workspace: string,
): Promise<Map<string, PersonalAccountState>> {
  const snapshot = await api.accounts.listUserAccounts({ parent: workspace })
  return new Map(
    snapshot.accounts.map((row) => [row.provider, accountConnectionState(row.account?.account)]),
  )
}
/** Your own account for one provider, under Accounts. Connecting starts here in one click. */
export function userAccountPath(provider: string, returnTo?: string | null) {
  const [, workspace, , id] = provider.split('/')
  const path = `/workspaces/${workspace}/accounts/${id}`
  return returnTo ? `${path}?${new URLSearchParams({ returnTo })}` : path
}

/** Your own accounts, one row per separate-account provider, from one snapshot. */
export async function loadUserAccounts(
  request: Request,
  context: Readonly<RouterContextProvider>,
  params: { workspace: string },
): Promise<UserAccountsView> {
  const { workspace, api } = await authorizedWorkspace(request, context, params.workspace)
  const snapshot = await api.accounts.listUserAccounts({ parent: workspace.name })
  const path = `${workspace.path}/accounts`
  return {
    workspace,
    path,
    accounts: snapshot.accounts.map((row) => {
      const current = row.account
      const account = current?.account
      return {
        provider: row.provider,
        name: row.providerDisplayName,
        state: accountConnectionState(account),
        accountId: account?.id,
        oauth: current ? isOAuth(current.credentialMethod) : false,
        canConnect: Boolean(current?.canConnect || current?.canReconnect),
        canDisconnect:
          Boolean(account?.canManage) && accountConnectionState(account) !== 'not_connected',
        waitingUntil: row.pendingConnection
          ? timestamp(row.pendingConnection.expireTime)
          : undefined,
        connectPath: userAccountPath(row.provider, path),
        detailPath: userAccountPath(row.provider),
      }
    }),
    form: formContext(context),
  }
}
export async function userAccountsAction(
  request: Request,
  context: Readonly<RouterContextProvider>,
  params: { workspace: string },
  form: FormData,
) {
  const { workspace, api } = await authorizedWorkspace(request, context, params.workspace)
  if (field(form, 'intent') !== 'disconnect')
    throw new Response('Invalid action', { status: 400, headers: accountHeaders })
  const provider = field(form, 'provider')
  if (!provider.startsWith(`${workspace.name}/providers/`))
    throw new Response('Not found', { status: 404, headers: accountHeaders })
  return await disconnectUserAccount(api, workspace.name, provider, field(form, 'account'))
}
async function disconnectUserAccount(
  api: ReturnType<typeof management>,
  parent: string,
  provider: string,
  account: string,
) {
  await api.accounts.disconnectProviderAccount({ parent, provider, account })
  return data(
    { error: null, message: 'Account disconnected.', credential: null },
    { headers: accountHeaders },
  )
}

async function userAccountContext(
  request: Request,
  context: Readonly<RouterContextProvider>,
  params: { workspace: string; provider: string },
) {
  const { workspace, api } = await authorizedWorkspace(request, context, params.workspace)
  const provider = await api.providers.getProvider({
    name: providerName(workspace.name, params.provider),
  })
  // Only a separate-account provider has a personal account.
  if (provider.credentialRoute?.kind !== CredentialRoute_Kind.CALLER)
    throw new Response('Not found', { status: 404, headers: accountHeaders })
  const current = await api.accounts.getProviderAccount({
    parent: workspace.name,
    provider: provider.name,
    scope: ProviderAccountScope.USER,
  })
  const query = new URL(request.url).searchParams
  return {
    api,
    workspace,
    provider,
    current,
    returnTo: safeReturnTo(query.get('returnTo')),
    path: userAccountPath(provider.name),
  }
}
export async function loadUserAccount(
  request: Request,
  context: Readonly<RouterContextProvider>,
  params: { workspace: string; provider: string },
): Promise<UserAccountView> {
  const { workspace, provider, current, returnTo } = await userAccountContext(
    request,
    context,
    params,
  )
  const account = current.account ? accountView(current.account) : undefined
  return {
    workspace,
    providerName: provider.displayName,
    path: userAccountPath(provider.name, returnTo),
    returnTo,
    accountsPath: `${workspace.path}/accounts`,
    state: accountConnectionState(current.account),
    account,
    scopes: current.requestedScopes,
    canConnect: current.canConnect || current.canReconnect,
    canDisconnect: Boolean(account?.canManage) && account?.state !== 'disconnected',
    oauth: isOAuth(current.credentialMethod),
    credentialFields: credentialFieldViews(current.credentialFields),
    form: formContext(context),
  }
}
function isOAuth(method: CredentialMethod) {
  return (
    method === CredentialMethod.OAUTH_AUTHORIZATION_CODE ||
    method === CredentialMethod.OAUTH_DEVICE_CODE
  )
}
/**
 * One click from anywhere: start your own authorization and go straight to the provider's
 * sign-in. Secret entry happens on this page. Success returns to `returnTo`, else here.
 */
export async function userAccountAction(
  request: Request,
  context: Readonly<RouterContextProvider>,
  params: { workspace: string; provider: string },
  form: FormData,
): Promise<Response> {
  const { api, workspace, provider, current, returnTo, path } = await userAccountContext(
    request,
    context,
    params,
  )
  const intent = field(form, 'intent')
  const here = userAccountPath(provider.name, returnTo)
  if (intent !== 'connect' && intent !== 'secret')
    throw new Response('Invalid action', { status: 400, headers: accountHeaders })
  const ready = accountConnectionState(current.account) === 'connected'
  // This page shows secret entry unless the account is ready; a settled state shows here too.
  if (
    (intent === 'connect' && !isOAuth(current.credentialMethod) && !ready) ||
    !(current.canConnect || current.canReconnect)
  )
    return redirect(here, { headers: accountHeaders })
  const authorization = await startYourAuthorization(
    api,
    workspace.name,
    provider.name,
    current,
    (form.has('requestId') && field(form, 'requestId')) || randomUUID(),
    callbackRedirectUri(request, context),
  )
  // Without a Run to return to, this page shows the connection.
  const progress = authorizationPath(authorization.name, returnTo ?? path)
  if (intent === 'secret') {
    try {
      await api.accounts.submitProviderAccountSecret({
        name: authorization.name,
        credentialValues: credentialValuesFrom(form),
      })
    } catch (error) {
      // A settled or rejected entry shows on its progress page, where you can retry or cancel.
      if (!(error instanceof ConnectError && error.code === Code.FailedPrecondition)) throw error
    }
    return redirect(progress, { headers: accountHeaders })
  }
  // Secret replacement of a ready account happens on the progress page.
  const consent = Boolean(authorization.consentNavigation)
  if (!consent) return redirect(progress, { headers: accountHeaders })
  return await consentRedirect(api, context, authorization.name, progress)
}

/** Starts your own account's connection: a reconnect when Coral allows one, else a new one. */
function startYourAuthorization(
  api: ReturnType<typeof management>,
  parent: string,
  provider: string,
  current: Awaited<ReturnType<typeof userAccountContext>>['current'],
  requestId: string,
  redirectUri: string,
) {
  return api.accounts.startProviderAccountAuthorization({
    parent,
    requestId,
    redirectUri,
    target:
      current.canReconnect && current.account
        ? { case: 'reconnect', value: { provider, account: current.account.id } }
        : { case: 'provider', value: provider },
  })
}

/**
 * Connects your own account in a dialog, without leaving the page it opens on.
 * The dialog follows the returned progress. A sign-in returns to `returnTo`,
 * the page you came from, else to your account page.
 */
export async function connectYourAccountAction(
  request: Request,
  context: Readonly<RouterContextProvider>,
  params: { workspace: string; provider: string },
  form: FormData,
) {
  try {
    const { api, workspace, provider, current, returnTo, path } = await userAccountContext(
      request,
      context,
      params,
    )
    if (!(current.canConnect || current.canReconnect))
      return data(
        { error: 'You cannot connect this account now.', credential: null },
        { headers: accountHeaders },
      )
    const authorization = await startYourAuthorization(
      api,
      workspace.name,
      provider.name,
      current,
      field(form, 'requestId'),
      callbackRedirectUri(request, context),
    )
    return data(
      { error: null, progress: authorizationProgressPath(authorization.name, returnTo ?? path) },
      { headers: accountHeaders },
    )
  } catch (error) {
    if (error instanceof Response) throw error
    return actionFailure(error, 'Coral could not connect your account. ')
  }
}

/** Disconnects your own account for this provider. */
export async function disconnectUserAccountAction(
  request: Request,
  context: Readonly<RouterContextProvider>,
  params: { workspace: string; provider: string },
) {
  const { api, workspace, provider, current } = await userAccountContext(request, context, params)
  if (!current.account) throw new Response('Not found', { status: 404, headers: accountHeaders })
  return await disconnectUserAccount(api, workspace.name, provider.name, current.account.id)
}

/** Sends the browser to fresh Provider consent; the callback reopens `path`. */
async function consentRedirect(
  api: ReturnType<typeof management>,
  context: Readonly<RouterContextProvider>,
  name: string,
  path: string,
) {
  const response = await redirectToProviderConsent(api, { kind: 'account', name })
  response.headers.set(
    'Set-Cookie',
    await commitCallbackReturn({ setup: name, returnTo: path }, context),
  )
  // Desktop keeps its internal app page open while the system browser signs in.
  response.headers.set('X-Coral-Authorization-Return-To', path)
  return response
}

export async function authorizationAction(
  request: Request,
  context: Readonly<RouterContextProvider>,
  params: { workspace: string; authorization: string },
  form: FormData,
): Promise<Response> {
  const api = management(request, context)
  const name = authorizationName(params)
  const path = authorizationPath(name, new URL(request.url).searchParams.get('returnTo'))
  try {
    switch (field(form, 'intent')) {
      case 'navigate':
        return await consentRedirect(api, context, name, path)
      case 'secret':
        await api.accounts.submitProviderAccountSecret({
          name,
          credentialValues: credentialValuesFrom(form),
        })
        break
      case 'cancel':
        await api.accounts.cancelProviderAccountAuthorization({ name })
        break
      default:
        throw new Response('Invalid action', { status: 400, headers: accountHeaders })
    }
  } catch (error) {
    if (
      (error instanceof ConnectError && error.code === Code.FailedPrecondition) ||
      (error instanceof Response && error.status === 409)
    ) {
      const current = await api.accounts.getProviderAccountAuthorization({ name })
      if (
        ![
          SetupAuthorizationState.EXCHANGING,
          SetupAuthorizationState.SUCCEEDED,
          SetupAuthorizationState.FAILED,
          SetupAuthorizationState.CANCELED,
          SetupAuthorizationState.EXPIRED,
        ].includes(current.state)
      )
        throw error
      // A stale form can only return to this authorization's actual progress.
    } else throw error
  }
  // Post/redirect/get drops submitted material, including on a safe domain failure.
  return redirect(path, {
    headers: accountHeaders,
  })
}

export function connectionState(value: AccountConnectionState): ConnectionState {
  const state = (
    {
      [AccountConnectionState.PENDING]: 'pending',
      [AccountConnectionState.CONNECTED]: 'connected',
      [AccountConnectionState.DECLINED]: 'declined',
      [AccountConnectionState.EXPIRED]: 'expired',
      [AccountConnectionState.ROUTE_CHANGED]: 'route_changed',
      [AccountConnectionState.AUTHORIZATION_CHANGED]: 'authorization_changed',
    } as Partial<Record<AccountConnectionState, ConnectionState>>
  )[value]
  if (!state)
    throw new Response('Unrecognized connection state', { status: 503, headers: accountHeaders })
  return state
}
