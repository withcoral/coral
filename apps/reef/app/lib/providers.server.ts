import { Code, ConnectError } from '@connectrpc/connect'
import { data } from 'react-router'
import {
  CredentialRoute_Kind,
  ProviderBlocker,
  ProviderState,
  type CredentialRoute,
  type OperationHints,
  type Provider,
  type ProviderOperation,
} from '@/generated/coral/management/v1/provider_pb'
import {
  OperationAccessState,
  type OperationPolicy,
} from '@/generated/coral/management/v1/policy_pb'
import type {
  AccountChoice,
  AccountLabel,
  OperationHintsView,
  OperationView,
  ProviderSettingsView,
  ProviderView,
} from '@/views/providers/types'
import type { PersonalAccountState } from '@/views/accounts/types'
import { accountLabels } from '@/views/providers/account-labels'
import { isBuiltinProvider, operationCallPath } from './call-root'
import { privateHeaders, type ManagementApi } from './management.server'
import { OPENAPI_DOCUMENT_SIZE_LABEL } from './provider-input-limits'

// A malformed identifier names nothing, so it reads as not found.
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i
export function uuidOr404(id: string) {
  if (!UUID.test(id)) throw new Response('Not found', { status: 404, headers: privateHeaders })
  return id.toLowerCase()
}
export function providerName(workspace: string, id: string) {
  return `${workspace}/providers/${uuidOr404(id)}`
}
export function providerPath(workspacePath: string, name: string) {
  return `${workspacePath}/providers/${name.split('/').at(-1)}`
}

/** Follows page tokens to the end; the tree shows every Provider. */
export async function listAllProviders(api: Pick<ManagementApi, 'providers'>, parent: string) {
  const providers: Provider[] = []
  let pageToken = ''
  do {
    const page = await api.providers.listProviders({
      parent,
      pageSize: 100,
      pageToken,
    })
    providers.push(...page.providers)
    pageToken = page.nextPageToken
  } while (pageToken)
  return providers
}
export async function listAllOperations(api: Pick<ManagementApi, 'providers'>, parent: string) {
  const operations: ProviderOperation[] = []
  let pageToken = ''
  do {
    const page = await api.providers.listOperations({
      parent,
      pageSize: 100,
      pageToken,
    })
    operations.push(...page.operations)
    pageToken = page.nextPageToken
  } while (pageToken)
  return operations
}
export function accountLabel(route: CredentialRoute | undefined): AccountLabel {
  switch (route?.kind) {
    case CredentialRoute_Kind.FIXED:
      return accountLabels.fixed
    case CredentialRoute_Kind.CALLER:
      return accountLabels.caller
    case CredentialRoute_Kind.NONE:
      return accountLabels.none
    default:
      return accountLabels.none
  }
}
export function routeFromLabel(value: string): CredentialRoute {
  switch (value) {
    case 'none':
      return { kind: CredentialRoute_Kind.NONE } as CredentialRoute
    case 'caller':
      return { kind: CredentialRoute_Kind.CALLER } as CredentialRoute
    case 'fixed':
      return { kind: CredentialRoute_Kind.FIXED } as CredentialRoute
    default:
      throw new Response('Invalid sign-in choice', {
        status: 400,
        headers: privateHeaders,
      })
  }
}

const blockerLabels: Partial<Record<ProviderBlocker, string>> = {
  [ProviderBlocker.WORKSPACE_ARCHIVED]: 'Workspace archived',
  [ProviderBlocker.PROVIDER_DISABLED]: 'Provider disabled',
  [ProviderBlocker.CALLER_ACCOUNT_REQUIRED]: 'Connect an account for this agent',
  [ProviderBlocker.SHARED_ACCOUNT_REQUIRED]: 'Connect the shared account',
  [ProviderBlocker.ACCOUNT_SETTING_INCOMPATIBLE]: 'Change the account setting',
  [ProviderBlocker.NO_OPERATIONS_READY]: 'No operations ready',
}
export function blockerLabel(blocker: ProviderBlocker) {
  const label = blockerLabels[blocker]
  return label ?? 'Unavailable'
}

const personalAccountActions: Record<PersonalAccountState, string | null> = {
  connected: null,
  not_connected: 'Connect your account',
  needs_reconnect: 'Reconnect your account',
}
/**
 * Coral leaves your own account out of provider readiness, so it stays apart from
 * the blockers. The account setting decides whether you have one: a missing row
 * for a separate-account provider means you never connected it.
 */
export function personalAccountAction(
  provider: Provider,
  personalAccounts: ReadonlyMap<string, PersonalAccountState>,
): string | null {
  if (accountChoice(provider.credentialRoute) !== 'caller') return null
  return personalAccountActions[personalAccounts.get(provider.name) ?? 'not_connected']
}

/** Only the hints the provider declared; Coral never assumes a default. */
export function operationHints(hints: OperationHints | undefined): OperationHintsView {
  const view: OperationHintsView = {}
  if (hints?.readOnly !== undefined) view.readOnly = hints.readOnly
  if (hints?.destructive !== undefined) view.destructive = hints.destructive
  if (hints?.idempotent !== undefined) view.idempotent = hints.idempotent
  if (hints?.openWorld !== undefined) view.openWorld = hints.openWorld
  return view
}

function accessState(value: OperationAccessState): OperationView['policy'] {
  switch (value) {
    case OperationAccessState.ENABLED:
      return 'enabled'
    case OperationAccessState.APPROVAL_REQUIRED:
      return 'approval_required'
    case OperationAccessState.DISABLED:
      return 'disabled'
    default:
      return 'approval_required'
  }
}

/**
 * Groups by the first tag, else the dotted prefix; a flat list otherwise. The
 * setup review groups its own preview by the same rule, so it lives here.
 */
export function operationGroup(o: { tags: readonly string[]; relativePath: string }) {
  if (o.tags[0]) return o.tags[0]
  const dot = o.relativePath.lastIndexOf('.')
  return dot > 0 ? o.relativePath.slice(0, dot) : ''
}

export function operationView(
  o: ProviderOperation,
  options: { callRoot: string; basePath: string; policy?: OperationPolicy },
): OperationView {
  const hints = operationHints(o.hints)
  // Coral's default when no rule exists: only a declared read-only operation runs freely.
  const policyState =
    options.policy?.accessState ??
    (hints.readOnly === true
      ? OperationAccessState.ENABLED
      : OperationAccessState.APPROVAL_REQUIRED)
  return {
    name: o.name,
    relativePath: o.relativePath,
    callPath: options.callRoot
      ? operationCallPath(options.callRoot, o.relativePath)
      : o.relativePath,
    summary: o.summary,
    description: o.description,
    hints,
    policy: accessState(policyState),
    available: o.available,
    reasons: o.diagnostics.map((d) => d.message),
    group: operationGroup(o),
    path: `${options.basePath}/ops/${encodeURIComponent(o.relativePath)}`,
  }
}

async function operationPolicies(
  api: ManagementApi,
  parent: string,
  operations: readonly ProviderOperation[],
) {
  if (operations.length === 0) return []
  try {
    return (
      await api.policies.getOperationPolicies({
        parent,
        operations: operations.map((o) => o.name),
      })
    ).policies
  } catch (error) {
    // A deployment without the policy service reports Unimplemented; showing
    // classification defaults is correct there. Any other failure — expired
    // auth, permission, transient — must not render defaults as the saved
    // policy on the screen an operator reviews policy from.
    if (error instanceof ConnectError && error.code === Code.Unimplemented) return null
    throw error
  }
}

function enabled(state: ProviderState) {
  switch (state) {
    case ProviderState.ACTIVE:
      return true
    case ProviderState.DISABLED:
      return false
    default:
      return false
  }
}

export async function providerView(
  api: ManagementApi,
  provider: Provider,
  workspacePath: string,
  /** Your own account's state for each separate-account provider, by provider name. */
  personalAccounts: ReadonlyMap<string, PersonalAccountState>,
): Promise<ProviderView> {
  const path = providerPath(workspacePath, provider.name)
  const operations: ProviderOperation[] = []
  const policies = new Map<string, OperationPolicy>()
  let hasPolicies = true
  const readPage = (pageToken: string) =>
    api.providers.listOperations({ parent: provider.name, pageSize: 100, pageToken })
  let page = await readPage('')
  while (page) {
    // Keep at most one policy read and one catalog read in flight per provider.
    // Both promises are observed together, so either failure rejects the load.
    const [decisions, next] = await Promise.all([
      hasPolicies ? operationPolicies(api, provider.name, page.operations) : [],
      page.nextPageToken ? readPage(page.nextPageToken) : undefined,
    ])
    operations.push(...page.operations)
    if (decisions === null) {
      hasPolicies = false
      policies.clear()
    } else {
      for (const policy of decisions) policies.set(policy.operation, policy)
    }
    if (!next) break
    page = next
  }
  const ops = operations.map((o) =>
    operationView(o, {
      callRoot: provider.callRoot,
      basePath: path,
      policy: policies.get(o.name),
    }),
  )
  const id = provider.name.split('/').at(-1)!
  const isEnabled = enabled(provider.state)
  return {
    id,
    displayName: provider.displayName,
    namespace: provider.namespace,
    builtin: isBuiltinProvider(provider.callRoot),
    account: accountLabel(provider.credentialRoute),
    blockers: provider.blockers.map(blockerLabel),
    personalAccountAction: personalAccountAction(provider, personalAccounts),
    readyCount: ops.filter((o) => o.available).length,
    totalCount: ops.length,
    etag: provider.etag,
    path,
    enabled: isEnabled,
    operations: ops,
  }
}

function accountChoice(route: CredentialRoute | undefined): AccountChoice {
  switch (route?.kind) {
    case CredentialRoute_Kind.FIXED:
      return 'fixed'
    case CredentialRoute_Kind.CALLER:
      return 'caller'
    default:
      return 'none'
  }
}

/**
 * Counts a provider's catalog for its settings: groups as the operations view
 * groups them, the hints providers declared, and unavailable operations.
 */
export function operationSummary(
  operations: readonly ProviderOperation[],
): ProviderSettingsView['operations'] {
  const groups = new Set<string>()
  let grouped = false
  const summary = { total: operations.length, groups: 0, readOnly: 0, destructive: 0, blocked: 0 }
  for (const operation of operations) {
    // The operations view lists ungrouped operations under Other.
    const group = operationGroup(operation)
    if (group) grouped = true
    groups.add(group || 'Other')
    if (operation.hints?.readOnly === true) summary.readOnly += 1
    if (operation.hints?.destructive === true) summary.destructive += 1
    if (!operation.available) summary.blocked += 1
  }
  // A catalog where no operation has a group is a flat list, whatever a
  // provider calls its own groups.
  summary.groups = grouped ? groups.size : 0
  return summary
}

/**
 * Settings from the provider read alone; it holds no configuration or source.
 * The shared account's state comes from its blocker; your own account's is
 * unknown here. The settings loader replaces both with the account read where
 * the reader may make one.
 */
export function providerSettingsView(
  provider: Provider,
  workspacePath: string,
  operations: readonly ProviderOperation[],
): ProviderSettingsView {
  const account = accountChoice(provider.credentialRoute)
  return {
    id: provider.name.split('/').at(-1)!,
    displayName: provider.displayName,
    namespace: provider.namespace,
    callRoot: provider.callRoot,
    builtin: isBuiltinProvider(provider.callRoot),
    etag: provider.etag,
    path: providerPath(workspacePath, provider.name),
    enabled: enabled(provider.state),
    operationsReady: !provider.blockers.includes(ProviderBlocker.NO_OPERATIONS_READY),
    operations: operationSummary(operations),
    account,
    // Blockers report the shared account, but not a User's own account.
    connection:
      account === 'caller'
        ? 'unknown'
        : provider.blockers.includes(ProviderBlocker.SHARED_ACCOUNT_REQUIRED)
          ? 'not_connected'
          : 'connected',
    canConnect: true,
    canReconnect: true,
    accountSettingIncompatible: provider.blockers.includes(
      ProviderBlocker.ACCOUNT_SETTING_INCOMPATIBLE,
    ),
  }
}

const reasons: Record<string, string> = {
  ETAG_MISMATCH: 'This provider changed. Reload the page. Review the changes before you try again.',
  PROVIDER_CHANGED:
    'This provider changed. Reload the page. Review the changes before you try again.',
  NAMESPACE_IN_USE: 'This program name is in use in this workspace. Select a different name.',
  ACCOUNT_REQUIRED: 'This choice needs an account. Sign in first.',
  WORKSPACE_ARCHIVED: 'This workspace is archived. Restore it in settings before you make changes.',
  INVALID_LIFECYCLE_STATE: 'The provider is already in that state. Reload the page.',
  BUILTIN_PROVIDER_MANAGED: 'Ask your Coral administrator to make this change.',
  REQUEST_ID_CONFLICT:
    'Coral used this request for a different change. Reload the page. Start the change again.',
  SETUP_CONSUMED: 'This attempt is complete. Start over to try again.',
  CONSENT_DESTINATION_REJECTED:
    'Coral cannot open this sign-in address. Use HTTPS or a local or private HTTP address.',
  // One message for both a configured denial and the fixed blocklist: the
  // rejection names neither, and the copy must not reveal the address class.
  OUTBOUND_DESTINATION_REJECTED:
    "Coral's outbound settings do not allow this address. Check the deployment's outbound settings before you try again.",
  SETUP_NOT_READY: 'Complete the previous steps before you add or update the provider.',
  SETUP_NOT_APPLICABLE: 'That step does not apply now. Reload the page.',
  INPUT_UNAVAILABLE: 'Coral could not reach this address. Check it and try again.',
  OPENAPI_DOCUMENT_TOO_LARGE: `This OpenAPI document is larger than ${OPENAPI_DOCUMENT_SIZE_LABEL}. Select a smaller document.`,
  CLIENT_BOOTSTRAP_UNAVAILABLE:
    'This server needs an OAuth client ID. Register Coral with the server to get one.',
  CLIENT_METADATA_NOT_PUBLIC:
    "Coral cannot confirm that this server can read Coral's client metadata document. The document address resolves to a non-public address on Coral's host. Enter a client ID. If you run Coral, you can also publish the document on a public address. Or set the MCP client bootstrap policy to prefer the document.",
  CLIENT_METADATA_UNRESOLVED:
    "Coral cannot confirm that this server can read Coral's client metadata document. Coral's host could not resolve the document address. Enter a client ID. If you run Coral, check the document address. Or set the MCP client bootstrap policy to prefer the document.",
  INVALID_PROVIDER_INPUT: 'Coral cannot use this document. Check the fields.',
}
const statuses: Partial<Record<Code, number>> = {
  [Code.PermissionDenied]: 403,
  [Code.NotFound]: 404,
  [Code.InvalidArgument]: 400,
  [Code.FailedPrecondition]: 409,
  [Code.AlreadyExists]: 409,
  [Code.Aborted]: 409,
}
/** Maps a failed Provider mutation to one safe sentence and a status the form can show. */
export function providerFailure(error: unknown, operation = 'Coral could not apply this change. ') {
  if (error instanceof Response) throw error
  if (!(error instanceof ConnectError)) console.error('Unexpected failure in a Reef action:', error)
  const status = error instanceof ConnectError ? (statuses[error.code] ?? 503) : 503
  const reason =
    error instanceof ConnectError && error.rawMessage.startsWith('AUTHORIZATION_DISCOVERY_')
      ? "Coral does not support this server's sign-in requirements."
      : error instanceof ConnectError
        ? reasons[error.rawMessage]
        : undefined
  const message =
    reason ??
    (status === 503
      ? 'Reload the page to check whether Coral applied the change before you submit it again.'
      : status === 403
        ? 'You do not have permission for this action.'
        : status === 404
          ? 'Coral cannot find this item. Reload the page.'
          : 'Check the form fields. Correct any errors before you try again.')
  return data(
    {
      error: reason ? message : `${operation}${message}`,
      credential: null,
    },
    { status, headers: privateHeaders },
  )
}
